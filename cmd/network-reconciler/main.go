package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/hostzero/network-reconciler/internal/config"
	"github.com/hostzero/network-reconciler/internal/frr"
	"github.com/hostzero/network-reconciler/internal/natsconn"
	"github.com/hostzero/network-reconciler/internal/netbox"
	"github.com/hostzero/network-reconciler/internal/nftables"
	"github.com/hostzero/network-reconciler/internal/reconciler"
	"github.com/hostzero/network-reconciler/internal/state"
	"github.com/hostzero/network-reconciler/internal/webhook"
)

// version is set at build time via -ldflags.
var version = "dev"

func main() {
	cfgPath := flag.String("config", "/etc/network-reconciler/config.yaml", "path to config file")
	printVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *printVersion {
		fmt.Println("network-reconciler", version)
		os.Exit(0)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}

	log := buildLogger(cfg.LogLevel)
	defer log.Sync() //nolint:errcheck

	log.Info("starting network-reconciler",
		zap.String("version", version),
		zap.String("node", cfg.Node),
		zap.String("cluster", cfg.Cluster),
	)

	checkIPv4Forwarding(log)

	interval, err := time.ParseDuration(cfg.ReconcileInterval)
	if err != nil {
		log.Fatal("invalid reconcile_interval",
			zap.String("value", cfg.ReconcileInterval),
			zap.Error(err),
		)
	}

	// ── Subsystem initialisation ─────────────────────────────────────────────
	nbClient := netbox.NewClient(cfg.Netbox.URL, cfg.Netbox.Token)
	nftMgr := nftables.New()
	frrMgr := frr.New(cfg.FRR.Enabled, cfg.FRR.LoopbackInterface)
	stateStore := state.New()

	rec := reconciler.New(cfg.Node, cfg.Cluster, nbClient, nftMgr, frrMgr, stateStore, log)

	if cfg.FRR.Enabled {
		checkFRRRedistribution(frrMgr, log)
	}

	// ── NATS connection ──────────────────────────────────────────────────────
	sub, err := natsconn.Connect(
		cfg.NATS.Servers,
		cfg.NATS.Cert,
		cfg.NATS.Key,
		cfg.NATS.CA,
		cfg.Cluster,
		cfg.Node,
		log,
	)
	if err != nil {
		log.Fatal("failed to connect to NATS", zap.Error(err))
	}
	defer sub.Close()

	if err := sub.Subscribe(rec.HandleEvent); err != nil {
		log.Fatal("failed to set up NATS subscriptions", zap.Error(err))
	}

	// ── Context wired to OS signals ──────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	// ── Netbox webhook server (optional) ─────────────────────────────────────
	if cfg.Webhook.Enabled {
		whSrv := webhook.NewWithDelta(
			cfg.Webhook.ListenAddr,
			cfg.Webhook.Secret,
			rec.TriggerReconcile,
			func(d webhook.Delta) bool {
				return rec.ApplyWebhookDelta(reconciler.WebhookDelta{
					Event:         d.Event,
					Model:         d.Model,
					VMName:        d.VMName,
					VMProxmoxVMID: d.VMProxmoxVMID,
					HasVMID:       d.HasVMID,
					InternalIP:    d.InternalIP,
					NATOutside:    d.NATOutside,
				})
			},
			log,
		)
		go func() {
			if err := whSrv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("webhook server error", zap.Error(err))
			}
		}()
		go func() {
			<-ctx.Done()
			shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutCancel()
			if err := whSrv.Shutdown(shutCtx); err != nil {
				log.Warn("webhook server shutdown error", zap.Error(err))
			}
		}()

		// Auto-register this node's webhook in Netbox if it doesn't exist yet.
		payloadURL := webhookPayloadURL(cfg.Node, cfg.NodeIPs[cfg.Node], cfg.Webhook.ListenAddr, cfg.Webhook.BaseURL)
		webhookName := "network-reconciler-" + cfg.Node
		log.Info("ensuring netbox webhook registration",
			zap.String("name", webhookName),
			zap.String("payload_url", payloadURL),
		)
		// Registration runs in the background and retries: Netbox is frequently
		// unreachable while a node is still booting, and a one-shot attempt there
		// would leave that node permanently unregistered (no webhook, no events).
		go ensureWebhookRegistration(ctx, nbClient, webhookName, payloadURL, cfg.Webhook.Secret, log)
	}
	// Cold-start: request a full snapshot from all nodes so the state store
	// is populated before the first periodic reconcile fires.
	if err := sub.RequestSnapshot(ctx); err != nil {
		log.Warn("cold-start snapshot request failed (will reconcile on first tick)", zap.Error(err))
	}

	// Warm the backup and NetBox mapping caches so an early migration does not have
	// to fall back to a multi-second NetBox fetch on the cutover path.
	primeCtx, primeCancel := context.WithTimeout(ctx, 15*time.Second)
	rec.Prime(primeCtx)
	primeCancel()

	// ── Main loop ────────────────────────────────────────────────────────────
	rec.Run(ctx, interval)

	// ── Graceful shutdown ────────────────────────────────────────────────────
	log.Info("shutting down")

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer flushCancel()

	if err := rec.CloseBackups(flushCtx); err != nil {
		log.Error("failed to flush pending VM config writes", zap.Error(err))
	}

	if err := rec.FlushRoutes(flushCtx); err != nil {
		log.Error("failed to withdraw loopback interface IPs on shutdown", zap.Error(err))
	} else {
		log.Info("loopback interface IPs withdrawn")
	}

	if err := nftMgr.Flush(flushCtx); err != nil {
		log.Error("failed to flush nftables table on shutdown", zap.Error(err))
	} else {
		log.Info("nftables table flushed")
	}

	log.Info("shutdown complete")
}

// ensureWebhookRegistration keeps retrying EnsureWebhook until it succeeds or ctx
// is cancelled. Failures are never fatal — the webhook server still runs and the
// periodic reconcile keeps converging — but they must not be silently permanent,
// so each attempt is logged with the retry delay.
func ensureWebhookRegistration(ctx context.Context, nb *netbox.Client, name, payloadURL, secret string, log *zap.Logger) {
	const (
		attemptTimeout = 15 * time.Second
		minBackoff     = 5 * time.Second
		maxBackoff     = 2 * time.Minute
	)

	backoff := minBackoff
	for attempt := 1; ; attempt++ {
		regCtx, regCancel := context.WithTimeout(ctx, attemptTimeout)
		err := nb.EnsureWebhook(regCtx, name, payloadURL, secret)
		regCancel()

		if err == nil {
			log.Info("netbox webhook registration confirmed",
				zap.String("name", name),
				zap.String("payload_url", payloadURL),
				zap.Int("attempt", attempt),
			)
			return
		}
		if ctx.Err() != nil {
			return
		}

		log.Warn("netbox webhook auto-registration failed; retrying (manual setup may be required)",
			zap.String("name", name),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", backoff),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// webhookPayloadURL returns the full URL Netbox should POST to.
// If baseURL is explicitly set, uses it (trimming trailing slash) + "/webhook".
// Otherwise it prefers this node's cluster IP from /etc/pve/.members, because
// Netbox usually cannot resolve a Proxmox node's short hostname; the hostname is
// only used when no IP is known.
func webhookPayloadURL(node, nodeIP, listenAddr, baseURL string) string {
	if baseURL != "" {
		return strings.TrimRight(baseURL, "/") + "/webhook"
	}
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil || port == "" {
		port = "9095"
	}
	host := strings.TrimSpace(nodeIP)
	if host == "" {
		host = node
	}
	return "http://" + net.JoinHostPort(host, port) + "/webhook"
}

func buildLogger(level string) *zap.Logger {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := cfg.Build()
	if err != nil {
		// Fallback: should not happen with a valid config.
		panic("failed to build logger: " + err.Error())
	}
	return logger
}

// checkFRRRedistribution warns when bgpd is not configured to announce what this
// service installs. frr.conf is operator-owned and the package never edits it, so a
// missing `redistribute connected` otherwise fails silently: this service reports the
// loopback address being added while the upstream learns nothing, and VMs on the node
// are simply unreachable.
func checkFRRRedistribution(frrMgr *frr.Manager, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	redist, err := frrMgr.CheckRedistribution(ctx)
	if err != nil {
		// Warn, not Debug: this check exists to stop a misconfiguration being silent,
		// so the check being unable to run must not be silent either. Non-fatal —
		// vtysh may be absent, or blocked by the unit's sandboxing.
		log.Warn("could not verify FRR configuration; a node that announces nothing would look healthy. "+
			"Check manually with: vtysh -c \"show running-config\" | grep redistribute",
			zap.Error(err))
		return
	}

	if redist.Connected {
		log.Info("FRR redistribution verified — VMs on this node will be announced")
		return
	}
	log.Warn("FRR is not redistributing connected routes — VMs on this node will NOT be announced; " +
		"add 'redistribute connected' to /etc/frr/frr.conf and run 'systemctl reload frr' " +
		"(see /usr/share/doc/network-reconciler/frr-bgp-example.conf)")
}

func checkIPv4Forwarding(log *zap.Logger) {
	if runtime.GOOS != "linux" {
		log.Warn("cannot verify net.ipv4.ip_forward on non-linux host",
			zap.String("goos", runtime.GOOS),
		)
		return
	}

	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_forward")
	if err != nil {
		log.Warn("failed to read net.ipv4.ip_forward",
			zap.String("path", "/proc/sys/net/ipv4/ip_forward"),
			zap.Error(err),
		)
		return
	}

	value := strings.ToLower(strings.TrimSpace(string(data)))
	if value != "1" && value != "true" {
		log.Warn("net.ipv4.ip_forward is not enabled; forwarding may not work",
			zap.String("value", value),
		)
	}
}
