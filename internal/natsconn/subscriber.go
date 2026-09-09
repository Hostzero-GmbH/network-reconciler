// Package natsconn manages the NATS connection and event subscriptions for
// the proxmox-eventbus CloudEvents stream.
package natsconn

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/hostzero/network-reconciler/internal/events"
)

// Handler is called for each received CloudEvent.
// subject is the raw NATS message subject.
type Handler func(subject string, evt events.CloudEvent)

// Subscriber wraps a NATS connection with typed CloudEvent subscriptions.
type Subscriber struct {
	conn    *nats.Conn
	cluster string
	node    string
	log     *zap.Logger
	tapMax  int
	tapSeen int
}

// Connect establishes a mTLS NATS connection to the proxmox-eventbus cluster.
// cert, key and ca are paths to the client certificate, private key, and
// PVE cluster CA certificate respectively (issued via `proxmox-eventbus issue-client-cert`).
func Connect(servers []string, cert, key, ca, cluster, node string, log *zap.Logger) (*Subscriber, error) {
	opts := []nats.Option{
		nats.Name("network-reconciler-" + node),
		nats.ClientCert(cert, key),
		nats.RootCAs(ca),
		nats.MaxReconnects(-1),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			if err == nil {
				return
			}
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}
			if subject != "" {
				log.Warn("NATS async error", zap.String("subject", subject), zap.Error(err))
				return
			}
			log.Warn("NATS async error", zap.Error(err))
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Warn("NATS disconnected", zap.Error(err))
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			log.Info("NATS reconnected")
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Info("NATS connection closed")
		}),
	}

	conn, err := nats.Connect(strings.Join(servers, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS (%s): %w", strings.Join(servers, ","), err)
	}

	log.Info("connected to NATS",
		zap.String("cluster", cluster),
		zap.Strings("servers", servers),
	)

	return &Subscriber{
		conn:    conn,
		cluster: cluster,
		node:    node,
		log:     log,
		tapMax:  natsTapMax(),
	}, nil
}

// Subscribe registers all event subscriptions and dispatches to handler.
//
// Subscriptions:
//   - Migration events from ALL nodes (we may be the target or source).
//   - Start/stop/shutdown/destroy lifecycle events from THIS node only.
//   - Periodic per-VM state snapshots from THIS node.
//   - Snapshot batch-complete notifications from THIS node.
func (s *Subscriber) Subscribe(handler Handler) error {
	var subjects []string
	for _, cluster := range clusterSubjectVariants(s.cluster) {
		subjects = append(subjects,
			// Migration events: subscribe cluster-wide because migrations involve
			// both source and target nodes.
			fmt.Sprintf("pve.%s.*.*.*.migrate.*", cluster),

			// Lifecycle events: subscribe cluster-wide and filter by payload node
			// in reconciler. This avoids missing events when subject node token
			// differs from configured hostname aliases.
			fmt.Sprintf("pve.%s.*.*.*.start.*", cluster),
			fmt.Sprintf("pve.%s.*.*.*.stop.*", cluster),
			fmt.Sprintf("pve.%s.*.*.*.shutdown.*", cluster),
			fmt.Sprintf("pve.%s.*.*.*.destroy.*", cluster),

			// Periodic per-VM state snapshots: subscribe cluster-wide and filter
			// by payload node in reconciler.
			fmt.Sprintf("pve.%s.*.*.*.state.snapshot", cluster),

			// Batch snapshot completion: subscribe cluster-wide and filter by
			// payload node in reconciler.
			fmt.Sprintf("pve.%s.*.snapshot.complete", cluster),
		)
	}

	for _, subj := range subjects {
		subj := subj // capture loop variable
		if _, err := s.conn.Subscribe(subj, func(msg *nats.Msg) {
			var evt events.CloudEvent
			if err := json.Unmarshal(msg.Data, &evt); err != nil {
				s.log.Warn("failed to decode CloudEvent",
					zap.String("subject", msg.Subject),
					zap.Error(err),
				)
				return
			}
			handler(msg.Subject, evt)
		}); err != nil {
			return fmt.Errorf("subscribing to %q: %w", subj, err)
		}
		s.log.Debug("subscribed", zap.String("subject", subj))
	}

	// Ensure all subscriptions are registered server-side before proceeding.
	if err := s.conn.FlushTimeout(5 * time.Second); err != nil {
		return fmt.Errorf("flushing NATS subscriptions: %w", err)
	}

	if natsTapEnabled() {
		if _, err := s.conn.Subscribe("pve.>", func(msg *nats.Msg) {
			s.tapSeen++
			if s.tapSeen <= s.tapMax {
				s.log.Info("NATS tap message observed",
					zap.String("subject", msg.Subject),
					zap.Int("sample", s.tapSeen),
				)
				if s.tapSeen == s.tapMax {
					s.log.Info("NATS tap sample limit reached", zap.Int("max_messages", s.tapMax))
				}
			}
		}); err != nil {
			return fmt.Errorf("subscribing to NATS tap subject pve.>: %w", err)
		}
		s.log.Warn("NATS tap enabled; sampling raw subjects",
			zap.Int("max_messages", s.tapMax),
		)
		if err := s.conn.FlushTimeout(5 * time.Second); err != nil {
			return fmt.Errorf("flushing NATS tap subscription: %w", err)
		}
	}

	return nil
}

// RequestSnapshot publishes a snapshot request so all nodes respond with their
// current VM states. Used at cold-start to bootstrap placement without polling
// the Proxmox HTTP API.
func (s *Subscriber) RequestSnapshot(ctx context.Context) error {
	_ = ctx
	var subjects []string
	for _, cluster := range clusterSubjectVariants(s.cluster) {
		subjects = append(subjects,
			fmt.Sprintf("pve.%s.snapshot.request", cluster),
			fmt.Sprintf("pve.%s.%s.snapshot.request", cluster, s.node),
		)
	}
	for _, subject := range subjects {
		if err := s.conn.Publish(subject, nil); err != nil {
			return fmt.Errorf("publishing to %s: %w", subject, err)
		}
		s.log.Info("cold-start snapshot request published", zap.String("subject", subject))
	}
	if err := s.conn.FlushTimeout(5 * time.Second); err != nil {
		return fmt.Errorf("flushing snapshot request publishes: %w", err)
	}
	return nil
}

func clusterSubjectVariants(cluster string) []string {
	trimmed := strings.TrimSpace(cluster)
	if trimmed == "" {
		return []string{""}
	}

	variants := []string{trimmed}
	lower := strings.ToLower(trimmed)
	upper := strings.ToUpper(trimmed)
	if lower != trimmed {
		variants = append(variants, lower)
	}
	if upper != trimmed && upper != lower {
		variants = append(variants, upper)
	}
	return variants
}

func natsTapEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("NR_NATS_TAP")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func natsTapMax() int {
	raw := strings.TrimSpace(os.Getenv("NR_NATS_TAP_MAX"))
	if raw == "" {
		return 25
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 25
	}
	if n > 500 {
		return 500
	}
	return n
}

// Close drains and closes the NATS connection gracefully.
func (s *Subscriber) Close() {
	_ = s.conn.Drain()
}
