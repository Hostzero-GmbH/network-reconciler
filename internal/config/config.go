package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds the complete runtime configuration for network-reconciler.
type Config struct {
	// Cluster is the Proxmox cluster name loaded from pve_members_path at runtime.
	Cluster string `mapstructure:"-"`
	// Node is this node's short hostname loaded from pve_members_path at runtime.
	Node string `mapstructure:"-"`
	// NodeIPs maps cluster node name to its IP, loaded from pve_members_path.
	// Used to build this node's webhook payload URL when auto-registering in Netbox,
	// so registration does not depend on the node's hostname being resolvable.
	NodeIPs map[string]string `mapstructure:"-"`
	// PVEMembersPath is the path to the Proxmox cluster members file.
	// Default: /etc/pve/.members.
	PVEMembersPath string `mapstructure:"pve_members_path"`

	NATS    NATSConfig    `mapstructure:"nats"`
	Netbox  NetboxConfig  `mapstructure:"netbox"`
	FRR     FRRConfig     `mapstructure:"frr"`
	Webhook WebhookConfig `mapstructure:"webhook"`

	// ReconcileInterval is a Go duration string (e.g. "60s", "2m"). Default: "60s".
	ReconcileInterval string `mapstructure:"reconcile_interval"`
	// LogLevel is one of debug, info, warn, error. Default: "info".
	LogLevel string `mapstructure:"log_level"`
}

// NATSConfig holds connection settings for the embedded proxmox-eventbus NATS cluster.
type NATSConfig struct {
	// Servers is a list of NATS server URLs, e.g. ["tls://pve01:4222", "tls://pve02:4222"].
	Servers []string `mapstructure:"servers"`
	// Cert is the path to the client TLS certificate (issued by proxmox-eventbus).
	Cert string `mapstructure:"cert"`
	// Key is the path to the client TLS private key.
	Key string `mapstructure:"key"`
	// CA is the path to the PVE cluster CA certificate.
	CA string `mapstructure:"ca"`
}

// NetboxConfig holds Netbox API connection settings.
type NetboxConfig struct {
	// URL is the base URL of the Netbox instance, e.g. "https://netbox.internal".
	URL string `mapstructure:"url"`
	// Token is the Netbox API token. Can be overridden via NR_NETBOX_TOKEN env var.
	Token string `mapstructure:"token"`
}

// FRRConfig controls loopback IP assignment used for FRR advertisement.
type FRRConfig struct {
	// Enabled controls whether BGP routes are managed. Default: true.
	Enabled bool `mapstructure:"enabled"`
	// LoopbackInterface is the interface used for /32 host IP addresses. Default: "lo".
	LoopbackInterface string `mapstructure:"loopback_interface"`
}

// WebhookConfig controls the Netbox webhook receiver.
type WebhookConfig struct {
	// Enabled controls whether the webhook HTTP server is started. Default: false.
	Enabled bool `mapstructure:"enabled"`
	// ListenAddr is the address and port to listen on, e.g. ":9095". Default: ":9095".
	ListenAddr string `mapstructure:"listen_addr"`
	// Secret is the HMAC-SHA512 shared secret configured in the Netbox webhook.
	// If empty, signature verification is skipped (warn-only). Set via NR_WEBHOOK_SECRET.
	Secret string `mapstructure:"secret"`
	// BaseURL is the externally reachable base URL of this node's webhook server,
	// e.g. "http://pve01:9095". Used when auto-registering in Netbox.
	// If empty, constructed as http://<node>:<port> from the node hostname and ListenAddr.
	BaseURL string `mapstructure:"base_url"`
}

// Load reads and validates the configuration from the given YAML file path.
// Environment variables prefixed with NR_ override config file values
// (e.g. NR_NETBOX_TOKEN overrides netbox.token).
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	v.SetDefault("reconcile_interval", "60s")
	v.SetDefault("log_level", "info")
	v.SetDefault("frr.enabled", true)
	v.SetDefault("frr.loopback_interface", "lo")
	v.SetDefault("nats.cert", "/etc/network-reconciler/certs/client.pem")
	v.SetDefault("nats.key", "/etc/network-reconciler/certs/client.key")
	v.SetDefault("nats.ca", "/etc/network-reconciler/certs/ca.pem")
	v.SetDefault("pve_members_path", "/etc/pve/.members")
	v.SetDefault("webhook.enabled", false)
	v.SetDefault("webhook.listen_addr", ":9095")

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetEnvPrefix("NR")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("reading config %q: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	members, err := readPVEMembers(cfg.PVEMembersPath)
	if err != nil {
		return nil, fmt.Errorf("reading cluster metadata: %w", err)
	}

	// Proxmox cluster metadata is sourced from /etc/pve/.members.
	cfg.Cluster = members.ClusterName
	cfg.Node = members.NodeName
	cfg.NodeIPs = members.NodeIPs

	if len(cfg.NATS.Servers) == 0 {
		cfg.NATS.Servers = members.DefaultNATSServers()
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	if c.PVEMembersPath == "" {
		return fmt.Errorf("pve_members_path must be set")
	}
	if c.Cluster == "" {
		return fmt.Errorf("cluster must be set")
	}
	if c.Node == "" {
		return fmt.Errorf("node must be set")
	}
	if len(c.NATS.Servers) == 0 {
		return fmt.Errorf("nats.servers must contain at least one URL")
	}
	if c.Netbox.URL == "" {
		return fmt.Errorf("netbox.url must be set")
	}
	if c.Netbox.Token == "" {
		return fmt.Errorf("netbox.token must be set (or NR_NETBOX_TOKEN env var)")
	}
	return nil
}
