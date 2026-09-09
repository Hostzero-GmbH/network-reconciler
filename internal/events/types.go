// Package events defines the CloudEvents 1.0 types published by proxmox-eventbus.
// Schema: https://github.com/Hostzero-GmbH/proxmox-eventbus/blob/main/docs/EVENTS.md
package events

// CloudEvent is the CloudEvents 1.0 envelope published by proxmox-eventbus.
type CloudEvent struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	Subject         string `json:"subject"`
	Time            string `json:"time"`
	DataContentType string `json:"datacontenttype"`
	Data            VMData `json:"data"`
}

// VMData is the proxmox-eventbus CloudEvent data payload (schemaVersion "1").
type VMData struct {
	SchemaVersion string `json:"schemaVersion"`
	Cluster       string `json:"cluster"`
	Node          string `json:"node"`

	// VM identity — present on lifecycle and per-VM snapshot events.
	Kind        string   `json:"kind"`   // "qemu" | "lxc"
	VMID        int      `json:"vmid"`
	Name        string   `json:"name"`
	Tags        []string `json:"tags"`
	Description string   `json:"description"`

	// Lifecycle-only fields.
	Action string `json:"action"` // start|stop|shutdown|reboot|reset|suspend|resume|migrate|clone|create|destroy|template|state
	Phase  string `json:"phase"`  // started|finished|synced|failed|snapshot

	UPID       string `json:"upid"`
	User       string `json:"user"`
	SourceNode string `json:"source_node"`
	TargetNode string `json:"target_node"`
	DurationMs int64  `json:"duration_ms"`
	ExitStatus string `json:"exit_status"`

	// Per-VM snapshot-only fields.
	State        string `json:"state"`          // running|stopped|paused|suspended|unknown
	SnapshotID   string `json:"snapshot_id"`
	ObservedAtNs int64  `json:"observed_at_ns"` // nanoseconds since epoch

	// snapshot.complete-only.
	Count int `json:"count"`
}
