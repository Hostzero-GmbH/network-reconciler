package reconciler_test

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/hostzero/network-reconciler/internal/events"
	"github.com/hostzero/network-reconciler/internal/netbox"
	"github.com/hostzero/network-reconciler/internal/reconciler"
	"github.com/hostzero/network-reconciler/internal/state"
)

// ── Stubs ────────────────────────────────────────────────────────────────────

type stubNetbox struct {
	mappings []netbox.NATMapping
	fetches  int
	err      error
}

func (s *stubNetbox) FetchNATMappings(_ context.Context) ([]netbox.NATMapping, error) {
	s.fetches++
	if s.err != nil {
		return nil, s.err
	}
	return s.mappings, nil
}

// opLog records the order of kernel-facing operations so tests can assert sequencing
// (e.g. that the loopback address is added only once the DNAT rule exists).
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) record(op string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ops = append(l.ops, op)
}

func (l *opLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ops...)
}

func (l *opLog) indexOf(op string) int {
	for i, got := range l.snapshot() {
		if got == op {
			return i
		}
	}
	return -1
}

type stubNFT struct {
	lastMappings []netbox.NATMapping
	applyCalls   int
	flushCalls   int
	ops          *opLog
}

func (s *stubNFT) Apply(_ context.Context, m []netbox.NATMapping) error {
	s.lastMappings = m
	s.applyCalls++
	s.ops.record("nft:apply")
	return nil
}

func (s *stubNFT) Flush(_ context.Context) error {
	s.flushCalls++
	return nil
}

type stubFRR struct {
	advertised map[netip.Addr]int
	withdrawn  map[netip.Addr]int
	ops        *opLog
}

func newStubFRR() *stubFRR {
	return &stubFRR{
		advertised: make(map[netip.Addr]int),
		withdrawn:  make(map[netip.Addr]int),
	}
}

func (s *stubFRR) Advertise(_ context.Context, ip netip.Addr) error {
	s.advertised[ip]++
	s.ops.record("frr:advertise:" + ip.String())
	return nil
}

func (s *stubFRR) Withdraw(_ context.Context, ip netip.Addr) error {
	s.withdrawn[ip]++
	s.ops.record("frr:withdraw:" + ip.String())
	return nil
}

func (s *stubFRR) ActiveIPs(_ context.Context) (map[netip.Addr]struct{}, error) {
	return map[netip.Addr]struct{}{}, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

var testBackupDirOnce sync.Once

func ensureTestBackupDir() {
	testBackupDirOnce.Do(func() {
		dir, err := os.MkdirTemp("", "network-reconciler-backup-*")
		if err != nil {
			panic(err)
		}
		if err := os.Setenv("NR_BACKUP_DIR", dir); err != nil {
			panic(err)
		}
	})
}

func newTestReconciler(nb *stubNetbox, nft *stubNFT, frrm *stubFRR) *reconciler.Reconciler {
	ensureTestBackupDir()
	store := state.New()
	log := zap.NewNop()
	return reconciler.NewWithDeps("pve01", "hzero", nb, nft, frrm, store, log)
}

func lifecycleEvent(action, phase, node string, vmid int, name string) (string, events.CloudEvent) {
	subject := "pve.hzero." + node + ".qemu." + itoa(vmid) + "." + action + "." + phase
	evt := events.CloudEvent{
		Type: "dev.proxmox.eventbus.qemu." + action + "." + phase,
		Time: time.Now().UTC().Format(time.RFC3339Nano),
		Data: events.VMData{
			Cluster: "hzero",
			Node:    node,
			Kind:    "qemu",
			VMID:    vmid,
			Name:    name,
			Action:  action,
			Phase:   phase,
		},
	}
	return subject, evt
}

func migrateEvent(phase, srcNode, dstNode string, vmid int, name string) (string, events.CloudEvent) {
	subject := "pve.hzero." + srcNode + ".qemu." + itoa(vmid) + ".migrate." + phase
	evt := events.CloudEvent{
		Type: "dev.proxmox.eventbus.qemu.migrate." + phase,
		Time: time.Now().UTC().Format(time.RFC3339Nano),
		Data: events.VMData{
			Cluster:    "hzero",
			Node:       srcNode,
			Kind:       "qemu",
			VMID:       vmid,
			Name:       name,
			Action:     "migrate",
			Phase:      phase,
			SourceNode: srcNode,
			TargetNode: dstNode,
		},
	}
	return subject, evt
}

func snapshotEvent(node string, vmid int, name, vmState string, observedAtNs int64) (string, events.CloudEvent) {
	subject := "pve.hzero." + node + ".qemu." + itoa(vmid) + ".state.snapshot"
	evt := events.CloudEvent{
		Type: "dev.proxmox.eventbus.qemu.state.snapshot",
		Time: time.Unix(0, observedAtNs).UTC().Format(time.RFC3339Nano),
		Data: events.VMData{
			Cluster:      "hzero",
			Node:         node,
			Kind:         "qemu",
			VMID:         vmid,
			Name:         name,
			Action:       "state",
			Phase:        "snapshot",
			State:        vmState,
			ObservedAtNs: observedAtNs,
		},
	}
	return subject, evt
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestVMStartActivatesRulesAndBGP(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	// Drain the reconcile channel.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rec.RunOnce(ctx)

	if nft.applyCalls == 0 {
		t.Fatal("expected nftables Apply to be called")
	}
	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected 1 nft mapping, got %d", len(nft.lastMappings))
	}
	if frrm.advertised[extIP] == 0 {
		t.Fatalf("expected BGP advertisement for %s", extIP)
	}
}

func TestFullFetchWritesBackupForActiveVM(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	// Backup writes are queued off the apply path (pmxcfs must never block a
	// cutover), so drain them before asserting on disk.
	if err := rec.FlushBackups(); err != nil {
		t.Fatalf("flushing backups: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(os.Getenv("NR_BACKUP_DIR"), "101.config"))
	if err != nil {
		t.Fatalf("expected backup file for VM 101: %v", err)
	}
	if !strings.Contains(string(data), "203.0.113.1") {
		t.Fatalf("expected backup file to contain external IP, got %s", string(data))
	}
}

func TestFallbackUsesBackupWhenNetboxUnavailable(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.44")
	intIP := netip.MustParseAddr("10.0.1.100")

	ensureTestBackupDir()
	backupPath := filepath.Join(os.Getenv("NR_BACKUP_DIR"), "101.config")
	if err := os.WriteFile(backupPath, []byte("[{\"ExternalIP\":\"203.0.113.44\",\"InternalIP\":\"10.0.1.100\",\"VMProxmoxVMID\":101,\"VMName\":\"web-01\"}]"), 0o600); err != nil {
		t.Fatalf("writing backup file: %v", err)
	}

	nb := &stubNetbox{err: fmt.Errorf("netbox unavailable")}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected 1 nft mapping from backup fallback, got %d", len(nft.lastMappings))
	}
	if nft.lastMappings[0].ExternalIP != extIP {
		t.Fatalf("expected fallback external IP %s, got %s", extIP, nft.lastMappings[0].ExternalIP)
	}
	if nft.lastMappings[0].InternalIP != intIP {
		t.Fatalf("expected fallback internal IP %s, got %s", intIP, nft.lastMappings[0].InternalIP)
	}
}

func TestNetboxUnavailableWithoutBackupLeavesExistingRulesUnchanged(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	backupPath := filepath.Join(os.Getenv("NR_BACKUP_DIR"), "101.config")
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("removing backup file: %v", err)
	}

	baselineApplyCalls := nft.applyCalls
	nb.err = fmt.Errorf("netbox unavailable")
	rec.RunOnce(context.Background())

	if nft.applyCalls != baselineApplyCalls {
		t.Fatalf("expected no additional nftables apply when NetBox and backup are unavailable, got %d -> %d", baselineApplyCalls, nft.applyCalls)
	}
	if frrm.withdrawn[extIP] != 0 {
		t.Fatalf("expected no BGP withdrawal when NetBox and backup are unavailable, got %d", frrm.withdrawn[extIP])
	}
	if frrm.advertised[extIP] == 0 {
		t.Fatalf("expected existing BGP advertisement to remain for %s", extIP)
	}
}

func TestWebhookDeleteRemovesBackupFile(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	if err := rec.FlushBackups(); err != nil {
		t.Fatalf("flushing backups: %v", err)
	}

	backupPath := filepath.Join(os.Getenv("NR_BACKUP_DIR"), "101.config")
	if _, err := os.Stat(backupPath); err != nil {
		t.Fatalf("expected backup file before delete: %v", err)
	}

	accepted := rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:         "deleted",
		Model:         "ipaddress",
		VMName:        "web-01",
		VMProxmoxVMID: 101,
		HasVMID:       true,
		InternalIP:    "10.0.1.100/24",
		NATOutside:    []string{"203.0.113.1/32"},
	})
	if !accepted {
		t.Fatal("expected webhook delete delta to be accepted")
	}
	rec.RunOnce(context.Background())

	if err := rec.FlushBackups(); err != nil {
		t.Fatalf("flushing backups: %v", err)
	}

	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		t.Fatalf("expected backup file to be removed after delete, got err=%v", err)
	}
	if frrm.withdrawn[extIP] == 0 {
		t.Fatalf("expected BGP withdrawal for %s after delete", extIP)
	}
	if nft.applyCalls < 2 {
		t.Fatalf("expected nftables apply to run for delete update, got %d calls", nft.applyCalls)
	}
}

func TestFullFetchDoesNotActivateRulesWhenStateEmpty(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	rec.RunOnce(context.Background())

	if nft.applyCalls == 0 {
		t.Fatal("expected nftables Apply to be called")
	}
	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected 0 nft mappings when node state is empty, got %d", len(nft.lastMappings))
	}
	if frrm.advertised[extIP] != 0 {
		t.Fatalf("expected no BGP advertisement for %s when node state is empty", extIP)
	}
}

func TestVMStopWithdrawsRulesAndBGP(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	// Start VM.
	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	// Stop VM.
	subj, evt = lifecycleEvent("stop", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected 0 nft mappings after stop, got %d", len(nft.lastMappings))
	}
	if frrm.withdrawn[extIP] == 0 {
		t.Fatalf("expected BGP withdrawal for %s after VM stop", extIP)
	}
}

// A staged VM produces neither NAT rules nor an advertisement: it is still serving on
// the source, so both would be wrong here. This also covers the part unique to the
// staged marker — a local start.finished arriving before migrate.synced must not
// activate the VM.
func TestStagedVMProducesNothingUntilMigrationSynced(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	// Migration started, this node is the target.
	subj, evt := migrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	// The VM's local start can finish before migration.synced, but settings must stay staged.
	subj, evt = lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected no nft mappings before migration.synced, got %d", len(nft.lastMappings))
	}
	if frrm.advertised[extIP] != 0 {
		t.Fatal("expected no loopback IP advertisement while the VM is staged")
	}
}

func TestMigrationSyncedActivatesBGP(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	subj, evt := migrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	subj, evt = migrateEvent("synced", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if frrm.advertised[extIP] == 0 {
		t.Fatalf("expected BGP advertisement after migration.synced")
	}
}

func TestMigrationAwayFromSourceWithdrawsRulesAndBGP(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	// VM starts on this node.
	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	// Migration synced event emitted by source node indicates VM moved away.
	subj, evt = migrateEvent("synced", "pve01", "pve02", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected 0 nft mappings after migration away, got %d", len(nft.lastMappings))
	}
	if frrm.withdrawn[extIP] == 0 {
		t.Fatalf("expected BGP withdrawal for %s after migration away", extIP)
	}
}

func TestMigrationFailedRollsBack(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	subj, evt := migrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	subj, evt = migrateEvent("failed", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected 0 nft mappings after migration failure, got %d", len(nft.lastMappings))
	}
	if frrm.advertised[extIP] != 0 {
		t.Fatal("expected no loopback IP advertisement before migration.synced")
	}
	if frrm.withdrawn[extIP] != 0 {
		t.Fatal("expected no loopback IP withdrawal when nothing was advertised")
	}
}

func TestMigrationSyncedOnSourceClearsImmediatelyDespiteNewerSnapshotTime(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	// VM is active on this source node.
	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	// Snapshot reports a newer running state timestamp than migration.synced event time.
	base := time.Now().UTC()
	subj, evt = snapshotEvent("pve01", 101, "web-01", "running", base.Add(10*time.Second).UnixNano())
	rec.HandleEvent(subj, evt)

	subj, evt = migrateEvent("synced", "pve01", "pve02", 101, "web-01")
	evt.Time = base.Add(2 * time.Second).Format(time.RFC3339Nano)
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected source nft mappings cleared immediately after migration.synced, got %d", len(nft.lastMappings))
	}
	if frrm.withdrawn[extIP] == 0 {
		t.Fatalf("expected BGP withdrawal for %s after migration.synced", extIP)
	}
}

func TestMigrationSyncedFastPathUsesCacheOnSourceWithoutImmediateFetch(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	// Warm cache + advertise rule on source.
	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	baselineFetches := nb.fetches

	// Source receives migration.synced and should clear immediately from cache.
	subj, evt = migrateEvent("synced", "pve01", "pve02", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if nb.fetches != baselineFetches {
		t.Fatalf("expected no immediate NetBox fetch in migration.synced fast-path, got %d -> %d", baselineFetches, nb.fetches)
	}
	if len(nft.lastMappings) != 0 {
		t.Fatalf("expected source nft mappings cleared immediately after migration.synced, got %d", len(nft.lastMappings))
	}
	if frrm.withdrawn[extIP] == 0 {
		t.Fatalf("expected BGP withdrawal for %s in migration.synced fast-path", extIP)
	}

	// Standard full-fetch reconcile remains queued for convergence.
	rec.RunOnce(context.Background())
	if nb.fetches != baselineFetches+1 {
		t.Fatalf("expected queued full-fetch reconcile after migration.synced, got %d -> %d", baselineFetches, nb.fetches)
	}
}

func TestMigrationSyncedFastPathUsesCacheOnTargetWithoutImmediateFetch(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	// Warm cache and stage migration onto target.
	subj, evt := migrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	baselineFetches := nb.fetches

	// Target receives migration.synced and should activate immediately from cache.
	subj, evt = migrateEvent("synced", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if nb.fetches != baselineFetches {
		t.Fatalf("expected no immediate NetBox fetch in migration.synced fast-path, got %d -> %d", baselineFetches, nb.fetches)
	}
	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected target nft mappings applied immediately after migration.synced, got %d", len(nft.lastMappings))
	}
	if frrm.advertised[extIP] == 0 {
		t.Fatalf("expected BGP advertisement for %s in migration.synced fast-path", extIP)
	}

	// Standard full-fetch reconcile remains queued for convergence.
	rec.RunOnce(context.Background())
	if nb.fetches != baselineFetches+1 {
		t.Fatalf("expected queued full-fetch reconcile after migration.synced, got %d -> %d", baselineFetches, nb.fetches)
	}
}

func TestMigrationSyncedCacheWithEmptyCacheRetainsExistingRules(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	// VM 101 running here with rules applied and its /32 advertised.
	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())
	if frrm.advertised[extIP] == 0 {
		t.Fatalf("expected initial BGP advertisement for %s", extIP)
	}

	// NetBox now answers HTTP 200 with an empty list. The full fetch itself distrusts
	// that and retains, but it leaves the mapping cache ready-and-empty.
	nb.mappings = nil
	rec.RunOnce(context.Background())

	baselineWithdrawn := frrm.withdrawn[extIP]

	// A cutover fast path now applies from that empty cache. An empty cache here is
	// absence of knowledge, not a NetBox deletion: VM 101 must keep its rules.
	subj, evt = migrateEvent("synced", "pve02", "pve01", 102, "db-01")
	rec.HandleEvent(subj, evt)

	if frrm.withdrawn[extIP] != baselineWithdrawn {
		t.Fatalf("expected no BGP withdrawal for %s from an empty-cache fast path, got %d -> %d",
			extIP, baselineWithdrawn, frrm.withdrawn[extIP])
	}
	if len(nft.lastMappings) != 1 || nft.lastMappings[0].ExternalIP != extIP {
		t.Fatalf("expected VM 101 rules to be retained, got %+v", nft.lastMappings)
	}
}

func TestFlushRoutesWithdrawsAdvertisedRoutes(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"},
	}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)
	ctx := context.Background()

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(ctx)

	if frrm.advertised[extIP] == 0 {
		t.Fatalf("expected BGP advertisement for %s before flush", extIP)
	}

	if err := rec.FlushRoutes(ctx); err != nil {
		t.Fatalf("FlushRoutes failed: %v", err)
	}

	if frrm.withdrawn[extIP] == 0 {
		t.Fatalf("expected BGP withdrawal for %s during FlushRoutes", extIP)
	}
}

func TestWebhookIPDeltaAppliesWithoutAdditionalFullFetch(t *testing.T) {
	oldExt := netip.MustParseAddr("203.0.113.1")
	newExt := netip.MustParseAddr("203.0.113.44")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: oldExt, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	fetchesAfterWarmup := nb.fetches
	accepted := rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:      "updated",
		Model:      "ipaddress",
		VMName:     "web-01",
		InternalIP: "10.0.1.100/24",
		NATOutside: []string{"203.0.113.44/32"},
	})
	if !accepted {
		t.Fatal("expected webhook delta to be accepted")
	}
	rec.RunOnce(context.Background())

	if nb.fetches != fetchesAfterWarmup {
		t.Fatalf("expected no extra full NetBox fetch for webhook delta, got %d -> %d", fetchesAfterWarmup, nb.fetches)
	}
	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected one nft mapping after webhook delta, got %d", len(nft.lastMappings))
	}
	if nft.lastMappings[0].ExternalIP != newExt {
		t.Fatalf("expected external IP %s after delta, got %s", newExt, nft.lastMappings[0].ExternalIP)
	}
}

func TestPairedIPAndVMWebhookUpdatesDoNotForceFullFetch(t *testing.T) {
	oldExt := netip.MustParseAddr("203.0.113.1")
	newExt := netip.MustParseAddr("203.0.113.9")
	intIP := netip.MustParseAddr("10.0.1.100")

	nb := &stubNetbox{mappings: []netbox.NATMapping{{ExternalIP: oldExt, InternalIP: intIP, VMProxmoxVMID: 101, VMName: "web-01"}}}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	subj, evt := lifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	baselineFetches := nb.fetches
	if !rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:         "updated",
		Model:         "ipaddress",
		VMName:        "web-01",
		VMProxmoxVMID: 101,
		HasVMID:       true,
		InternalIP:    "10.0.1.100/24",
		NATOutside:    []string{"203.0.113.9/32"},
	}) {
		t.Fatal("expected ip webhook delta accepted")
	}
	rec.RunOnce(context.Background())

	if !rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:         "updated",
		Model:         "virtualmachine",
		VMName:        "web-01",
		VMProxmoxVMID: 101,
		HasVMID:       true,
	}) {
		t.Fatal("expected vm webhook delta accepted")
	}
	rec.RunOnce(context.Background())

	if nb.fetches != baselineFetches {
		t.Fatalf("expected paired IP+VM webhook handling without extra full fetch, got %d -> %d", baselineFetches, nb.fetches)
	}
	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected one nft mapping after paired updates, got %d", len(nft.lastMappings))
	}
	if nft.lastMappings[0].ExternalIP != newExt {
		t.Fatalf("expected external IP %s after paired updates, got %s", newExt, nft.lastMappings[0].ExternalIP)
	}
}

func TestWebhookOnlyIPThenVMCreatesRulesWithoutFullFetch(t *testing.T) {
	nb := &stubNetbox{mappings: nil}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	// IP webhook arrives first with NAT mapping details.
	if !rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:      "updated",
		Model:      "ipaddress",
		VMName:     "NAT TEST11",
		InternalIP: "192.0.2.51/32",
		NATOutside: []string{"172.16.1.1/32"},
	}) {
		t.Fatal("expected IP webhook delta accepted")
	}
	rec.RunOnce(context.Background())

	// VM webhook arrives second with proxmox VMID, enabling active-state update.
	if !rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:         "updated",
		Model:         "virtualmachine",
		VMName:        "NAT TEST11",
		VMProxmoxVMID: 999,
		HasVMID:       true,
	}) {
		t.Fatal("expected VM webhook delta accepted")
	}
	rec.RunOnce(context.Background())

	if nb.fetches != 0 {
		t.Fatalf("expected zero full NetBox fetches in webhook-only flow, got %d", nb.fetches)
	}
	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected one nft mapping after paired webhook deltas, got %d", len(nft.lastMappings))
	}
	if got := nft.lastMappings[0].VMName; got != "NAT TEST11" {
		t.Fatalf("expected VMName NAT TEST11, got %s", got)
	}
	if got := nft.lastMappings[0].ExternalIP.String(); got != "172.16.1.1" {
		t.Fatalf("expected external IP 172.16.1.1, got %s", got)
	}
}

func TestWebhookIPWithoutVMIDThenVMWithVMIDAppliesRules(t *testing.T) {
	nb := &stubNetbox{mappings: nil}
	nft := &stubNFT{}
	frrm := newStubFRR()
	rec := newTestReconciler(nb, nft, frrm)

	if !rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:      "updated",
		Model:      "ipaddress",
		VMName:     "NAT TEST11",
		InternalIP: "192.0.2.51/32",
		NATOutside: []string{"172.16.1.1/32"},
	}) {
		t.Fatal("expected IP webhook delta accepted")
	}
	rec.RunOnce(context.Background())

	if !rec.ApplyWebhookDelta(reconciler.WebhookDelta{
		Event:         "updated",
		Model:         "virtualmachine",
		VMName:        "NAT TEST11",
		VMProxmoxVMID: 999,
		HasVMID:       true,
	}) {
		t.Fatal("expected VM webhook delta accepted")
	}
	rec.RunOnce(context.Background())

	if len(nft.lastMappings) != 1 {
		t.Fatalf("expected one nft mapping after IP+VM sequence, got %d", len(nft.lastMappings))
	}
	if got := nft.lastMappings[0].VMProxmoxVMID; got != 999 {
		t.Fatalf("expected VMProxmoxVMID 999, got %d", got)
	}
	if got := nft.lastMappings[0].ExternalIP.String(); got != "172.16.1.1" {
		t.Fatalf("expected external IP 172.16.1.1, got %s", got)
	}
}
