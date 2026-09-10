package reconciler

// Internal tests, so they can read the backup store's disk-operation counters. The
// central promise of the cutover path is that it touches neither pmxcfs nor `nft`, and
// that is only assertable from inside the package.

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/hostzero/network-reconciler/internal/events"
	"github.com/hostzero/network-reconciler/internal/netbox"
	"github.com/hostzero/network-reconciler/internal/state"
)

func newTestStore() *state.Store { return state.New() }

// opLog records kernel-facing operations in order, so ordering can be asserted
// across the nftables and FRR stubs together.
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) record(op string) {
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

type orderedNFT struct{ ops *opLog }

func (o *orderedNFT) Apply(_ context.Context, _ []netbox.NATMapping) error {
	o.ops.record("nft:apply")
	return nil
}
func (o *orderedNFT) Flush(context.Context) error { return nil }

type orderedFRR struct {
	ops  *opLog
	live map[netip.Addr]int
}

func (o *orderedFRR) Advertise(_ context.Context, ip netip.Addr) error {
	if o.live == nil {
		o.live = make(map[netip.Addr]int)
	}
	o.live[ip]++
	o.ops.record("frr:advertise:" + ip.String())
	return nil
}
func (o *orderedFRR) Withdraw(_ context.Context, ip netip.Addr) error {
	delete(o.live, ip)
	o.ops.record("frr:withdraw:" + ip.String())
	return nil
}
func (o *orderedFRR) ActiveIPs(context.Context) (map[netip.Addr]struct{}, error) {
	return map[netip.Addr]struct{}{}, nil
}

// internalLifecycleEvent builds a non-migration lifecycle CloudEvent.
func internalLifecycleEvent(action, phase, node string, vmid int, name string) (string, events.CloudEvent) {
	subject := fmt.Sprintf("pve.hzero.%s.qemu.%d.%s.%s", node, vmid, action, phase)
	return subject, events.CloudEvent{
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
}

// internalMigrateEvent builds a migrate CloudEvent as proxmox-eventbus would publish
// it, including the target_node the staged branch is gated on.
func internalMigrateEvent(phase, srcNode, dstNode string, vmid int, name string) (string, events.CloudEvent) {
	subject := fmt.Sprintf("pve.hzero.%s.qemu.%d.migrate.%s", srcNode, vmid, phase)
	return subject, events.CloudEvent{
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
}

func TestBackupSkipsWriteWhenUnchanged(t *testing.T) {
	store := newBackupStore(t.TempDir())
	mappings := []netbox.NATMapping{{
		ExternalIP:    netip.MustParseAddr("203.0.113.1"),
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}

	for i := 0; i < 5; i++ {
		store.save(101, mappings)
		if err := store.flushNow(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	if got := store.diskWrites.Load(); got != 1 {
		t.Fatalf("expected 1 disk write for 5 identical saves, got %d", got)
	}

	// A genuine change must still be written.
	changed := []netbox.NATMapping{{
		ExternalIP:    netip.MustParseAddr("203.0.113.2"),
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}
	store.save(101, changed)
	if err := store.flushNow(); err != nil {
		t.Fatalf("flush after change: %v", err)
	}
	if got := store.diskWrites.Load(); got != 2 {
		t.Fatalf("expected 2 disk writes after a real change, got %d", got)
	}
}

func TestBackupWriteFailureInvalidatesCacheAndRetries(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions, so the write cannot be made to fail")
	}

	dir := filepath.Join(t.TempDir(), "unwritable", "nested")
	store := newBackupStore(dir)
	mappings := []netbox.NATMapping{{
		ExternalIP:    netip.MustParseAddr("203.0.113.1"),
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
	}}

	// Make the parent unwritable so MkdirAll fails.
	parent := filepath.Dir(dir)
	if err := os.MkdirAll(parent, 0o500); err != nil {
		t.Fatalf("creating parent: %v", err)
	}

	store.save(101, mappings)
	if err := store.flushNow(); err == nil {
		t.Fatal("expected flush to report a failure")
	}

	// The failed write must not be remembered as on-disk, or change detection would
	// suppress the retry forever.
	store.mu.Lock()
	_, known := store.known[101]
	pending := len(store.pending)
	store.mu.Unlock()
	if known {
		t.Fatal("expected cache entry to be invalidated after a failed write")
	}
	if pending != 1 {
		t.Fatalf("expected the failed write to be requeued, got %d pending", pending)
	}

	// Once writable, the retry must succeed.
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatalf("chmod parent: %v", err)
	}
	if err := store.flushNow(); err != nil {
		t.Fatalf("expected retry to succeed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "101.config")); err != nil {
		t.Fatalf("expected backup file after retry: %v", err)
	}
}

func TestBackupLoadUsesMemoryCacheAfterWarm(t *testing.T) {
	dir := t.TempDir()
	store := newBackupStore(dir)
	mappings := []netbox.NATMapping{{
		ExternalIP:    netip.MustParseAddr("203.0.113.1"),
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
	}}
	store.save(101, mappings)
	if err := store.flushNow(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := store.warm(); err != nil {
		t.Fatalf("warm: %v", err)
	}

	// Remove the file behind the store's back: a warmed load must not need it.
	if err := os.Remove(filepath.Join(dir, "101.config")); err != nil {
		t.Fatalf("removing file: %v", err)
	}
	before := store.diskReads.Load()

	got, ok, err := store.load(101)
	if err != nil || !ok {
		t.Fatalf("expected cached load to succeed, ok=%v err=%v", ok, err)
	}
	if len(got) != 1 || got[0].ExternalIP != mappings[0].ExternalIP {
		t.Fatalf("unexpected cached mappings: %+v", got)
	}
	if after := store.diskReads.Load(); after != before {
		t.Fatalf("expected no disk reads after warm, got %d -> %d", before, after)
	}

	// An unknown VMID must also answer from the directory scan, without a syscall.
	if _, ok, err := store.load(999); ok || err != nil {
		t.Fatalf("expected absent result for unknown VMID, ok=%v err=%v", ok, err)
	}
	if after := store.diskReads.Load(); after != before {
		t.Fatalf("expected no disk reads for a known-absent VMID, got %d -> %d", before, after)
	}
}

func TestBackupCloseIsIdempotentWhenFlusherNeverStarted(t *testing.T) {
	store := newBackupStore(t.TempDir())

	// Nothing was ever saved, so no flusher goroutine exists. Both closes must return
	// promptly; the second must not close `stop` and then wait on a `done` that nobody
	// will ever close, which would burn the caller's whole shutdown deadline.
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err, timedOut := store.close(ctx), ctx.Err() != nil
		cancel()
		if err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		if timedOut {
			t.Fatalf("close %d consumed its context deadline instead of returning", i)
		}
	}
}

func TestBackupCloseIsIdempotentAfterFlusherStarted(t *testing.T) {
	dir := t.TempDir()
	store := newBackupStore(dir)
	store.save(101, []netbox.NATMapping{{
		ExternalIP:    netip.MustParseAddr("203.0.113.1"),
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
	}})

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.close(ctx)
		cancel()
		if err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "101.config")); err != nil {
		t.Fatalf("expected the queued write to be drained by close: %v", err)
	}
}

func TestBackupEnqueueAfterCloseIsRefused(t *testing.T) {
	dir := t.TempDir()
	store := newBackupStore(dir)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Post-close saves cannot start a flusher, so accepting them would strand the
	// write in `pending` forever while close() had already reported success.
	store.save(101, []netbox.NATMapping{{
		ExternalIP:    netip.MustParseAddr("203.0.113.1"),
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
	}})

	store.mu.Lock()
	pending := len(store.pending)
	store.mu.Unlock()
	if pending != 0 {
		t.Fatalf("expected post-close save to be refused, got %d pending", pending)
	}
	if got := store.diskWrites.Load(); got != 0 {
		t.Fatalf("expected no disk writes after close, got %d", got)
	}
}

func TestRulesetKeyIsOrderIndependent(t *testing.T) {
	a := netbox.NATMapping{
		ExternalIP: netip.MustParseAddr("203.0.113.1"),
		InternalIP: netip.MustParseAddr("10.0.1.100"),
	}
	b := netbox.NATMapping{
		ExternalIP: netip.MustParseAddr("203.0.113.2"),
		InternalIP: netip.MustParseAddr("10.0.1.101"),
	}

	first := []netbox.NATMapping{a, b}
	second := []netbox.NATMapping{b, a}
	sortMappings(first)
	sortMappings(second)

	// cachedMappings iterates maps, so without sorting the two orders would produce
	// different keys and the nft skip would never fire.
	if rulesetKey(first) != rulesetKey(second) {
		t.Fatalf("ruleset key is order dependent:\n %q\n %q", rulesetKey(first), rulesetKey(second))
	}
}

// ── Cutover: the headline assertions ────────────────────────────────────────────

type countingNFT struct {
	applyCalls int
	last       []netbox.NATMapping
}

func (c *countingNFT) Apply(_ context.Context, m []netbox.NATMapping) error {
	c.applyCalls++
	c.last = m
	return nil
}
func (c *countingNFT) Flush(_ context.Context) error { return nil }

type recordingFRR struct {
	ops        []string
	advertised map[netip.Addr]int
}

func newRecordingFRR() *recordingFRR {
	return &recordingFRR{
		advertised: make(map[netip.Addr]int),
	}
}

func (f *recordingFRR) Advertise(_ context.Context, ip netip.Addr) error {
	f.advertised[ip]++
	f.ops = append(f.ops, "advertise:"+ip.String())
	return nil
}
func (f *recordingFRR) Withdraw(_ context.Context, ip netip.Addr) error {
	delete(f.advertised, ip)
	f.ops = append(f.ops, "withdraw:"+ip.String())
	return nil
}
func (f *recordingFRR) ActiveIPs(context.Context) (map[netip.Addr]struct{}, error) {
	return map[netip.Addr]struct{}{}, nil
}

type staticNetbox struct {
	mappings []netbox.NATMapping
	fetches  int
}

func (s *staticNetbox) FetchNATMappings(context.Context) ([]netbox.NATMapping, error) {
	s.fetches++
	return s.mappings, nil
}

func (f *recordingFRR) indexOf(op string) int {
	for i, got := range f.ops {
		if got == op {
			return i
		}
	}
	return -1
}

// migrationFixture builds a reconciler on pve01 that is the target of an incoming
// migration of VM 101 from pve02, with caches already warm.
func migrationFixture(t *testing.T) (*Reconciler, *countingNFT, *recordingFRR, netip.Addr) {
	t.Helper()

	extIP := netip.MustParseAddr("203.0.113.1")
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}
	nft := &countingNFT{}
	frrm := newRecordingFRR()

	rec := New("pve01", "hzero", nb, nft, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	// Warm the mapping cache so the fast path is available, as Prime would.
	rec.reconcileMu.Lock()
	rec.rebuildMappingCache(nb.mappings)
	rec.reconcileMu.Unlock()

	return rec, nft, frrm, extIP
}

// A staged VM produces nothing at all: it is still serving on the source, so both a
// rule and an advertisement here would be wrong. Staged exists only so this node's own
// start.finished cannot activate the VM before migrate.synced.
func TestStagedVMProducesNoRulesOrAdvertisement(t *testing.T) {
	rec, nft, frrm, extIP := migrationFixture(t)

	subj, evt := internalMigrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if frrm.advertised[extIP] != 0 {
		t.Fatal("expected no loopback advertisement while staged")
	}
	// The table itself may be (re)established, but it must contain no rules for a VM
	// that has not arrived.
	if len(nft.last) != 0 {
		t.Fatalf("expected no nftables rules while staged, got %d: %+v", len(nft.last), nft.last)
	}
}

// The cutover itself must touch pmxcfs zero times. That is the property that makes it
// bounded: /etc/pve is a corosync-replicated filesystem whose write latency is worst
// precisely while the cluster is migrating a VM.
func TestCutoverPerformsNoPmxcfsIO(t *testing.T) {
	rec, nft, frrm, extIP := migrationFixture(t)

	subj, evt := internalMigrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	if err := rec.FlushBackups(); err != nil {
		t.Fatalf("flushing backups after the staged marker: %v", err)
	}

	// Baselines taken immediately before the cutover.
	readsBefore := rec.backup.diskReads.Load()
	writesBefore := rec.backup.diskWrites.Load()
	nftBefore := nft.applyCalls

	subj, evt = internalMigrateEvent("synced", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if got := rec.backup.diskWrites.Load(); got != writesBefore {
		t.Fatalf("cutover performed %d pmxcfs write(s); it must perform none", got-writesBefore)
	}
	if got := rec.backup.diskReads.Load(); got != readsBefore {
		t.Fatalf("cutover performed %d pmxcfs read(s); it must perform none", got-readsBefore)
	}

	// Exactly one nftables apply installs the VM's rules; the loopback address follows.
	if got := nft.applyCalls - nftBefore; got != 1 {
		t.Fatalf("expected exactly 1 nftables apply at cutover, got %d", got)
	}
	if len(nft.last) != 1 {
		t.Fatalf("expected 1 nft rule after cutover, got %d: %+v", len(nft.last), nft.last)
	}
	if frrm.advertised[extIP] != 1 {
		t.Fatalf("expected %s to be advertised at cutover, have %+v", extIP, frrm.advertised)
	}
}

// Ordering on the target: the DNAT rule must exist before the external IP becomes a
// local address, or the host answers with a RST instead of forwarding the packet.
func TestTargetCutoverAppliesRulesBeforeAdvertising(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}

	ops := &opLog{}
	nft := &orderedNFT{ops: ops}
	frrm := &orderedFRR{ops: ops}

	rec := New("pve01", "hzero", nb, nft, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())
	rec.reconcileMu.Lock()
	rec.rebuildMappingCache(nb.mappings)
	rec.reconcileMu.Unlock()

	subj, evt := internalMigrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	ops.ops = nil // only care about cutover ordering

	subj, evt = internalMigrateEvent("synced", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	apply := ops.indexOf("nft:apply")
	advertise := ops.indexOf("frr:advertise:" + extIP.String())
	if apply < 0 || advertise < 0 {
		t.Fatalf("expected both apply and advertise at cutover, got %v", ops.snapshot())
	}
	if apply > advertise {
		t.Fatalf("expected nftables rules before the loopback advertisement, got %v", ops.snapshot())
	}
}

// On the node losing the VM, the withdraw must precede the nftables teardown.
// Withdrawing first means packets still routed here during convergence are forwarded
// toward the departed VM (a drop); tearing down nftables first would leave the /32 as
// a local address and the host would answer with a RST, killing the connection.
func TestSourceCutoverWithdrawsBeforeTearingDownNFT(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}

	ops := &opLog{}
	nft := &orderedNFT{ops: ops}
	frrm := &orderedFRR{ops: ops}

	// pve02 is the source: the VM runs here and is about to leave for pve01.
	rec := New("pve02", "hzero", nb, nft, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	subj, evt := internalLifecycleEvent("start", "finished", "pve02", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())

	if frrm.live[extIP] != 1 {
		t.Fatalf("expected %s advertised while running here, have %+v", extIP, frrm.live)
	}
	ops.ops = nil // only care about cutover ordering

	subj, evt = internalMigrateEvent("synced", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	withdraw := ops.indexOf("frr:withdraw:" + extIP.String())
	apply := ops.indexOf("nft:apply")
	if withdraw < 0 {
		t.Fatalf("expected a withdraw at source cutover, got %v", ops.snapshot())
	}
	if apply < 0 {
		t.Fatalf("expected an nftables teardown at source cutover, got %v", ops.snapshot())
	}
	if withdraw > apply {
		t.Fatalf("expected withdraw before nftables teardown, got %v", ops.snapshot())
	}
}

func TestMigrationFailedRemovesStagedStateWithoutFullFetch(t *testing.T) {
	rec, _, frrm, extIP := migrationFixture(t)
	nb := rec.netbox.(*staticNetbox)

	subj, evt := internalMigrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	fetchesBefore := nb.fetches

	subj, evt = internalMigrateEvent("failed", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if frrm.advertised[extIP] != 0 {
		t.Fatal("expected no advertisement after a failed migration")
	}
	if nb.fetches != fetchesBefore {
		t.Fatalf("rollback should use the cache, not a NetBox fetch (%d -> %d)", fetchesBefore, nb.fetches)
	}
}

func TestColdMappingCacheFallsBackAndReportsIt(t *testing.T) {
	nb := &staticNetbox{}
	rec := New("pve01", "hzero", nb, &countingNFT{}, newRecordingFRR(), newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	if ok := rec.applyCachedImmediately("test"); ok {
		t.Fatal("expected the fast path to report itself unavailable with a cold cache")
	}
}

// preloadedFRR reports loopback addresses and staged routes that already exist, as a
// node whose loopback interface is shared with other tooling would.
type preloadedFRR struct {
	*recordingFRR
	existingActive map[netip.Addr]struct{}
}

func (p *preloadedFRR) ActiveIPs(context.Context) (map[netip.Addr]struct{}, error) {
	return p.existingActive, nil
}

// The loopback interface is shared. pve01 in the target cluster carries 19 unrelated
// /32s including public service addresses; adopting them would mean withdrawing them on
// the next reconcile, because they are not in any NetBox mapping. Only addresses this
// service is responsible for may be adopted.
func TestForeignLoopbackAddressesAreNeverWithdrawn(t *testing.T) {
	ours := netip.MustParseAddr("172.16.1.1")
	foreign := []netip.Addr{
		netip.MustParseAddr("198.51.100.7"),
		netip.MustParseAddr("192.0.2.103"),
	}

	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    ours,
		InternalIP:    netip.MustParseAddr("192.0.2.51"),
		VMProxmoxVMID: 999,
		VMName:        "natvm",
	}}}

	existing := map[netip.Addr]struct{}{ours: {}}
	for _, ip := range foreign {
		existing[ip] = struct{}{}
	}
	frrm := &preloadedFRR{
		recordingFRR:   newRecordingFRR(),
		existingActive: existing,
	}

	rec := New("pve01", "hzero", nb, &countingNFT{}, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	// No VMs active here, so the desired set is empty: every adopted address would be
	// withdrawn on this pass.
	rec.reconcileMu.Lock()
	rec.rebuildMappingCache(nb.mappings)
	rec.reconcileMu.Unlock()
	rec.RunOnce(context.Background())

	for _, ip := range foreign {
		for _, op := range frrm.ops {
			if op == "withdraw:"+ip.String() {
				t.Fatalf("withdrew a foreign loopback address %s; ops=%v", ip, frrm.ops)
			}
		}
	}

	// Ours is fair game: it is in NetBox but no VM is active here, so it must go.
	found := false
	for _, op := range frrm.ops {
		if op == "withdraw:"+ours.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected our own stale address %s to be withdrawn; ops=%v", ours, frrm.ops)
	}
}

type failingNetbox struct{ calls int }

func (f *failingNetbox) FetchNATMappings(context.Context) ([]netbox.NATMapping, error) {
	f.calls++
	return nil, fmt.Errorf("netbox unavailable")
}

// If NetBox is down at boot the fast path must still be available, seeded from the
// on-disk per-VM backups — otherwise the first migration after a restart pays for a
// multi-second NetBox round trip on the cutover path.
func TestPrimeSeedsCacheFromBackupsWhenNetboxIsDown(t *testing.T) {
	dir := t.TempDir()
	extIP := netip.MustParseAddr("203.0.113.1")

	// Seed a backup file as a previous run would have left behind.
	seed := newBackupStore(dir)
	seed.save(101, []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}})
	if err := seed.flushNow(); err != nil {
		t.Fatalf("seeding backup: %v", err)
	}

	nb := &failingNetbox{}
	rec := New("pve01", "hzero", nb, &countingNFT{}, newRecordingFRR(), newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(dir)

	rec.Prime(context.Background())

	if nb.calls != 1 {
		t.Fatalf("expected Prime to attempt one NetBox fetch, got %d", nb.calls)
	}
	if !rec.applyCachedImmediately("test") {
		t.Fatal("expected the fast path to be available after priming from backups")
	}

	rec.reconcileMu.Lock()
	cached := rec.cachedMappings()
	rec.reconcileMu.Unlock()
	if len(cached) != 1 || cached[0].ExternalIP != extIP {
		t.Fatalf("expected the backup mapping to seed the cache, got %+v", cached)
	}
}

func TestStagedVMsExpireAfterTTL(t *testing.T) {
	rec, _, frrm, extIP := migrationFixture(t)

	subj, evt := internalMigrateEvent("started", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	// Backdate the staged timestamp past the TTL, as an abandoned migration would.
	rec.store.SetStaged(101, "web-01", time.Now().Add(-2*stagedTTL))
	rec.expireStagedVMs()

	if vm, ok := rec.store.Get(101); ok && vm.Status.String() == "staged" {
		t.Fatal("expected the staged VM to be expired")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rec.RunOnce(ctx)

	if frrm.advertised[extIP] != 0 {
		t.Fatalf("expected no advertisement for an expired staged VM, have %+v", frrm.advertised)
	}
}

// ── The stale-advertisement bug ─────────────────────────────────────────────────

// This is the bug that caused a production outage. The old code bailed out of
// applyMappings entirely whenever *no* active VM resolved to a mapping, which also
// skipped withdrawing IPs belonging to VMs that had genuinely left the node. The source
// node kept 172.16.1.1 on lo after the VM migrated away, and because the upstream had
// iBGP multipath enabled it kept forwarding there — a total outage rather than a blip.
func TestDepartedVMIsWithdrawnEvenWhenAnotherVMCannotResolve(t *testing.T) {
	departed := netip.MustParseAddr("203.0.113.1") // VM 101, migrating away
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    departed,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}
	frrm := newRecordingFRR()

	rec := New("pve02", "hzero", nb, &countingNFT{}, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	// VM 101 starts here and gets advertised.
	subj, evt := internalLifecycleEvent("start", "finished", "pve02", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())
	if frrm.advertised[departed] != 1 {
		t.Fatalf("expected %s advertised while running here, have %+v", departed, frrm.advertised)
	}

	// VM 102 is also active here but has no NetBox mapping at all — the condition that
	// used to trip the global bail-out.
	rec.store.SetActive(102, "no-netbox-entry", time.Now())

	// VM 101 migrates away.
	subj, evt = internalMigrateEvent("synced", "pve02", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	if _, still := frrm.advertised[departed]; still {
		t.Fatalf("VM 101 left this node but %s is still advertised: %+v; "+
			"an unresolvable co-tenant VM must not block withdrawal", departed, frrm.advertised)
	}
}

// The bail-out it replaced existed for a real reason: NetBox answers HTTP 200 with an
// empty list, and believing that would withdraw every VM on the node at once.
func TestEmptyFullFetchRetainsInsteadOfWithdrawingEverything(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}
	frrm := newRecordingFRR()

	rec := New("pve01", "hzero", nb, &countingNFT{}, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	subj, evt := internalLifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())
	if frrm.advertised[extIP] != 1 {
		t.Fatalf("expected %s advertised, have %+v", extIP, frrm.advertised)
	}

	// NetBox now returns an empty list while the VM is still running here.
	nb.mappings = nil
	rec.RunOnce(context.Background())

	if frrm.advertised[extIP] != 1 {
		t.Fatalf("an empty NetBox response must not withdraw a running VM, have %+v", frrm.advertised)
	}
}

// A deliberate deletion, by contrast, must take effect — otherwise retention would make
// removing an IP from NetBox impossible. It costs one grace cycle: a believable empty
// still lets the VM's backup serve once, so that a cleared proxmox_vmid or a custom
// field the token cannot read does not drop a working VM instantly. Pruning the backup
// on that cycle is what makes the second one withdraw.
func TestAuthoritativeRemovalStillWithdraws(t *testing.T) {
	gone := netip.MustParseAddr("203.0.113.1")
	kept := netip.MustParseAddr("203.0.113.2")
	nb := &staticNetbox{mappings: []netbox.NATMapping{
		{ExternalIP: gone, InternalIP: netip.MustParseAddr("10.0.1.100"), VMProxmoxVMID: 101, VMName: "web-01"},
		{ExternalIP: kept, InternalIP: netip.MustParseAddr("10.0.1.101"), VMProxmoxVMID: 102, VMName: "web-02"},
	}}
	frrm := newRecordingFRR()

	rec := New("pve01", "hzero", nb, &countingNFT{}, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	for _, vmid := range []int{101, 102} {
		subj, evt := internalLifecycleEvent("start", "finished", "pve01", vmid, "web")
		rec.HandleEvent(subj, evt)
	}
	rec.RunOnce(context.Background())
	if frrm.advertised[gone] != 1 || frrm.advertised[kept] != 1 {
		t.Fatalf("expected both advertised, have %+v", frrm.advertised)
	}

	// VM 101's mapping is removed from NetBox; VM 102's remains, so the response is
	// non-empty and therefore authoritative.
	nb.mappings = nb.mappings[1:]
	rec.RunOnce(context.Background())

	if frrm.advertised[gone] != 1 {
		t.Fatalf("expected %s retained for one grace cycle from its backup, have %+v", gone, frrm.advertised)
	}

	// Second authoritative cycle: the backup was pruned above, so there is nothing left
	// to retain and the withdrawal lands.
	rec.RunOnce(context.Background())

	if _, still := frrm.advertised[gone]; still {
		t.Fatalf("expected %s withdrawn after its NetBox mapping was deleted, have %+v", gone, frrm.advertised)
	}
	if frrm.advertised[kept] != 1 {
		t.Fatalf("expected %s to remain advertised, have %+v", kept, frrm.advertised)
	}
}

// The grace cycle must not become a second source of truth: an IP deleted through a
// webhook delta is explicit knowledge and has to withdraw on the spot, backup and all.
//
// The delta carries no internal address, which is the case that discriminates: the
// cache entry is cleared either way, but syncBackupFromDelta cannot rewrite a backup it
// cannot match an IP against, so only treating the delta source as final withdraws here
// instead of retaining the VM from a backup for another cycle.
func TestWebhookDeltaRemovalWithdrawsWithoutGraceCycle(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	internalIP := netip.MustParseAddr("10.0.1.100")
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    internalIP,
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}
	frrm := newRecordingFRR()

	rec := New("pve01", "hzero", nb, &countingNFT{}, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	subj, evt := internalLifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)
	rec.RunOnce(context.Background())
	if frrm.advertised[extIP] != 1 {
		t.Fatalf("expected %s advertised, have %+v", extIP, frrm.advertised)
	}

	if err := rec.reconcileFromWebhookDelta(context.Background(), WebhookDelta{
		Model:         "ipaddress",
		Event:         "deleted",
		VMProxmoxVMID: 101,
		HasVMID:       true,
		VMName:        "web-01",
		InternalIP:    "", // prechange-only payload: the internal address is not in it
		NATOutside:    nil,
	}); err != nil {
		t.Fatalf("webhook delta reconcile: %v", err)
	}

	if _, still := frrm.advertised[extIP]; still {
		t.Fatalf("expected %s withdrawn immediately on an explicit deletion, have %+v", extIP, frrm.advertised)
	}

	// And the backup must be gone with it, or the next failed fetch would bring it back.
	if _, found, err := rec.backup.load(101); err != nil || found {
		t.Fatalf("expected VM 101 backup pruned by the deletion, found=%v err=%v", found, err)
	}
}

// ── The snapshot full-fetch gate ────────────────────────────────────────────────

// flakyNetbox fails until healthy is set, so a test can watch what a failed fetch does
// to the state that gates the next one.
type flakyNetbox struct {
	healthy  bool
	mappings []netbox.NATMapping
	calls    int
}

func (f *flakyNetbox) FetchNATMappings(context.Context) ([]netbox.NATMapping, error) {
	f.calls++
	if !f.healthy {
		return nil, fmt.Errorf("netbox unavailable")
	}
	return f.mappings, nil
}

// lastFullFetch/lastActiveKey exist to suppress *redundant* fetches. A fetch that failed
// brought back nothing to be redundant with, so recording it would leave the gate shut
// for a whole interval — snapshot batches land every ~30s and the interval is minutes,
// so a NetBox blip during a VM change would go unnoticed until the periodic timer.
func TestFailedFetchDoesNotCloseTheSnapshotFetchGate(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	nb := &flakyNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}
	frrm := newRecordingFRR()

	rec := New("pve01", "hzero", nb, &countingNFT{}, frrm, newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	// A VM appears on the node, so the batch that reports it warrants a fetch.
	rec.store.SetActive(101, "web-01", time.Now())
	if !rec.shouldFullFetchForSnapshot(time.Hour) {
		t.Fatal("a changed VM set must warrant a full fetch")
	}

	// That fetch fails.
	rec.RunOnce(context.Background())
	if nb.calls != 1 {
		t.Fatalf("expected one fetch attempt, got %d", nb.calls)
	}
	if frrm.advertised[extIP] != 0 {
		t.Fatalf("nothing should be advertised while NetBox is down, have %+v", frrm.advertised)
	}

	// The very next snapshot batch must try again rather than wait out the interval.
	if !rec.shouldFullFetchForSnapshot(time.Hour) {
		t.Fatal("a failed fetch must not suppress snapshot-driven retries for a whole interval")
	}

	nb.healthy = true
	rec.RunOnce(context.Background())
	if frrm.advertised[extIP] != 1 {
		t.Fatalf("expected %s advertised once the retry succeeded, have %+v", extIP, frrm.advertised)
	}

	// Now there is something to be redundant with, so the gate closes.
	if rec.shouldFullFetchForSnapshot(time.Hour) {
		t.Fatal("a successful fetch with an unchanged VM set must not refetch within the interval")
	}
}

// Only reconcileBGP's one-time kernel adoption reads the managed-IP allow-list, and
// building it decodes every backed-up VM's JSON while reconcileMu is held. Paying that
// on every apply — including a cutover held to a 50ms watchdog — buys nothing once
// adoption has already run.
func TestManagedIPsNotRecomputedAfterKernelAdoption(t *testing.T) {
	extIP := netip.MustParseAddr("203.0.113.1")
	nb := &staticNetbox{mappings: []netbox.NATMapping{{
		ExternalIP:    extIP,
		InternalIP:    netip.MustParseAddr("10.0.1.100"),
		VMProxmoxVMID: 101,
		VMName:        "web-01",
	}}}

	rec := New("pve01", "hzero", nb, &countingNFT{}, newRecordingFRR(), newTestStore(), zap.NewNop())
	rec.backup = newBackupStore(t.TempDir())

	subj, evt := internalLifecycleEvent("start", "finished", "pve01", 101, "web-01")
	rec.HandleEvent(subj, evt)

	rec.RunOnce(context.Background())
	if !rec.kernelAdoptionDone() {
		t.Fatal("expected the first reconcile to have adopted kernel state")
	}
	if scans := rec.backup.allScans.Load(); scans == 0 {
		t.Fatal("expected the first reconcile to build the managed-IP allow-list")
	}

	before := rec.backup.allScans.Load()
	for i := 0; i < 3; i++ {
		rec.RunOnce(context.Background())
	}
	if after := rec.backup.allScans.Load(); after != before {
		t.Fatalf("managed-IP allow-list rebuilt %d time(s) after adoption; it has no reader left",
			after-before)
	}
}
