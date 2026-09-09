// Package reconciler implements the core reconciliation logic:
// it listens for VM lifecycle events, maintains per-VM state, and drives
// nftables and FRR to match the desired state from Netbox.
package reconciler

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/hostzero/network-reconciler/internal/events"
	"github.com/hostzero/network-reconciler/internal/netbox"
	"github.com/hostzero/network-reconciler/internal/state"
)

// handlerWatchdog is the latency budget for a single NATS event handler call. The
// cutover path is a couple of milliseconds of work; exceeding this means something
// slow (pmxcfs, a NetBox fetch) has crept back onto the critical path.
const handlerWatchdog = 50 * time.Millisecond

// stagedTTL bounds how long pre-staged rules may persist without a terminal
// migration event. Aborted migrations emit nothing, so without this they leak.
const stagedTTL = 15 * time.Minute

// Apply sources. These name where the mappings for an apply came from, which decides
// whether an empty result for a VM is trustworthy — see applyMappingsTimed.
const (
	sourceFullFetch      = "full-fetch"
	sourceBackupFallback = "backup-fallback"
	sourceWebhookDelta   = "webhook-delta"
)

// unresolvedEscalateAfter is how many consecutive applies with an unresolvable active
// VM are tolerated before the warning becomes an error.
const unresolvedEscalateAfter = 5

// verifyDelay is how long after a fast-path apply the confirming full reconcile
// runs. Long enough to stay clear of the cutover, short enough to catch drift.
const verifyDelay = 5 * time.Second

// defaultReconcileInterval mirrors the config default, used before Run publishes
// the configured value.
const defaultReconcileInterval = 60 * time.Second

// netboxClient is the interface the Reconciler uses to fetch NAT mappings.
// Implemented by *netbox.Client; also used by test stubs.
type netboxClient interface {
	FetchNATMappings(ctx context.Context) ([]netbox.NATMapping, error)
}

// nftablesManager is the interface for applying and flushing nftables rules.
type nftablesManager interface {
	Apply(ctx context.Context, mappings []netbox.NATMapping) error
	Flush(ctx context.Context) error
}

// frrManager manages the loopback addresses backing VM external IP announcements.
// ActiveIPs reads the kernel back, so advertisements left by a previous process can be
// reconciled rather than lingering unnoticed.
type frrManager interface {
	Advertise(ctx context.Context, ip netip.Addr) error
	Withdraw(ctx context.Context, ip netip.Addr) error
	ActiveIPs(ctx context.Context) (map[netip.Addr]struct{}, error)
}

// WebhookDelta is the normalized subset of webhook payload data used for
// incremental, no-full-fetch reconciliation.
type WebhookDelta struct {
	Event         string
	Model         string
	VMName        string
	VMProxmoxVMID int
	HasVMID       bool
	InternalIP    string
	NATOutside    []string
}

// Reconciler drives nftables and FRR to match the desired state derived from
// Netbox IP mappings for VMs currently running on this node.
type Reconciler struct {
	node    string
	cluster string

	netbox netboxClient
	nft    nftablesManager
	frr    frrManager
	store  *state.Store
	log    *zap.Logger

	reconcileMu sync.Mutex

	reconcileCh    chan struct{}
	webhookDeltaCh chan WebhookDelta
	backup         *backupStore

	// lastRulesetKey fingerprints the mappings last handed to nftables so an
	// unchanged ruleset can skip the `nft -f` fork+exec entirely. Guarded by
	// reconcileMu, like the rest of the apply path.
	lastRulesetKey   string
	rulesetKeyValid  bool
	appliesSinceSync int

	// lastAppliedByVMID records, per VM, the mappings most recently programmed. It is
	// the last resort when neither NetBox nor the on-disk backup can resolve a VM, so
	// that VM keeps its rules while IPs of VMs that have left are still withdrawn.
	// Guarded by reconcileMu, like the rest of the apply path.
	lastAppliedByVMID map[int][]netbox.NATMapping
	// unresolvedStreak counts consecutive applies with at least one unresolvable VM,
	// so a persistent data problem escalates instead of warning forever.
	unresolvedStreak int

	// mapping cache used by webhook deltas to avoid a full NetBox fetch.
	mappingCacheReady    bool
	mappingByVMID        map[int][]netbox.NATMapping
	mappingPendingByName map[string][]netbox.NATMapping
	vmIDByName           map[string]int

	// verifyPending guards the debounced post-cutover verification reconcile, so a
	// burst of migrations schedules one convergence check rather than one each.
	verifyPending atomic.Bool

	// lastFullFetch and lastActiveKey gate snapshot-driven full fetches. Snapshot
	// batches arrive roughly every 30s per node; refetching NetBox each time costs
	// seconds of HTTP and buys nothing when the node's VM set has not changed.
	fetchMu       sync.Mutex
	lastFullFetch time.Time
	lastActiveKey string
	// intervalNs is the periodic reconcile interval, published by Run and read from
	// the NATS delivery goroutine.
	intervalNs atomic.Int64

	// advertisedIPs tracks which external IPs we have currently assigned/advertised.
	// Used to detect and withdraw IPs no longer active on this node,
	// including cases where a Netbox mapping is deleted.
	advMu         sync.Mutex
	advertisedIPs map[netip.Addr]struct{}
	// kernelSynced records whether the advertised set has been reconciled against the
	// kernel since startup. Until it has, in-memory state is unaware of anything a
	// previous process left behind.
	kernelSynced bool
}

// New creates a new Reconciler wired to the concrete production dependencies.
func New(
	node, cluster string,
	nb netboxClient,
	nft nftablesManager,
	frrm frrManager,
	store *state.Store,
	log *zap.Logger,
) *Reconciler {
	return newReconciler(node, cluster, nb, nft, frrm, store, log)
}

// NewWithDeps is identical to New and exists for use in tests where the
// dependency types are stubs rather than the concrete package types.
func NewWithDeps(
	node, cluster string,
	nb netboxClient,
	nft nftablesManager,
	frrm frrManager,
	store *state.Store,
	log *zap.Logger,
) *Reconciler {
	return newReconciler(node, cluster, nb, nft, frrm, store, log)
}

func newReconciler(
	node, cluster string,
	nb netboxClient,
	nft nftablesManager,
	frrm frrManager,
	store *state.Store,
	log *zap.Logger,
) *Reconciler {
	return &Reconciler{
		node:                 node,
		cluster:              cluster,
		netbox:               nb,
		nft:                  nft,
		frr:                  frrm,
		store:                store,
		log:                  log,
		reconcileCh:          make(chan struct{}, 1),
		webhookDeltaCh:       make(chan WebhookDelta, 32),
		backup:               newBackupStoreWithLogger(os.Getenv("NR_BACKUP_DIR"), log),
		mappingByVMID:        make(map[int][]netbox.NATMapping),
		mappingPendingByName: make(map[string][]netbox.NATMapping),
		vmIDByName:           make(map[string]int),
		advertisedIPs:        make(map[netip.Addr]struct{}),
		lastAppliedByVMID:    make(map[int][]netbox.NATMapping),
	}
}

// managedIPs returns every external IP this service is responsible for, drawn from the
// NetBox mapping cache and the on-disk backups — i.e. cluster-wide, not just this node.
// It is the allow-list for adopting pre-existing kernel state, so that addresses put on
// the loopback interface by an operator or another tool are never withdrawn.
//
// Callers must hold reconcileMu, since it reads the mapping cache.
func (r *Reconciler) managedIPs() map[netip.Addr]struct{} {
	managed := make(map[netip.Addr]struct{})
	for _, m := range r.cachedMappings() {
		managed[m.ExternalIP] = struct{}{}
	}
	for _, m := range r.backup.all() {
		managed[m.ExternalIP] = struct{}{}
	}
	return managed
}

// kernelAdoptionDone reports whether the one-time reconciliation of the advertised set
// against the kernel has already happened, so callers can skip preparing its inputs.
func (r *Reconciler) kernelAdoptionDone() bool {
	r.advMu.Lock()
	defer r.advMu.Unlock()
	return r.kernelSynced
}

// hasNewAdvertisements reports whether converging to the desired sets would add any
// announcement that is not live yet. When nothing is being added, the apply is purely
// a teardown and BGP should go first.
func (r *Reconciler) hasNewAdvertisements(desired map[netip.Addr]struct{}) bool {
	r.advMu.Lock()
	defer r.advMu.Unlock()

	for ip := range desired {
		if _, live := r.advertisedIPs[ip]; !live {
			return true
		}
	}
	return false
}

// ApplyWebhookDelta enqueues a webhook delta for incremental reconcile.
// Returns false when the queue is full; caller should fall back to TriggerReconcile.
func (r *Reconciler) ApplyWebhookDelta(delta WebhookDelta) bool {
	select {
	case r.webhookDeltaCh <- delta:
		return true
	default:
		return false
	}
}

// TriggerReconcile enqueues an immediate reconcile. Safe to call from any
// goroutine (e.g. the Netbox webhook handler). If a reconcile is already
// pending the extra signal is dropped — coalesced into the pending one.
func (r *Reconciler) TriggerReconcile() {
	select {
	case r.reconcileCh <- struct{}{}:
	default:
	}
}

// Prime warms local caches so the first cutover after startup can take the fast
// path. Failures are non-fatal — the periodic reconcile converges regardless.
func (r *Reconciler) Prime(ctx context.Context) {
	if err := r.backup.warm(); err != nil {
		r.log.Warn("failed to warm VM config backups", zap.Error(err))
	}

	mappings, err := r.netbox.FetchNATMappings(ctx)
	if err != nil {
		// Fall back to the on-disk per-VM backups. They are what the fast path would
		// have resolved anyway, so seeding from them keeps cutovers fast even when
		// NetBox is unreachable at boot.
		cached := r.backup.all()
		r.log.Warn("startup NetBox fetch failed; priming the mapping cache from on-disk backups",
			zap.Int("backup_mappings", len(cached)),
			zap.Error(err),
		)
		if len(cached) == 0 {
			return
		}
		r.reconcileMu.Lock()
		r.rebuildMappingCache(cached)
		r.reconcileMu.Unlock()
		return
	}

	r.reconcileMu.Lock()
	r.rebuildMappingCache(mappings)
	r.reconcileMu.Unlock()

	r.log.Info("mapping cache primed", zap.Int("netbox_mappings", len(mappings)))
}

// FlushBackups synchronously writes any queued VM config changes. Safe to call
// repeatedly; used by tests and by callers that need the on-disk state current.
func (r *Reconciler) FlushBackups() error {
	return r.backup.flushNow()
}

// CloseBackups drains outstanding writes and stops the background writer.
func (r *Reconciler) CloseBackups(ctx context.Context) error {
	return r.backup.close(ctx)
}

// FlushRoutes withdraws the loopback IP addresses and staged host routes this process
// installed. Intended for graceful shutdown so they are cleaned up alongside nftables.
//
// The empty managed set skips kernel adoption deliberately: on shutdown we withdraw only
// what we know we advertised, never something we merely found on the interface.
func (r *Reconciler) FlushRoutes(ctx context.Context) error {
	return r.reconcileBGP(ctx, map[netip.Addr]struct{}{}, nil)
}

// triggerReconcile is the internal alias used within the package.
func (r *Reconciler) triggerReconcile() { r.TriggerReconcile() }

func (r *Reconciler) reconcileImmediately() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.runReconcile(ctx); err != nil {
		r.log.Error("immediate reconcile failed", zap.Error(err))
	}
}

// applyCachedImmediately applies rules from local mapping cache without a NetBox fetch.
// Returns false when no cache is available yet; caller should rely on queued full-fetch.
func (r *Reconciler) applyCachedImmediately(source string) bool {
	_, ok := r.applyCachedImmediatelyTimed(source)
	return ok
}

func (r *Reconciler) applyCachedImmediatelyTimed(source string) (applyTimings, bool) {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()

	if !r.mappingCacheReady {
		// Warn, not Debug: production runs at info, and this silently downgrades a
		// ~20ms cutover to a multi-second NetBox round trip.
		r.log.Warn("cutover fast path unavailable — mapping cache is cold, falling back to a full NetBox fetch",
			zap.String("source", source))
		return applyTimings{}, false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t, err := r.applyMappingsTimed(ctx, r.cachedMappings(), source)
	if err != nil {
		r.log.Error("cached fast-path reconcile failed", zap.String("source", source), zap.Error(err))
	}
	return t, true
}

func (r *Reconciler) runReconcile(ctx context.Context) error {
	return r.reconcile(ctx)
}

// Run processes events and periodic ticks until ctx is cancelled.
// It must be called after Subscribe() has been set up so snapshot responses
// arrive and populate the store before the first reconcile tick.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	r.intervalNs.Store(int64(interval))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.expireStagedVMs()
			r.triggerReconcile()
		case d := <-r.webhookDeltaCh:
			if err := r.reconcileFromWebhookDelta(ctx, d); err != nil {
				r.log.Error("webhook-delta reconcile failed", zap.Error(err))
			}
		case <-r.reconcileCh:
			if err := r.runReconcile(ctx); err != nil {
				r.log.Error("reconcile failed", zap.Error(err))
			}
		}
	}
}

// verifyLater schedules a single full reconcile a short time from now, to confirm
// that a fast-path apply matches what NetBox actually says. Running it inline would
// put a multi-second NetBox fetch directly behind every cutover.
func (r *Reconciler) verifyLater() {
	if !r.verifyPending.CompareAndSwap(false, true) {
		return // one already scheduled
	}
	time.AfterFunc(verifyDelay, func() {
		r.verifyPending.Store(false)
		r.triggerReconcile()
	})
}

// reconcileIntervalOrDefault returns the periodic interval Run was started with,
// falling back to the shipped default before Run has published it.
func (r *Reconciler) reconcileIntervalOrDefault() time.Duration {
	if ns := r.intervalNs.Load(); ns > 0 {
		return time.Duration(ns)
	}
	return defaultReconcileInterval
}

// shouldFullFetchForSnapshot reports whether a completed snapshot batch warrants a
// full NetBox fetch. It does when the set of VMs on this node changed, or when the
// last fetch is older than the periodic interval.
//
// The predicate is deliberately read-only: only a successful fetch advances the gate.
// Recording the key here instead would mean a fetch that then failed had consumed the
// "VM set changed" signal, and the following snapshot batches would see a key that
// matches and decline to retry.
func (r *Reconciler) shouldFullFetchForSnapshot(interval time.Duration) bool {
	key := r.activeStateKey()

	r.fetchMu.Lock()
	defer r.fetchMu.Unlock()

	if key != r.lastActiveKey {
		return true
	}
	return interval <= 0 || time.Since(r.lastFullFetch) >= interval
}

// activeStateKey fingerprints the VMs this node currently holds rules for.
func (r *Reconciler) activeStateKey() string {
	vms := append(r.store.GetActive(), r.store.GetStaged()...)
	ids := make([]int, 0, len(vms))
	for _, vm := range vms {
		ids = append(ids, vm.VMID)
	}
	sort.Ints(ids)

	var b strings.Builder
	for _, id := range ids {
		b.WriteString(strconv.Itoa(id))
		b.WriteByte(',')
	}
	return b.String()
}

// expireStagedVMs clears pre-staged VMs whose migration never reached a terminal
// event, so their rules do not linger indefinitely.
func (r *Reconciler) expireStagedVMs() {
	expired := r.store.ExpireStaged(time.Now().Add(-stagedTTL))
	if len(expired) == 0 {
		return
	}
	r.log.Warn("expiring pre-staged VMs with no terminal migration event",
		zap.Ints("vmids", expired),
		zap.Duration("ttl", stagedTTL),
	)
}

// RunOnce drains any pending reconcile signal and executes one reconcile cycle.
// Intended for use in tests to drive reconcile synchronously without a ticker.
func (r *Reconciler) RunOnce(ctx context.Context) {
	select {
	case d := <-r.webhookDeltaCh:
		if err := r.reconcileFromWebhookDelta(ctx, d); err != nil {
			r.log.Error("webhook-delta reconcile failed", zap.Error(err))
		}
		return
	default:
	}

	// Drain the channel (a HandleEvent call may have enqueued a signal).
	select {
	case <-r.reconcileCh:
	default:
	}
	if err := r.runReconcile(ctx); err != nil {
		r.log.Error("reconcile failed", zap.Error(err))
	}
}

// HandleEvent is the NATS event callback. It updates the in-memory state store
// and triggers a reconcile when the VM placement on this node changes.
func (r *Reconciler) HandleEvent(subject string, evt events.CloudEvent) {
	d := evt.Data
	t, haveEventTime := parseEventTime(evt.Time)
	if !haveEventTime {
		t = time.Now()
	}
	received := time.Now()

	// The cutover path must stay in the low milliseconds. Anything slower is a
	// regression worth a log line rather than a silent slowdown.
	defer func() {
		if elapsed := time.Since(received); elapsed > handlerWatchdog {
			r.log.Warn("event handler exceeded its latency budget",
				zap.String("subject", subject),
				zap.Int("vmid", d.VMID),
				zap.String("action", d.Action),
				zap.String("phase", d.Phase),
				zap.Float64("elapsed_ms", msOf(elapsed)),
			)
		}
	}()

	fields := []zap.Field{
		zap.String("type", evt.Type),
		zap.String("subject", subject),
		zap.Int("vmid", d.VMID),
		zap.String("action", d.Action),
		zap.String("phase", d.Phase),
		zap.String("node", d.Node),
		zap.String("target_node", d.TargetNode),
	}
	if haveEventTime {
		fields = append(fields, zap.Float64("publish_lag_ms", msOf(received.Sub(t))))
	}
	r.log.Debug("event received", fields...)

	// logCutover emits the single greppable per-migration record, at info so it is
	// visible in production (where debug-level event logging is off).
	logCutover := func(role string, timings applyTimings, fastPath bool) {
		f := []zap.Field{
			zap.Int("vmid", d.VMID),
			zap.String("name", d.Name),
			zap.String("role", role),
			zap.Bool("fast_path", fastPath),
			zap.Bool("nft_skipped", timings.nftSkipped),
			zap.Float64("nft_ms", msOf(timings.nft)),
			zap.Float64("bgp_ms", msOf(timings.bgp)),
			zap.Float64("apply_ms", msOf(timings.total)),
			zap.Float64("handler_ms", msOf(time.Since(received))),
		}
		if haveEventTime {
			f = append(f, zap.Float64("publish_lag_ms", msOf(received.Sub(t))))
		}
		r.log.Info("cutover", f...)
	}

	switch {

	// ── Migration inbound: mark staged so a local start cannot activate early ──
	case d.Action == "migrate" && d.Phase == "started" && sameNode(d.TargetNode, r.node):
		r.log.Info("migration started — marking VM staged",
			zap.Int("vmid", d.VMID),
			zap.String("name", d.Name),
			zap.String("source", d.Node),
		)
		// Staged produces no rules and no advertisement — the VM is still serving on
		// the source. Its only job is to stop this node's own `start.finished` from
		// activating the VM before migrate.synced. Reconcile anyway, so a stale
		// advertisement for this VM on this node is withdrawn.
		r.store.SetStaged(d.VMID, d.Name, t)
		if !r.applyCachedImmediately("migration-started") {
			r.triggerReconcile()
		}

	// ── Migration synced: activate on target, clean up on source ──────────────
	case d.Action == "migrate" && d.Phase == "synced":
		if sameNode(d.TargetNode, r.node) {
			r.log.Info("migration synced — activating rules on this node",
				zap.Int("vmid", d.VMID),
				zap.String("name", d.Name),
				zap.String("source", d.Node),
			)
			r.store.SetActive(d.VMID, d.Name, t)
			timings, fast := r.applyCachedImmediatelyTimed("migration-synced-cache")
			if fast {
				// Confirm against NetBox shortly, not inline: a full fetch costs
				// seconds and the rules are already live.
				r.verifyLater()
			} else {
				r.triggerReconcile()
			}
			logCutover("target", timings, fast)
		}
		if sameNode(d.Node, r.node) { // source node publishes the synced event
			r.log.Info("migration synced — removing rules from source node",
				zap.Int("vmid", d.VMID),
				zap.String("name", d.Name),
				zap.String("target", d.TargetNode),
			)
			r.store.SetAbsent(d.VMID, t)
			timings, fast := r.applyCachedImmediatelyTimed("migration-synced-cache")
			if fast {
				// Confirm against NetBox shortly, not inline: a full fetch costs
				// seconds and the rules are already live.
				r.verifyLater()
			} else {
				r.triggerReconcile()
			}
			logCutover("source", timings, fast)
		}

	// ── Migration failed: clear the staged marker on the target ───────────────
	case d.Action == "migrate" && d.Phase == "failed" && sameNode(d.TargetNode, r.node):
		r.log.Warn("migration failed — clearing staged state",
			zap.Int("vmid", d.VMID),
			zap.String("name", d.Name),
		)
		r.store.SetAbsent(d.VMID, t)
		if !r.applyCachedImmediately("migration-failed-cache") {
			r.triggerReconcile()
		}

	// ── VM started on this node ───────────────────────────────────────────────
	case d.Action == "start" && d.Phase == "finished" && sameNode(d.Node, r.node):
		if vm, ok := r.store.Get(d.VMID); ok && vm.Status == state.StatusStaged {
			r.log.Debug("VM start finished while migration is staged; waiting for migration.synced",
				zap.Int("vmid", d.VMID),
				zap.String("name", d.Name),
			)
			break
		}
		r.log.Info("VM started", zap.Int("vmid", d.VMID), zap.String("name", d.Name))
		r.store.SetActive(d.VMID, d.Name, t)
		r.triggerReconcile()

	// ── VM stopped / shut down / destroyed on this node ──────────────────────
	case (d.Action == "stop" || d.Action == "shutdown" || d.Action == "destroy") &&
		d.Phase == "finished" && sameNode(d.Node, r.node):
		r.log.Info("VM stopped/destroyed",
			zap.Int("vmid", d.VMID),
			zap.String("name", d.Name),
			zap.String("action", d.Action),
		)
		r.store.SetAbsent(d.VMID, t)
		r.triggerReconcile()

	// ── Periodic per-VM state snapshot ───────────────────────────────────────
	// Accumulate snapshots silently; the subsequent snapshot.complete triggers reconcile.
	case ((d.Action == "state" && d.Phase == "snapshot") || strings.HasSuffix(subject, ".state.snapshot")) && sameNode(d.Node, r.node):
		r.store.UpdateFromSnapshot(d.VMID, d.Name, d.State == "running", d.ObservedAtNs)

	// ── Snapshot batch complete: reconcile now that we have a fresh picture ───
	case (strings.HasSuffix(evt.Type, "snapshot.complete") || strings.HasSuffix(subject, ".snapshot.complete")) &&
		(sameNode(d.Node, r.node) || strings.TrimSpace(d.Node) == ""):
		r.log.Debug("snapshot batch complete", zap.Int("vm_count", d.Count))
		// Snapshot batches land every ~30s. Only pay for a full NetBox fetch when the
		// node's VM set actually moved, or when the periodic interval is due anyway.
		if r.shouldFullFetchForSnapshot(r.reconcileIntervalOrDefault()) {
			r.triggerReconcile()
		}
	}
}

func sameNode(observed, expected string) bool {
	return strings.EqualFold(strings.TrimSpace(observed), strings.TrimSpace(expected))
}

// reconcile computes desired state (from Netbox + active VMs) and applies
// nftables rules and loopback IP advertisements to match it.
func (r *Reconciler) reconcile(ctx context.Context) error {
	fetchStart := time.Now()
	allMappings, err := r.netbox.FetchNATMappings(ctx)
	fetchElapsed := time.Since(fetchStart)

	if err != nil {
		// Deliberately leave lastFullFetch/lastActiveKey untouched. They exist to
		// suppress redundant fetches, and a fetch that failed brought back nothing to
		// be redundant with: recording it here would make shouldFullFetchForSnapshot
		// decline every snapshot-driven retry for a whole interval, so a NetBox blip
		// during a VM change would go unnoticed until the periodic timer came round.
		r.log.Warn("netbox fetch failed; falling back to backup mappings",
			zap.Float64("fetch_ms", msOf(fetchElapsed)), zap.Error(err))
		return r.applyMappings(ctx, nil, sourceBackupFallback)
	}

	r.fetchMu.Lock()
	r.lastFullFetch = time.Now()
	r.lastActiveKey = r.activeStateKey()
	r.fetchMu.Unlock()

	r.log.Debug("netbox full fetch complete",
		zap.Int("mappings", len(allMappings)),
		zap.Float64("fetch_ms", msOf(fetchElapsed)),
	)

	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()

	if len(allMappings) == 0 {
		r.log.Warn("full-fetch returned zero NetBox mappings; check nat_outside data and proxmox_vmid visibility")
	}
	r.rebuildMappingCache(allMappings)
	if len(allMappings) > 0 && len(r.store.GetActive()) == 0 && len(r.store.GetStaged()) == 0 {
		r.log.Warn("full-fetch has mappings but node VM state is empty; waiting for snapshot/lifecycle events to scope rules to this node",
			zap.Int("netbox_mappings", len(allMappings)),
		)
	}

	return r.applyMappings(ctx, allMappings, sourceFullFetch)
}

func (r *Reconciler) reconcileFromWebhookDelta(ctx context.Context, d WebhookDelta) error {
	r.reconcileMu.Lock()
	defer r.reconcileMu.Unlock()

	if !r.mappingCacheReady {
		r.mappingByVMID = make(map[int][]netbox.NATMapping)
		r.mappingPendingByName = make(map[string][]netbox.NATMapping)
		r.vmIDByName = make(map[string]int)
		r.mappingCacheReady = true
	}

	r.applyDeltaToState(d)
	r.applyDeltaToCache(d)
	r.syncBackupFromDelta(d)
	return r.applyMappings(ctx, r.cachedMappings(), sourceWebhookDelta)
}

func (r *Reconciler) applyDeltaToState(d WebhookDelta) {
	if !d.HasVMID || strings.TrimSpace(d.VMName) == "" {
		return
	}

	now := time.Now()
	model := strings.ToLower(strings.TrimSpace(d.Model))
	event := strings.ToLower(strings.TrimSpace(d.Event))

	if model != "virtualmachine" {
		return
	}

	if event == "deleted" {
		r.store.SetAbsent(d.VMProxmoxVMID, now)
		return
	}

	// NetBox VM created/updated events don't carry node placement. In webhook-delta
	// mode we still mark the VM active so paired IP+VM updates can converge rules
	// without waiting for a full fetch.
	r.store.SetActive(d.VMProxmoxVMID, d.VMName, now)
}

// applyTimings records how long each stage of an apply took, so the "reconciled"
// log line carries a latency breakdown instead of a bare success message.
type applyTimings struct {
	nft        time.Duration
	bgp        time.Duration
	total      time.Duration
	nftSkipped bool
}

func (r *Reconciler) applyMappings(ctx context.Context, allMappings []netbox.NATMapping, source string) error {
	_, err := r.applyMappingsTimed(ctx, allMappings, source)
	return err
}

func (r *Reconciler) applyMappingsTimed(ctx context.Context, allMappings []netbox.NATMapping, source string) (applyTimings, error) {
	start := time.Now()
	var t applyTimings

	activeVMs := r.store.GetActive()
	stagedVMs := r.store.GetStaged()
	mappingsByVMID := make(map[int][]netbox.NATMapping)
	for _, m := range allMappings {
		mappingsByVMID[m.VMProxmoxVMID] = append(mappingsByVMID[m.VMProxmoxVMID], m)
	}

	// Rules and advertisements are derived from active VMs only. A staged VM (migration
	// inbound) is tracked so its local `start.finished` cannot activate it early, but it
	// produces nothing: advertising its /32 would pull traffic here before the VM
	// arrives.
	var nftMappings []netbox.NATMapping
	bgpDesired := make(map[netip.Addr]struct{})
	applied := make(map[int][]netbox.NATMapping, len(activeVMs))
	retained, unresolved := 0, 0

	// Whether an empty result for a single VM can be believed.
	//
	// A webhook delta is an explicit mutation: NetBox told us an IP or VM went away, so
	// an empty cache afterwards is knowledge, not ignorance, and must withdraw — that is
	// how deleting an IP address takes effect at all.
	//
	// Everywhere else an empty result is only believable if it is not *globally* empty.
	// A backup fallback means the fetch failed outright. A globally empty set while VMs
	// are running here — whether it came from a full fetch (NetBox answers HTTP 200 with
	// an empty list) or from a mapping cache that is ready but has nothing in it yet (a
	// cutover fast path right after such a fetch) — is absence of knowledge, and
	// believing it would withdraw every VM on the node over a transient API problem.
	// In those cases each VM retains what it had.
	//
	// Note that authoritative is about *believing* the empty result, not about acting on
	// it immediately: a believable empty still lets a VM's backup serve one final cycle
	// before it is pruned. See the retention block below.
	authoritative := source != sourceBackupFallback &&
		(source == sourceWebhookDelta || len(allMappings) > 0 || len(activeVMs) == 0)

	for _, vm := range activeVMs {
		mappings := mappingsByVMID[vm.VMID]
		switch {
		case len(mappings) > 0:
			// Queued, not written: persistence must never sit in front of the cutover.
			r.backup.save(vm.VMID, mappings)

		case source == sourceWebhookDelta:
			// An explicit mutation, so withdraw now — and drop the backup in the same
			// breath, otherwise the next failed fetch would resurrect the very mapping
			// NetBox just told us was deleted.
			r.backup.delete(vm.VMID)

		default:
			// Fall back to the on-disk backup, then to whatever was last programmed.
			// Retention is per VM rather than a global bail-out: the old code returned
			// early whenever *no* active VM resolved, which also skipped withdrawing IPs
			// belonging to VMs that had genuinely left — leaving stale /32s advertised
			// that upstream ECMP turns into an outage.
			backupMappings, ok, err := r.backup.load(vm.VMID)
			if err != nil {
				r.log.Warn("failed to load backup mappings", zap.Int("vmid", vm.VMID), zap.Error(err))
			} else if ok {
				mappings = backupMappings
			}

			switch {
			case authoritative && len(mappings) > 0:
				// The fetch succeeded and simply had nothing for this VM. Serve the
				// backup one last time and prune it: the grace cycle absorbs a NetBox
				// data glitch — a cleared proxmox_vmid, a custom field the API token
				// cannot see — instead of dropping a working VM's rules the instant it
				// appears, and the prune is what makes the next such cycle withdraw,
				// so a genuine deletion still takes effect.
				r.backup.delete(vm.VMID)

			case !authoritative && len(mappings) == 0:
				// Nothing left but the last programmed set. Confined to the
				// unbelievable-fetch case on purpose: under an authoritative empty it
				// would pin the VM alive forever, because lastAppliedByVMID is
				// refreshed each cycle from whatever the previous one retained.
				if last, ok := r.lastAppliedByVMID[vm.VMID]; ok && len(last) > 0 {
					mappings = last
				}
			}

			if len(mappings) > 0 {
				retained++
			} else if !authoritative {
				// An authoritative fetch with neither a mapping nor a backup is just a
				// VM with no NAT configured — the steady state for plenty of guests,
				// and nothing to escalate about.
				unresolved++
			}
		}

		if len(mappings) == 0 {
			continue
		}
		applied[vm.VMID] = mappings
		for _, m := range mappings {
			nftMappings = append(nftMappings, m)
			bgpDesired[m.ExternalIP] = struct{}{}
		}
	}

	if retained > 0 || unresolved > 0 {
		r.unresolvedStreak++
		level := r.log.Warn
		if r.unresolvedStreak >= unresolvedEscalateAfter {
			// Persisting across many cycles is a data problem, not a blip.
			level = r.log.Error
		}
		level("NetBox mappings unavailable; retaining existing state for affected VMs",
			zap.String("source", source),
			zap.Int("active_vms", len(activeVMs)),
			zap.Int("retained", retained),
			zap.Int("unresolved", unresolved),
			zap.Int("consecutive_cycles", r.unresolvedStreak),
		)
	} else {
		r.unresolvedStreak = 0
	}

	// Sort before fingerprinting: cachedMappings iterates maps, so without this the
	// ruleset key would differ run to run and the skip below would never fire.
	sortMappings(nftMappings)
	key := rulesetKey(nftMappings)

	// Ordering is per-direction, because the two directions fail differently.
	//
	// Adding an external IP to lo makes it a *local* address, and nat prerouting runs
	// before the routing decision — so advertising before the DNAT rule exists makes
	// the host answer with a RST/ICMP unreachable, which is worse than a drop. So
	// additions need nftables first, *unless* the rules are already in place, which is
	// exactly the pre-staged cutover case.
	//
	// Removals are the opposite: withdrawing first means packets still being routed
	// here during convergence are forwarded toward the departed VM (a drop) rather
	// than hitting a local address (a RST). Tearing down nftables first would also not
	// stop this node serving anyway, since established conntrack entries keep
	// translating after the rules are flushed.
	rulesetUnchanged := r.rulesetKeyValid && key == r.lastRulesetKey
	advertiseFirst := rulesetUnchanged || !r.hasNewAdvertisements(bgpDesired)

	applyNFT := func() error {
		if rulesetUnchanged {
			t.nftSkipped = true
			return nil
		}
		nftStart := time.Now()
		if err := r.nft.Apply(ctx, nftMappings); err != nil {
			// Force a real apply next time; the kernel state is now unknown.
			r.rulesetKeyValid = false
			t.nft = time.Since(nftStart)
			return fmt.Errorf("applying nftables: %w", err)
		}
		t.nft = time.Since(nftStart)
		r.lastRulesetKey, r.rulesetKeyValid = key, true
		return nil
	}

	// Only reconcileBGP's one-time kernel adoption reads this, and building it
	// JSON-decodes every backed-up VM while reconcileMu is held — far too much to pay
	// on a cutover bounded by a 50ms watchdog. Once adoption has run it is dead weight.
	var managed map[netip.Addr]struct{}
	if !r.kernelAdoptionDone() {
		managed = r.managedIPs()
	}

	applyBGP := func() {
		bgpStart := time.Now()
		if err := r.reconcileBGP(ctx, bgpDesired, managed); err != nil {
			// Non-fatal: log and continue; periodic reconcile will retry.
			r.log.Error("BGP reconcile error", zap.Error(err))
		}
		t.bgp = time.Since(bgpStart)
	}

	if advertiseFirst {
		applyBGP()
		if err := applyNFT(); err != nil {
			return t, err
		}
	} else {
		if err := applyNFT(); err != nil {
			return t, err
		}
		applyBGP()
	}

	// Record what is now programmed, so a VM that becomes unresolvable next cycle can
	// retain it. Replacing the map wholesale also prunes VMs that have left, which is
	// what stops it growing without bound.
	r.lastAppliedByVMID = applied

	t.total = time.Since(start)

	r.log.Info("reconciled",
		zap.String("source", source),
		zap.Int("netbox_mappings", len(allMappings)),
		zap.Int("nft_rules", len(nftMappings)),
		zap.Int("bgp_ips", len(bgpDesired)),
		zap.Int("active_vms", len(activeVMs)),
		zap.Int("staged_vms", len(stagedVMs)),
		zap.Int("retained_vms", retained),
		zap.Bool("nft_skipped", t.nftSkipped),
		zap.Float64("nft_ms", msOf(t.nft)),
		zap.Float64("bgp_ms", msOf(t.bgp)),
		zap.Float64("total_ms", msOf(t.total)),
	)

	return t, nil
}

// sortMappings puts mappings in a stable order so an unchanged desired state always
// renders to an identical ruleset.
func sortMappings(mappings []netbox.NATMapping) {
	sort.Slice(mappings, func(i, j int) bool {
		a, b := mappings[i], mappings[j]
		if a.ExternalIP != b.ExternalIP {
			return a.ExternalIP.Less(b.ExternalIP)
		}
		if a.InternalIP != b.InternalIP {
			return a.InternalIP.Less(b.InternalIP)
		}
		return a.VMProxmoxVMID < b.VMProxmoxVMID
	})
}

// rulesetKey fingerprints the mappings that determine the rendered nftables ruleset.
// Only the address pairs matter — VM name changes do not alter a single rule.
func rulesetKey(mappings []netbox.NATMapping) string {
	var b strings.Builder
	for _, m := range mappings {
		b.WriteString(m.ExternalIP.String())
		b.WriteByte('>')
		b.WriteString(m.InternalIP.String())
		b.WriteByte(';')
	}
	return b.String()
}

func msOf(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func (r *Reconciler) syncBackupFromDelta(d WebhookDelta) {
	vmID := 0
	if d.HasVMID {
		vmID = d.VMProxmoxVMID
	} else if d.VMName != "" {
		vmID = r.vmIDByName[d.VMName]
	}
	if vmID <= 0 {
		return
	}

	model := strings.ToLower(strings.TrimSpace(d.Model))
	event := strings.ToLower(strings.TrimSpace(d.Event))
	if model == "virtualmachine" {
		if event == "deleted" {
			r.backup.delete(vmID)
		}
		return
	}
	if model != "ipaddress" {
		return
	}

	vm, ok := r.store.Get(vmID)
	if !ok || vm.Status != state.StatusActive {
		return
	}

	internalIP, ok := parseIPorCIDR(d.InternalIP)
	if !ok {
		return
	}

	existing, found, err := r.backup.load(vmID)
	if err != nil {
		r.log.Warn("failed to load backup mappings before delta sync", zap.Int("vmid", vmID), zap.Error(err))
		return
	}

	updated := make([]netbox.NATMapping, 0, len(existing))
	for _, mapping := range existing {
		if mapping.InternalIP == internalIP {
			continue
		}
		keep := true
		for _, raw := range d.NATOutside {
			externalIP, ok := parseIPorCIDR(raw)
			if ok && mapping.ExternalIP == externalIP {
				keep = false
				break
			}
		}
		if keep {
			updated = append(updated, mapping)
		}
	}

	if event != "deleted" {
		for _, raw := range d.NATOutside {
			externalIP, ok := parseIPorCIDR(raw)
			if !ok {
				continue
			}
			updated = append(updated, netbox.NATMapping{
				ExternalIP:    externalIP,
				InternalIP:    internalIP,
				VMProxmoxVMID: vmID,
				VMName:        d.VMName,
			})
		}
	}

	if len(updated) == 0 {
		if found {
			r.backup.delete(vmID)
		}
		return
	}
	r.backup.save(vmID, updated)
}

func (r *Reconciler) rebuildMappingCache(mappings []netbox.NATMapping) {
	r.mappingByVMID = make(map[int][]netbox.NATMapping)
	r.mappingPendingByName = make(map[string][]netbox.NATMapping)
	r.vmIDByName = make(map[string]int)
	for _, m := range mappings {
		r.mappingByVMID[m.VMProxmoxVMID] = append(r.mappingByVMID[m.VMProxmoxVMID], m)
		if m.VMName != "" {
			r.vmIDByName[m.VMName] = m.VMProxmoxVMID
		}
	}
	r.mappingCacheReady = true
}

func (r *Reconciler) cachedMappings() []netbox.NATMapping {
	result := make([]netbox.NATMapping, 0)
	for _, entries := range r.mappingByVMID {
		result = append(result, entries...)
	}
	for _, entries := range r.mappingPendingByName {
		result = append(result, entries...)
	}
	return result
}

func (r *Reconciler) applyDeltaToCache(d WebhookDelta) {
	model := strings.ToLower(strings.TrimSpace(d.Model))
	event := strings.ToLower(strings.TrimSpace(d.Event))

	if d.HasVMID && d.VMName != "" {
		r.vmIDByName[d.VMName] = d.VMProxmoxVMID
		if pending, ok := r.mappingPendingByName[d.VMName]; ok {
			for i := range pending {
				pending[i].VMProxmoxVMID = d.VMProxmoxVMID
			}
			r.mappingByVMID[d.VMProxmoxVMID] = pending
			delete(r.mappingPendingByName, d.VMName)
		}
	}

	if model == "virtualmachine" {
		if event == "deleted" {
			vmID := 0
			if d.HasVMID {
				vmID = d.VMProxmoxVMID
			} else if d.VMName != "" {
				vmID = r.vmIDByName[d.VMName]
			}
			if vmID > 0 {
				delete(r.mappingByVMID, vmID)
				for name, mappedID := range r.vmIDByName {
					if mappedID == vmID {
						delete(r.vmIDByName, name)
						delete(r.mappingPendingByName, name)
					}
				}
			}
			if d.VMName != "" {
				delete(r.mappingPendingByName, d.VMName)
			}
		}
		return
	}

	if model != "ipaddress" {
		return
	}

	vmID := 0
	vmName := strings.TrimSpace(d.VMName)
	if d.HasVMID {
		vmID = d.VMProxmoxVMID
	} else if vmName != "" {
		vmID = r.vmIDByName[vmName]
	}
	if vmID <= 0 && vmName == "" {
		return
	}

	if event == "deleted" || strings.TrimSpace(d.InternalIP) == "" || len(d.NATOutside) == 0 {
		if vmID > 0 {
			delete(r.mappingByVMID, vmID)
		}
		if vmName != "" {
			delete(r.mappingPendingByName, vmName)
		}
		return
	}

	internalIP, ok := parseIPorCIDR(d.InternalIP)
	if !ok {
		return
	}

	entries := make([]netbox.NATMapping, 0, len(d.NATOutside))
	for _, raw := range d.NATOutside {
		extIP, ok := parseIPorCIDR(raw)
		if !ok {
			continue
		}
		entries = append(entries, netbox.NATMapping{
			ExternalIP:    extIP,
			InternalIP:    internalIP,
			VMProxmoxVMID: vmID,
			VMName:        vmName,
		})
	}

	if len(entries) == 0 {
		if vmID > 0 {
			delete(r.mappingByVMID, vmID)
		}
		if vmName != "" {
			delete(r.mappingPendingByName, vmName)
		}
		return
	}
	if vmID > 0 {
		r.mappingByVMID[vmID] = entries
	} else if vmName != "" {
		r.mappingPendingByName[vmName] = entries
	}
	if vmID > 0 && vmName != "" {
		r.vmIDByName[vmName] = vmID
	}
}

func parseIPorCIDR(value string) (netip.Addr, bool) {
	v := strings.TrimSpace(value)
	if v == "" {
		return netip.Addr{}, false
	}
	if prefix, err := netip.ParsePrefix(v); err == nil {
		return prefix.Addr(), true
	}
	addr, err := netip.ParseAddr(v)
	if err != nil {
		return netip.Addr{}, false
	}
	return addr, true
}

// reconcileBGP converges the set of advertised loopback IPs to desired.
//
// Additions run before removals, so a prefix that is both newly desired and currently
// advertised under a different VM never has a moment with no announcement.
func (r *Reconciler) reconcileBGP(
	ctx context.Context,
	desired map[netip.Addr]struct{},
	managed map[netip.Addr]struct{},
) error {
	r.advMu.Lock()
	defer r.advMu.Unlock()

	var errs []error

	// Adopt anything a previous process left in the kernel, so it can be withdrawn
	// rather than lingering unnoticed. Restricted to addresses we can attribute to
	// ourselves — see adoptKernelStateLocked.
	if !r.kernelSynced && len(managed) > 0 {
		r.adoptKernelStateLocked(ctx, managed)
	}

	// Advertise newly desired IPs first.
	for ip := range desired {
		if _, ok := r.advertisedIPs[ip]; !ok {
			if err := r.frr.Advertise(ctx, ip); err != nil {
				errs = append(errs, fmt.Errorf("advertise %s: %w", ip, err))
			} else {
				r.advertisedIPs[ip] = struct{}{}
				r.log.Info("loopback IP advertised", zap.String("ip", ip.String()))
			}
		}
	}

	// Withdraw IPs no longer desired.
	for ip := range r.advertisedIPs {
		if _, ok := desired[ip]; !ok {
			if err := r.frr.Withdraw(ctx, ip); err != nil {
				errs = append(errs, fmt.Errorf("withdraw %s: %w", ip, err))
			} else {
				delete(r.advertisedIPs, ip)
				r.log.Info("loopback IP withdrawn", zap.String("ip", ip.String()))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%d BGP error(s): %v", len(errs), errs)
	}
	return nil
}

// adoptKernelStateLocked merges what the kernel currently holds into the in-memory
// view. Called once, on the first reconcile, because the advertised set does not
// survive a restart: without this a crash mid-migration would leave a /32 on lo
// permanently, and two nodes announcing the same prefix means upstream ECMP and a
// silent partial outage.
//
// Adoption is restricted to addresses in `managed` — the external IPs this service is
// responsible for according to NetBox and the on-disk backups. A loopback interface is
// shared: operators and other tooling put anycast and service addresses there too, and
// adopting one of those would mean withdrawing it on the very next reconcile because it
// is not in the desired set. Anything we cannot attribute to ourselves is left alone.
//
// Caller must hold advMu.
func (r *Reconciler) adoptKernelStateLocked(ctx context.Context, managed map[netip.Addr]struct{}) {
	active, err := r.frr.ActiveIPs(ctx)
	if err != nil {
		r.log.Warn("could not enumerate loopback IPs; leftover advertisements may persist",
			zap.Error(err))
		return
	}
	adopted, foreign := 0, 0
	for ip := range active {
		if _, ours := managed[ip]; !ours {
			foreign++ // another operator's or tool's address; not ours to touch
			continue
		}
		if _, known := r.advertisedIPs[ip]; !known {
			r.advertisedIPs[ip] = struct{}{}
			adopted++
		}
	}

	r.kernelSynced = true
	if adopted > 0 {
		r.log.Warn("adopted pre-existing loopback addresses left by a previous process",
			zap.Int("loopback_ips", adopted),
		)
	}
	if foreign > 0 {
		r.log.Info("ignoring loopback addresses not managed by this service",
			zap.Int("count", foreign),
		)
	}
}

// parseEventTime parses the CloudEvent publish timestamp, reporting whether it was
// usable. Callers must not substitute time.Now() silently when computing publish
// lag — that would report a lag of zero for every unparseable timestamp.
func parseEventTime(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}
