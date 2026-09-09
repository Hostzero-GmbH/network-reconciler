package reconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/hostzero/network-reconciler/internal/netbox"
)

const defaultBackupDir = "/etc/pve/network-reconciler"

// writeDebounce is how long the flusher waits after a change before writing, so a
// burst of reconciles touching the same VM collapses into a single pmxcfs write.
const writeDebounce = 50 * time.Millisecond

const (
	writeBackoffMin = 1 * time.Second
	writeBackoffMax = 30 * time.Second
)

// backupStore persists per-VM NAT mappings so rules survive a NetBox outage.
//
// The directory lives on /etc/pve (pmxcfs), a corosync-replicated FUSE filesystem
// where every write is a synchronous cluster transaction — slowest precisely when
// the cluster is busy migrating a VM. Writes are therefore never performed on the
// caller's goroutine: Save records the desired bytes and returns, and a background
// flusher coalesces and writes them. Reads are served from memory after Warm.
type backupStore struct {
	dir string
	log *zap.Logger

	mu sync.Mutex
	// cache holds the bytes we believe are on disk, keyed by VMID. A nil value
	// means "known to be absent". Only meaningful when known[vmid] is true.
	cache map[int][]byte
	known map[int]bool
	// dirScanned records that Warm enumerated the directory, so a Load for an
	// unknown VMID can answer "absent" without a syscall.
	dirScanned bool
	// pending holds desired bytes not yet written; a nil value is a tombstone.
	// Keyed by VMID, so its size is bounded by the node's VM count no matter how
	// many events arrive — the queue cannot grow without bound.
	pending map[int][]byte

	// started records that the flusher goroutine exists, and closed that the store
	// has been shut down. Both are guarded by mu: an explicit flag rather than a
	// sync.Once probe, because probing a Once consumes it — a second close would
	// then think the flusher never ran and block on a done nobody closes.
	started bool
	closed  bool

	wake chan struct{}
	done chan struct{}
	stop chan struct{}

	diskReads  atomic.Int64
	diskWrites atomic.Int64
	// allScans counts calls to all(), which decodes every VM's stored JSON. Exposed
	// for the same reason as the counters above: the apply path is supposed to have
	// stopped doing this once kernel adoption has run, and that is only assertable
	// from inside the package.
	allScans atomic.Int64
}

func newBackupStore(dir string) *backupStore {
	return newBackupStoreWithLogger(dir, zap.NewNop())
}

func newBackupStoreWithLogger(dir string, log *zap.Logger) *backupStore {
	if dir == "" {
		dir = defaultBackupDir
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &backupStore{
		dir:     dir,
		log:     log,
		cache:   make(map[int][]byte),
		known:   make(map[int]bool),
		pending: make(map[int][]byte),
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		stop:    make(chan struct{}),
	}
}

// warm reads the whole backup directory once so subsequent loads need no I/O.
// Failures are non-fatal: the store falls back to reading individual files.
func (s *backupStore) warm() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if os.IsNotExist(err) {
			s.mu.Lock()
			s.dirScanned = true
			s.mu.Unlock()
			return nil
		}
		return fmt.Errorf("scanning backup dir %q: %w", s.dir, err)
	}

	loaded := make(map[int][]byte)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".config") {
			continue
		}
		vmid, err := strconv.Atoi(strings.TrimSuffix(entry.Name(), ".config"))
		if err != nil || vmid <= 0 {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		s.diskReads.Add(1)
		if err != nil {
			s.log.Warn("failed to read backup file during warm",
				zap.String("file", entry.Name()), zap.Error(err))
			continue
		}
		loaded[vmid] = data
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for vmid, data := range loaded {
		// A pending write is newer than anything on disk; don't clobber it.
		if _, queued := s.pending[vmid]; queued {
			continue
		}
		s.cache[vmid] = data
		s.known[vmid] = true
	}
	s.dirScanned = true
	return nil
}

// load returns the mappings for vmid, preferring the in-memory view.
func (s *backupStore) load(vmid int) ([]netbox.NATMapping, bool, error) {
	s.mu.Lock()
	if data, ok := s.pending[vmid]; ok {
		s.mu.Unlock()
		if data == nil {
			return nil, false, nil
		}
		return decodeMappings(data, s.path(vmid))
	}
	if s.known[vmid] {
		data := s.cache[vmid]
		s.mu.Unlock()
		if data == nil {
			return nil, false, nil
		}
		return decodeMappings(data, s.path(vmid))
	}
	if s.dirScanned {
		s.mu.Unlock()
		return nil, false, nil // warm proved it is absent
	}
	s.mu.Unlock()

	path := s.path(vmid)
	data, err := os.ReadFile(path)
	s.diskReads.Add(1)
	if err != nil {
		if os.IsNotExist(err) {
			s.mu.Lock()
			s.cache[vmid], s.known[vmid] = nil, true
			s.mu.Unlock()
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("reading backup file %q: %w", path, err)
	}

	s.mu.Lock()
	s.cache[vmid], s.known[vmid] = data, true
	s.mu.Unlock()

	return decodeMappings(data, path)
}

// all returns every mapping currently known to the store, across all VMs. Only
// meaningful after warm; used to seed the mapping cache when NetBox is unreachable.
func (s *backupStore) all() []netbox.NATMapping {
	s.allScans.Add(1)

	s.mu.Lock()
	vmids := make([]int, 0, len(s.known))
	for vmid, known := range s.known {
		if known {
			vmids = append(vmids, vmid)
		}
	}
	s.mu.Unlock()

	var out []netbox.NATMapping
	for _, vmid := range vmids {
		mappings, ok, err := s.load(vmid)
		if err != nil || !ok {
			continue
		}
		out = append(out, mappings...)
	}
	return out
}

func decodeMappings(data []byte, path string) ([]netbox.NATMapping, bool, error) {
	var mappings []netbox.NATMapping
	if err := json.Unmarshal(data, &mappings); err == nil {
		return mappings, true, nil
	}

	var single netbox.NATMapping
	if err := json.Unmarshal(data, &single); err == nil {
		return []netbox.NATMapping{single}, true, nil
	}

	return nil, false, fmt.Errorf("parsing backup file %q: invalid NATMapping JSON", path)
}

// save queues mappings for persistence. It never blocks on disk and never fails:
// write errors are retried in the background. An unchanged payload is dropped,
// which is what keeps steady-state reconciles off pmxcfs entirely.
func (s *backupStore) save(vmid int, mappings []netbox.NATMapping) {
	if len(mappings) == 0 {
		s.delete(vmid)
		return
	}

	data, err := encodeMappings(mappings)
	if err != nil {
		s.log.Warn("failed to encode backup mappings", zap.Int("vmid", vmid), zap.Error(err))
		return
	}
	s.enqueue(vmid, data)
}

// delete queues removal of the VM's backup file.
func (s *backupStore) delete(vmid int) {
	s.enqueue(vmid, nil)
}

func encodeMappings(mappings []netbox.NATMapping) ([]byte, error) {
	// Preserve the historical on-disk shape: a lone mapping is stored as an object,
	// several as an array.
	var payload any = mappings
	if len(mappings) == 1 {
		payload = mappings[0]
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (s *backupStore) enqueue(vmid int, data []byte) {
	s.mu.Lock()
	if s.closed {
		// Nothing will start a flusher again, so queueing here would lose the write
		// silently. Refuse loudly instead: after close the caller is shutting down and
		// the on-disk state has already been drained.
		s.mu.Unlock()
		s.log.Warn("dropping VM config backup write: store is closed",
			zap.Int("vmid", vmid), zap.String("dir", s.dir))
		return
	}
	if queued, ok := s.pending[vmid]; ok && bytes.Equal(queued, data) {
		s.mu.Unlock()
		return // identical write already queued
	}
	if _, queued := s.pending[vmid]; !queued && s.known[vmid] && bytes.Equal(s.cache[vmid], data) {
		s.mu.Unlock()
		return // already on disk
	}
	s.pending[vmid] = data
	startFlusher := !s.started
	s.started = true
	s.mu.Unlock()

	if startFlusher {
		go s.run()
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// run is the background flusher. It debounces, then writes every pending entry.
func (s *backupStore) run() {
	defer close(s.done)

	backoff := time.Duration(0)
	for {
		select {
		case <-s.stop:
			s.flush()
			return
		case <-s.wake:
		}

		delay := writeDebounce
		if backoff > delay {
			delay = backoff
		}
		timer := time.NewTimer(delay)
		select {
		case <-s.stop:
			timer.Stop()
			s.flush()
			return
		case <-timer.C:
		}

		if s.flush() {
			backoff = 0
			continue
		}
		// Something failed; slow down but keep retrying.
		if backoff == 0 {
			backoff = writeBackoffMin
		} else if backoff < writeBackoffMax {
			backoff *= 2
			if backoff > writeBackoffMax {
				backoff = writeBackoffMax
			}
		}
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// flush writes every pending entry, returning true when all succeeded.
func (s *backupStore) flush() bool {
	s.mu.Lock()
	if len(s.pending) == 0 {
		s.mu.Unlock()
		return true
	}
	batch := s.pending
	s.pending = make(map[int][]byte)
	s.mu.Unlock()

	ok := true
	for vmid, data := range batch {
		var err error
		if data == nil {
			err = s.removeFile(vmid)
		} else {
			err = s.writeFile(vmid, data)
		}

		s.mu.Lock()
		if err != nil {
			ok = false
			// Drop the optimistic cache entry so change detection cannot suppress
			// the retry, and requeue unless a newer value already superseded it.
			delete(s.cache, vmid)
			delete(s.known, vmid)
			if _, superseded := s.pending[vmid]; !superseded {
				s.pending[vmid] = data
			}
		} else {
			s.cache[vmid], s.known[vmid] = data, true
		}
		s.mu.Unlock()

		if err != nil {
			s.log.Warn("failed to persist VM config backup",
				zap.Int("vmid", vmid), zap.String("dir", s.dir), zap.Error(err))
		}
	}
	return ok
}

func (s *backupStore) writeFile(vmid int, data []byte) error {
	if err := os.MkdirAll(s.dir, 0o770); err != nil {
		return fmt.Errorf("creating backup dir %q: %w", s.dir, err)
	}

	path := s.path(vmid)
	tmp, err := os.CreateTemp(s.dir, strconv.Itoa(vmid)+".config.*")
	if err != nil {
		return fmt.Errorf("creating temp backup file in %q: %w", s.dir, err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("writing temp backup file %q: %w", tmpPath, err)
	}
	if err := tmp.Chmod(0o640); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("setting backup file mode for %q: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp backup file %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("installing backup file %q: %w", path, err)
	}
	s.diskWrites.Add(1)
	return nil
}

func (s *backupStore) removeFile(vmid int) error {
	path := s.path(vmid)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing backup file %q: %w", path, err)
	}
	s.diskWrites.Add(1)
	return nil
}

// close drains outstanding writes and stops the flusher. Safe to call more than
// once. Returns ctx.Err() if the drain does not finish in time.
func (s *backupStore) close(ctx context.Context) error {
	s.mu.Lock()
	started, alreadyClosed := s.started, s.closed
	s.closed = true
	s.mu.Unlock()

	if !started {
		// No flusher goroutine exists to drain, and enqueue can no longer start one.
		// Normally nothing is pending either, but enqueue records pending under the
		// lock before spawning the flusher, so write directly rather than dropping a
		// queued entry on the floor.
		return s.flushNow()
	}

	if !alreadyClosed {
		close(s.stop)
	}
	select {
	case <-s.done:
		return s.lastFlushError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flushNow synchronously writes anything pending. Used by tests and by shutdown
// paths that need the on-disk state to be current before proceeding.
func (s *backupStore) flushNow() error {
	if !s.flush() {
		return fmt.Errorf("one or more backup writes to %q failed", s.dir)
	}
	return nil
}

func (s *backupStore) lastFlushError() error {
	s.mu.Lock()
	remaining := len(s.pending)
	s.mu.Unlock()
	if remaining > 0 {
		return fmt.Errorf("%d backup write(s) to %q still pending", remaining, s.dir)
	}
	return nil
}

func (s *backupStore) path(vmid int) string {
	return filepath.Join(s.dir, fmt.Sprintf("%d.config", vmid))
}
