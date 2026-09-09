// Package state provides a thread-safe in-memory store for VM placement state.
package state

import (
	"sort"
	"sync"
	"time"
)

// Status represents the reconciler's view of a VM's lifecycle on this node.
type Status int8

const (
	// StatusAbsent means the VM is not running on this node.
	StatusAbsent Status = iota
	// StatusStaged means a migration to this node has started; rules are
	// pre-applied but BGP is not yet advertised.
	StatusStaged
	// StatusActive means the VM is running on this node and all rules are live.
	StatusActive
)

func (s Status) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusStaged:
		return "staged"
	default:
		return "absent"
	}
}

// VMState captures the reconciler's latest known state for a single VM.
type VMState struct {
	VMID          int
	Name          string
	Status        Status
	LastEventTime time.Time
	// ObservedAtNs is the nanosecond-epoch timestamp from snapshot events.
	// Used to reject out-of-order snapshot updates per the EVENTS.md reconciliation rules.
	ObservedAtNs int64
	// StagedAt is when the VM most recently entered StatusStaged. Zero unless the
	// VM is (or was last) staged. Used to expire abandoned migrations, since a
	// migration that never completes emits no terminal event to clear the state.
	StagedAt time.Time
}

// Store is a concurrency-safe map of VMID → VMState.
type Store struct {
	mu  sync.RWMutex
	vms map[int]*VMState
}

// New returns an empty Store.
func New() *Store {
	return &Store{vms: make(map[int]*VMState)}
}

// SetActive marks the VM as running and fully active on this node.
func (s *Store) SetActive(vmid int, name string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(vmid, name, StatusActive, t, 0)
}

// SetStaged marks the VM as pre-staged (migration started, targeting this node).
func (s *Store) SetStaged(vmid int, name string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(vmid, name, StatusStaged, t, 0)
	if vm, ok := s.vms[vmid]; ok && vm.Status == StatusStaged {
		vm.StagedAt = t
	}
}

// SetAbsent marks the VM as not present on this node.
func (s *Store) SetAbsent(vmid int, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(vmid, "", StatusAbsent, t, 0)
}

// UpdateFromSnapshot updates state from a periodic snapshot event, but only if
// the snapshot is newer than the last lifecycle event (prevents out-of-order updates).
func (s *Store) UpdateFromSnapshot(vmid int, name string, running bool, observedAtNs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.vms[vmid]
	if existing != nil && existing.ObservedAtNs > observedAtNs {
		return // stale snapshot — a lifecycle event already moved us past this point
	}

	// A VM staged for an incoming migration is legitimately not yet running on this
	// node, and migrations routinely outlast the snapshot cadence. Letting a
	// running=false snapshot win here would silently reset Staged → Absent partway
	// through and tear down everything pre-staged for the cutover. Only
	// migrate.synced, migrate.failed, or the staged TTL sweep may leave Staged.
	if existing != nil && existing.Status == StatusStaged && !running {
		existing.ObservedAtNs = observedAtNs
		if name != "" {
			existing.Name = name
		}
		return
	}

	status := StatusAbsent
	if running {
		status = StatusActive
	}
	t := time.Unix(0, observedAtNs)
	s.set(vmid, name, status, t, observedAtNs)
}

// ExpireStaged moves every VM that has been StatusStaged since before cutoff to
// StatusAbsent and returns their VMIDs. A migration that is aborted without a
// terminal event (eventbus restart, killed task) otherwise leaves staged state
// behind forever.
func (s *Store) ExpireStaged(cutoff time.Time) []int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []int
	for vmid, vm := range s.vms {
		if vm.Status != StatusStaged {
			continue
		}
		if vm.StagedAt.IsZero() || vm.StagedAt.After(cutoff) {
			continue
		}
		vm.Status = StatusAbsent
		vm.StagedAt = time.Time{}
		expired = append(expired, vmid)
	}
	sort.Ints(expired)
	return expired
}

// GetActive returns a snapshot of all VMs currently in StatusActive.
func (s *Store) GetActive() []VMState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]VMState, 0)
	for _, vm := range s.vms {
		if vm.Status == StatusActive {
			result = append(result, *vm)
		}
	}
	return result
}

// GetStaged returns a snapshot of all VMs currently in StatusStaged.
func (s *Store) GetStaged() []VMState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]VMState, 0)
	for _, vm := range s.vms {
		if vm.Status == StatusStaged {
			result = append(result, *vm)
		}
	}
	return result
}

// Get returns the VMState for the given VMID, and whether it exists.
func (s *Store) Get(vmid int) (VMState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if vm, ok := s.vms[vmid]; ok {
		return *vm, true
	}
	return VMState{}, false
}

// set upserts a VMState, rejecting updates older than the current record.
func (s *Store) set(vmid int, name string, status Status, t time.Time, observedAtNs int64) {
	existing, ok := s.vms[vmid]
	if ok {
		if !t.IsZero() && existing.LastEventTime.After(t) {
			// Lifecycle events are authoritative over snapshot-derived state.
			// Accept them despite slight clock skew between event time and snapshot time.
			if !(observedAtNs == 0 && existing.ObservedAtNs > 0) {
				return // reject stale update
			}
		}
		existing.Status = status
		existing.LastEventTime = t
		existing.ObservedAtNs = observedAtNs
		if status != StatusStaged {
			existing.StagedAt = time.Time{}
		}
		if name != "" {
			existing.Name = name
		}
		return
	}
	s.vms[vmid] = &VMState{
		VMID:          vmid,
		Name:          name,
		Status:        status,
		LastEventTime: t,
		ObservedAtNs:  observedAtNs,
	}
}
