package state_test

import (
	"testing"
	"time"

	"github.com/hostzero/network-reconciler/internal/state"
)

func TestSetActiveGetActive(t *testing.T) {
	s := state.New()
	now := time.Now()

	s.SetActive(101, "web-01", now)
	s.SetActive(102, "db-01", now)

	active := s.GetActive()
	if len(active) != 2 {
		t.Fatalf("expected 2 active VMs, got %d", len(active))
	}
}

func TestSetAbsentRemovesFromActive(t *testing.T) {
	s := state.New()
	now := time.Now()

	s.SetActive(101, "web-01", now)
	s.SetAbsent(101, now.Add(time.Second))

	active := s.GetActive()
	if len(active) != 0 {
		t.Fatalf("expected 0 active VMs after SetAbsent, got %d", len(active))
	}

	vm, ok := s.Get(101)
	if !ok {
		t.Fatal("expected VM to still exist in store after SetAbsent")
	}
	if vm.Status != state.StatusAbsent {
		t.Fatalf("expected StatusAbsent, got %v", vm.Status)
	}
}

func TestSetStagedGetStaged(t *testing.T) {
	s := state.New()
	now := time.Now()

	s.SetStaged(101, "web-01", now)

	staged := s.GetStaged()
	if len(staged) != 1 {
		t.Fatalf("expected 1 staged VM, got %d", len(staged))
	}
	if staged[0].VMID != 101 {
		t.Fatalf("unexpected VMID: %d", staged[0].VMID)
	}
	active := s.GetActive()
	if len(active) != 0 {
		t.Fatalf("staged VM should not appear in active list")
	}
}

func TestStaleUpdateRejected(t *testing.T) {
	s := state.New()
	now := time.Now()

	s.SetActive(101, "web-01", now)
	// Attempt to overwrite with an older timestamp → must be rejected.
	s.SetAbsent(101, now.Add(-time.Second))

	vm, _ := s.Get(101)
	if vm.Status != state.StatusActive {
		t.Fatalf("stale SetAbsent should not override newer SetActive; got %v", vm.Status)
	}
}

func TestUpdateFromSnapshotNewerWins(t *testing.T) {
	s := state.New()
	baseNs := time.Now().UnixNano()

	s.UpdateFromSnapshot(101, "web-01", true, baseNs)
	vm, _ := s.Get(101)
	if vm.Status != state.StatusActive {
		t.Fatalf("expected active from snapshot, got %v", vm.Status)
	}

	// Newer snapshot says stopped.
	s.UpdateFromSnapshot(101, "web-01", false, baseNs+int64(time.Second))
	vm, _ = s.Get(101)
	if vm.Status != state.StatusAbsent {
		t.Fatalf("expected absent from newer snapshot, got %v", vm.Status)
	}
}

func TestUpdateFromSnapshotStaleRejected(t *testing.T) {
	s := state.New()
	baseNs := time.Now().UnixNano()

	// First a newer snapshot that says running.
	s.UpdateFromSnapshot(101, "web-01", true, baseNs+int64(time.Second))

	// Then an older snapshot that says stopped — must be rejected.
	s.UpdateFromSnapshot(101, "web-01", false, baseNs)

	vm, _ := s.Get(101)
	if vm.Status != state.StatusActive {
		t.Fatalf("stale snapshot should not override newer snapshot; got %v", vm.Status)
	}
}

func TestLifecycleOverrideNewerSnapshotTimestamp(t *testing.T) {
	s := state.New()
	base := time.Now()

	// Snapshot marks VM running with a newer observed time.
	s.UpdateFromSnapshot(101, "web-01", true, base.Add(10*time.Second).UnixNano())

	// A lifecycle terminal event can still clear state even with older wall-clock time.
	s.SetAbsent(101, base.Add(2*time.Second))

	vm, _ := s.Get(101)
	if vm.Status != state.StatusAbsent {
		t.Fatalf("expected lifecycle SetAbsent to override snapshot state, got %v", vm.Status)
	}
}

func TestMigrationStateMachine(t *testing.T) {
	s := state.New()
	t0 := time.Now()

	// VM is active on source node; we are the target.
	s.SetStaged(101, "web-01", t0)
	vm, _ := s.Get(101)
	if vm.Status != state.StatusStaged {
		t.Fatalf("expected staged, got %v", vm.Status)
	}

	// Migration completes — activate.
	s.SetActive(101, "web-01", t0.Add(time.Second))
	vm, _ = s.Get(101)
	if vm.Status != state.StatusActive {
		t.Fatalf("expected active after migration, got %v", vm.Status)
	}
}

func TestMigrationFailedRollback(t *testing.T) {
	s := state.New()
	t0 := time.Now()

	s.SetStaged(101, "web-01", t0)
	s.SetAbsent(101, t0.Add(time.Second))

	vm, _ := s.Get(101)
	if vm.Status != state.StatusAbsent {
		t.Fatalf("expected absent after migration failure, got %v", vm.Status)
	}
}

// A migration routinely outlasts the 30s snapshot cadence, and during it the VM is
// legitimately not running on the target yet. If a running=false snapshot were allowed
// to win, Staged would silently reset to Absent partway through and everything staged
// for the cutover would be torn down.
func TestSnapshotDoesNotClearStagedStatus(t *testing.T) {
	s := state.New()
	stagedAt := time.Now()

	s.SetStaged(101, "web-01", stagedAt)

	// A snapshot from *after* staging began, reporting the VM as not running here.
	s.UpdateFromSnapshot(101, "web-01", false, stagedAt.Add(30*time.Second).UnixNano())

	vm, ok := s.Get(101)
	if !ok {
		t.Fatal("expected VM to exist in store")
	}
	if vm.Status != state.StatusStaged {
		t.Fatalf("expected StatusStaged to survive a running=false snapshot, got %v", vm.Status)
	}
	if len(s.GetStaged()) != 1 {
		t.Fatalf("expected 1 staged VM, got %d", len(s.GetStaged()))
	}

	// The terminal migration event must still be able to activate it.
	s.SetActive(101, "web-01", stagedAt.Add(time.Minute))
	if vm, _ := s.Get(101); vm.Status != state.StatusActive {
		t.Fatalf("expected migrate.synced to activate the VM, got %v", vm.Status)
	}
}

// A snapshot reporting the VM as running on this node should still promote it.
func TestSnapshotRunningPromotesStagedVM(t *testing.T) {
	s := state.New()
	stagedAt := time.Now()

	s.SetStaged(101, "web-01", stagedAt)
	s.UpdateFromSnapshot(101, "web-01", true, stagedAt.Add(30*time.Second).UnixNano())

	if vm, _ := s.Get(101); vm.Status != state.StatusActive {
		t.Fatalf("expected a running snapshot to promote staged -> active, got %v", vm.Status)
	}
}

func TestExpireStagedClearsAbandonedMigrations(t *testing.T) {
	s := state.New()
	old := time.Now().Add(-time.Hour)

	s.SetStaged(101, "web-01", old)
	s.SetStaged(102, "db-01", time.Now())
	s.SetActive(103, "cache-01", time.Now())

	expired := s.ExpireStaged(time.Now().Add(-30 * time.Minute))
	if len(expired) != 1 || expired[0] != 101 {
		t.Fatalf("expected only VM 101 to expire, got %v", expired)
	}
	if vm, _ := s.Get(101); vm.Status != state.StatusAbsent {
		t.Fatalf("expected expired VM to be absent, got %v", vm.Status)
	}
	if vm, _ := s.Get(102); vm.Status != state.StatusStaged {
		t.Fatalf("expected recently staged VM to survive, got %v", vm.Status)
	}
	if vm, _ := s.Get(103); vm.Status != state.StatusActive {
		t.Fatalf("expected active VM to be untouched, got %v", vm.Status)
	}
}
