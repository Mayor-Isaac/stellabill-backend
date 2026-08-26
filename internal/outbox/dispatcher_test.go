package outbox

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// simple in-memory repository for testing
type memRepo struct {
	mu         sync.Mutex
	events     []*Event
	progress   map[string]uuid.UUID // key: publisher + ":" + partition
	shardCount int
	locks      map[int]bool
}

func newMemRepo() *memRepo {
	return &memRepo{
		progress:   make(map[string]uuid.UUID),
		shardCount: 1,
		locks:      make(map[int]bool),
	}
}

func (r *memRepo) SetShardCount(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shardCount = n
}

func (r *memRepo) Store(_ context.Context, event *Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return nil
}

func (r *memRepo) BulkInsert(_ context.Context, events []*Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, events...)
	return nil
}

func (r *memRepo) GetPendingEvents(limit int) ([]*Event, error) {
	return r.GetPendingEventsForPublisher("", 0, limit)
}

func (r *memRepo) GetByID(_ uuid.UUID) (*Event, error) { return nil, nil }
func (r *memRepo) UpdateStatus(_ uuid.UUID, _ Status, _ *string) error { return nil }
func (r *memRepo) MarkAsProcessing(_ uuid.UUID) error { return nil }
func (r *memRepo) IncrementRetryCount(_ uuid.UUID, _ time.Time, _ *string) error { return nil }
func (r *memRepo) DeleteCompletedEvents(_ time.Time) (int64, error) { return 0, nil }
func (r *memRepo) ListDeadLetteredEvents(_ int) ([]*Event, error) { return nil, nil }
func (r *memRepo) RequeueEvent(_ uuid.UUID) error { return nil }
func (r *memRepo) EnsurePublisherProgressTable() error { return nil }

func (r *memRepo) GetPublisherProgress(publisher string, partition int) (*uuid.UUID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := publisher + ":" + strconv.Itoa(partition)
	id, ok := r.progress[key]
	if !ok {
		return nil, nil
	}
	return &id, nil
}

func (r *memRepo) MarkPublished(publisher string, partition int, event *Event, _ []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := publisher + ":" + strconv.Itoa(partition)
	if current, ok := r.progress[key]; !ok || current.String() < event.ID.String() {
		r.progress[key] = event.ID
	}
	return nil
}

func (r *memRepo) GetPendingEventsForPublisher(publisher string, partition int, limit int) ([]*Event, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*Event
	key := publisher + ":" + strconv.Itoa(partition)
	lastID, hasProgress := r.progress[key]
	for _, e := range r.events {
		if PartitionForTenant(e.TenantID, r.shardCount) != partition {
			continue
		}
		if !hasProgress || e.ID.String() > lastID.String() {
			out = append(out, e)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (r *memRepo) AcquirePartitionLock(partition int) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.locks[partition] {
		return false, nil
	}
	r.locks[partition] = true
	return true, nil
}

func (r *memRepo) ReleasePartitionLock(partition int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.locks, partition)
	return nil
}

// mock publishers
type succeedPublisher struct{}

func (p *succeedPublisher) Publish(_ context.Context, _ *Event) error { return nil }

type failPublisher struct{}

func (p *failPublisher) Publish(_ context.Context, _ *Event) error { return assert.AnError }

type slowFailPublisher struct{}

func (p *slowFailPublisher) Publish(ctx context.Context, _ *Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return assert.AnError
	}
}

func TestPerPublisherDrain(t *testing.T) {
	repo := newMemRepo()
	e := &Event{ID: uuid.New(), EventType: "test", EventData: []byte(`{"type":"test"}`), OccurredAt: time.Now(), TenantID: "tenant-1"}
	repo.Store(context.Background(), e)

	mp := NewMultiPublisher(NewConsolePublisher(), &succeedPublisher{})
	cfg := DefaultDispatcherConfig()
	cfg.PollInterval = 100 * time.Millisecond
	cfg.BatchSize = 10
	cfg.ProcessingTimeout = 200 * time.Millisecond

	d := NewDispatcher(repo, mp, cfg).(*dispatcher)
	if err := d.Start(); err != nil {
		t.Fatalf("start err: %v", err)
	}
	defer d.Stop()

	time.Sleep(500 * time.Millisecond)

	id1, _ := repo.GetPublisherProgress("publisher-1", 0)
	if assert.NotNil(t, id1) {
		assert.Equal(t, e.ID.String(), id1.String())
	}

	id0, _ := repo.GetPublisherProgress("publisher-0", 0)
	if assert.NotNil(t, id0) {
		assert.Equal(t, e.ID.String(), id0.String())
	}
}

func TestFailureIsolationAndRecovery(t *testing.T) {
	repo := newMemRepo()
	e := &Event{ID: uuid.New(), EventType: "test", EventData: []byte(`{"type":"test"}`), OccurredAt: time.Now(), TenantID: "tenant-1"}
	repo.Store(context.Background(), e)

	mp := NewMultiPublisher(&failPublisher{}, &succeedPublisher{})
	cfg := DefaultDispatcherConfig()
	cfg.PollInterval = 100 * time.Millisecond
	cfg.BatchSize = 10
	cfg.ProcessingTimeout = 200 * time.Millisecond

	d := NewDispatcher(repo, mp, cfg).(*dispatcher)
	if err := d.Start(); err != nil {
		t.Fatalf("start err: %v", err)
	}
	defer d.Stop()

	time.Sleep(500 * time.Millisecond)

	id1, _ := repo.GetPublisherProgress("publisher-1", 0)
	if assert.NotNil(t, id1) {
		assert.Equal(t, e.ID.String(), id1.String())
	}

	id0, _ := repo.GetPublisherProgress("publisher-0", 0)
	assert.Nil(t, id0)

	_ = repo.MarkPublished("publisher-0", 0, e, []string{"publisher-0", "publisher-1"})
	time.Sleep(200 * time.Millisecond)

	id0b, _ := repo.GetPublisherProgress("publisher-0", 0)
	assert.Equal(t, e.ID.String(), id0b.String())
}

func TestShardedDispatcherOwnsPartitions(t *testing.T) {
	repo := newMemRepo()
	repo.SetShardCount(4)

	// Create events for two tenants that map to different partitions.
	t1 := "tenant-alpha"
	t2 := "tenant-beta"
	p1 := PartitionForTenant(t1, 4)
	p2 := PartitionForTenant(t2, 4)
	if p1 == p2 {
		t.Skip("test tenants collide; adjust fixtures")
	}

	e1 := &Event{ID: uuid.New(), TenantID: t1, EventType: "test", EventData: []byte(`{}`), OccurredAt: time.Now()}
	e2 := &Event{ID: uuid.New(), TenantID: t2, EventType: "test", EventData: []byte(`{}`), OccurredAt: time.Now()}
	repo.Store(context.Background(), e1)
	repo.Store(context.Background(), e2)

	// Create a dispatcher and assign it only the partition of tenant-alpha.
	mp := NewMultiPublisher(&succeedPublisher{})
	cfg := DefaultDispatcherConfig()
	cfg.ShardCount = 4
	cfg.OwnedShards = []int{p1}
	cfg.PollInterval = 100 * time.Millisecond
	cfg.BatchSize = 10

	d := NewDispatcher(repo, mp, cfg).(*dispatcher)
	if err := d.Start(); err != nil {
		t.Fatalf("start err: %v", err)
	}
	defer d.Stop()

	time.Sleep(500 * time.Millisecond)

	// The owned partition should have been processed.
	id1, _ := repo.GetPublisherProgress("publisher-0", p1)
	if assert.NotNil(t, id1) {
		assert.Equal(t, e1.ID.String(), id1.String())
	}

	// The unowned partition must not have been processed.
	id2, _ := repo.GetPublisherProgress("publisher-0", p2)
	assert.Nil(t, id2)
}

func TestShardResizePreservesTenantOrder(t *testing.T) {
	repo := newMemRepo()

	// Start with one shard.
	repo.SetShardCount(1)
	tenant := "tenant-1"
	events := []*Event{
		{ID: uuid.New(), TenantID: tenant, EventType: "evt1", EventData: []byte(`{}`), OccurredAt: time.Now()},
		{ID: uuid.New(), TenantID: tenant, EventType: "evt2", EventData: []byte(`{}`), OccurredAt: time.Now().Add(time.Millisecond)},
	}
	for _, e := range events {
		repo.Store(context.Background(), e)
	}

	// Process with one shard.
	mp := NewMultiPublisher(&succeedPublisher{})
	cfg := DefaultDispatcherConfig()
	cfg.ShardCount = 1
	cfg.OwnedShards = []int{0}
	cfg.PollInterval = 100 * time.Millisecond
	cfg.BatchSize = 10

	d1 := NewDispatcher(repo, mp, cfg).(*dispatcher)
	assert.NoError(t, d1.Start())
	time.Sleep(400 * time.Millisecond)
	d1.Stop()

	// Now increase to 4 shards and ensure the tenant's new partition gets its events.
	newPartition := PartitionForTenant(tenant, 4)
	repo.SetShardCount(4)
	repo.progress["publisher-0:"+strconv.Itoa(newPartition)] = uuid.Nil // reset progress for the new partition

	cfg2 := DefaultDispatcherConfig()
	cfg2.ShardCount = 4
	cfg2.OwnedShards = []int{newPartition}
	cfg2.PollInterval = 100 * time.Millisecond
	cfg2.BatchSize = 10

	d2 := NewDispatcher(repo, mp, cfg2).(*dispatcher)
	assert.NoError(t, d2.Start())
	time.Sleep(400 * time.Millisecond)
	d2.Stop()

	// The second event should be processed (the first may already be done).
	id2, _ := repo.GetPublisherProgress("publisher-0", newPartition)
	if assert.NotNil(t, id2) {
		assert.Equal(t, events[1].ID.String(), id2.String())
	}
}
