package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Repository defines the persistence methods required by the outbox dispatcher.
type Repository interface {
	Store(ctx context.Context, event *Event) error
	BulkInsert(ctx context.Context, events []*Event) error
	GetPendingEvents(limit int) ([]*Event, error)
	GetByID(id uuid.UUID) (*Event, error)
	UpdateStatus(id uuid.UUID, status Status, errorMessage *string) error
	MarkAsProcessing(id uuid.UUID) error
	IncrementRetryCount(id uuid.UUID, nextRetryAt time.Time, errorMessage *string) error
	DeleteCompletedEvents(olderThan time.Time) (int64, error)
	EnsurePublisherProgressTable() error

	// Publisher progress methods (partition-aware)
	GetPublisherProgress(publisher string, partition int) (*uuid.UUID, error)
	GetPendingEventsForPublisher(publisher string, partition int, limit int) ([]*Event, error)
	MarkPublished(publisher string, partition int, event *Event, publishers []string) error

	// Advisory locks for partition ownership.
	AcquirePartitionLock(partition int) (bool, error)
	ReleasePartitionLock(partition int) error

	ListDeadLetteredEvents(limit int) ([]*Event, error)
	RequeueEvent(id uuid.UUID) error
}
