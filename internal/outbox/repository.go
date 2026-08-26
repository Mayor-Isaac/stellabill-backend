package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"stellarbill-backend/internal/db"
	"stellarbill-backend/internal/servertiming"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

// PostgreSQL repository implementation
type postgresRepository struct {
	db         db.DBTX
	shardCount int
}

type sqlTxBeginner interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type sqlProgressExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// NewPostgresRepository creates a new PostgreSQL repository
func NewPostgresRepository(executor db.DBTX) Repository {
	return &postgresRepository{db: executor, shardCount: 1}
}

// SetShardCount updates the repository's shard count used when inserting
// new events. The dispatcher should call this with the configured value.
func (r *postgresRepository) SetShardCount(n int) {
	if n <= 0 {
		n = 1
	}
	r.shardCount = n
}

// Store stores a new outbox event
func (r *postgresRepository) Store(ctx context.Context, event *Event) error {
	start := time.Now()
	defer func() {
		if rec := servertiming.FromContext(ctx); rec != nil {
			rec.RecordOutbox(time.Since(start))
		}
	}()

	partition := PartitionForTenant(event.TenantID, r.shardCount)
	query := `
		INSERT INTO outbox_events (
			id, tenant_id, event_type, event_data, aggregate_id, aggregate_type,
			occurred_at, status, retry_count, max_retries, next_retry_at,
			error_message, created_at, updated_at, version, deduplication_id, partition
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	`

	_, err := r.db.ExecContext(ctx, query,
		event.ID,
		event.TenantID,
		event.EventType,
		event.EventData,
		event.AggregateID,
		event.AggregateType,
		event.OccurredAt,
		event.Status,
		event.RetryCount,
		event.MaxRetries,
		event.NextRetryAt,
		event.ErrorMessage,
		event.CreatedAt,
		event.UpdatedAt,
		event.Version,
		event.DeduplicationID,
		partition,
	)
	if err != nil {
		return fmt.Errorf("failed to store outbox event: %w", err)
	}

	return nil
}

func (r *postgresRepository) BulkInsert(ctx context.Context, events []*Event) error {
	for _, e := range events {
		if err := r.Store(ctx, e); err != nil {
			return fmt.Errorf("failed to bulk insert event %s: %w", e.ID, err)
		}
	}
	return nil
}

// GetPendingEvents retrieves pending events for processing
func (r *postgresRepository) GetPendingEvents(limit int) ([]*Event, error) {
	query := `
		SELECT id, tenant_id, event_type, event_data, aggregate_id, aggregate_type,
			   occurred_at, status, retry_count, max_retries, next_retry_at,
			   error_message, created_at, updated_at, version, deduplication_id
		FROM outbox_events
		WHERE status = $1 OR (status = $2 AND next_retry_at <= $3)
		ORDER BY occurred_at ASC
		LIMIT $4
	`

	rows, err := r.db.Query(query, StatusPending, StatusFailed, time.Now(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending events: %w", err)
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		event, err := r.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pending events: %w", err)
	}

	return events, nil
}

// GetByID retrieves an event by ID
func (r *postgresRepository) GetByID(id uuid.UUID) (*Event, error) {
	query := `
		SELECT id, tenant_id, event_type, event_data, aggregate_id, aggregate_type,
			   occurred_at, status, retry_count, max_retries, next_retry_at,
			   error_message, created_at, updated_at, version, deduplication_id
		FROM outbox_events
		WHERE id = $1
	`

	row := r.db.QueryRow(query, id)
	return r.scanEvent(row)
}

// UpdateStatus updates the status of an event
func (r *postgresRepository) UpdateStatus(id uuid.UUID, status Status, errorMessage *string) error {
	query := `
		UPDATE outbox_events
		SET status = $1, error_message = $2, updated_at = $3
		WHERE id = $4
	`

	_, err := r.db.Exec(query, status, errorMessage, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update event status: %w", err)
	}

	return nil
}

// MarkAsProcessing marks an event as being processed
func (r *postgresRepository) MarkAsProcessing(id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET status = $1, updated_at = $2
		WHERE id = $3 AND status = $4
	`

	result, err := r.db.Exec(query, StatusProcessing, time.Now(), id, StatusPending)
	if err != nil {
		return fmt.Errorf("failed to mark event as processing: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("event not found or not in pending status")
	}

	return nil
}

// IncrementRetryCount increments the retry count and sets next retry time
func (r *postgresRepository) IncrementRetryCount(id uuid.UUID, nextRetryAt time.Time, errorMessage *string) error {
	query := `
		UPDATE outbox_events
		SET retry_count = retry_count + 1, 
			next_retry_at = $1, 
			status = $2, 
			error_message = $3,
			updated_at = $4
		WHERE id = $5
	`

	_, err := r.db.Exec(query, nextRetryAt, StatusFailed, errorMessage, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to increment retry count: %w", err)
	}

	return nil
}

// DeleteCompletedEvents deletes completed events older than the specified time
func (r *postgresRepository) DeleteCompletedEvents(olderThan time.Time) (int64, error) {
	query := `
		DELETE FROM outbox_events
		WHERE status = $1 AND updated_at < $2
	`

	result, err := r.db.Exec(query, StatusCompleted, olderThan)
	if err != nil {
		return 0, fmt.Errorf("failed to delete completed events: %w", err)
	}

	return result.RowsAffected()
}

// EnsurePublisherProgressTable ensures the publisher progress table exists
func (r *postgresRepository) EnsurePublisherProgressTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS outbox_publisher_progress (
		publisher VARCHAR(255) NOT NULL,
		partition INTEGER NOT NULL DEFAULT 0,
		last_event_id UUID,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (publisher, partition)
	)`
	if _, err := r.db.Exec(query); err != nil {
		return fmt.Errorf("failed to ensure publisher progress table: %w", err)
	}
	return nil
}

// GetPublisherProgress returns the last published event id for a publisher and partition.
func (r *postgresRepository) GetPublisherProgress(publisher string, partition int) (*uuid.UUID, error) {
	query := `SELECT last_event_id FROM outbox_publisher_progress WHERE publisher = $1 AND partition = $2 AND last_event_id IS NOT NULL`
	row := r.db.QueryRow(query, publisher, partition)
	var lastID uuid.UUID
	if err := row.Scan(&lastID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get publisher progress: %w", err)
	}
	return &lastID, nil
}

// GetPendingEventsForPublisher returns events above the publisher high-water mark for a partition.
func (r *postgresRepository) GetPendingEventsForPublisher(publisher string, partition int, limit int) ([]*Event, error) {
	query := `
		SELECT e.id, e.tenant_id, e.event_type, e.event_data, e.aggregate_id, e.aggregate_type,
			   e.occurred_at, e.status, e.retry_count, e.max_retries, e.next_retry_at,
			   e.error_message, e.created_at, e.updated_at, e.version, e.deduplication_id
		FROM outbox_events e
		LEFT JOIN outbox_publisher_progress p ON p.publisher = $1 AND p.partition = $2
		WHERE e.partition = $2
		  AND (e.status = $3 OR (e.status = $4 AND e.next_retry_at <= $5))
		  AND (p.last_event_id IS NULL OR e.id > p.last_event_id)
		ORDER BY e.id ASC
		LIMIT $6`

	rows, err := r.db.Query(query, publisher, partition, StatusPending, StatusFailed, time.Now(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending events for publisher: %w", err)
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		ev, err := r.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating pending events for publisher: %w", err)
	}
	return events, nil
}

// MarkPublished atomically stores publisher progress for a partition and completes the event once
// every configured publisher has reached this event.
func (r *postgresRepository) MarkPublished(publisher string, partition int, event *Event, publishers []string) error {
	ctx := context.Background()
	if tx, ok := r.db.(*sql.Tx); ok {
		return r.markPublished(ctx, tx, publisher, partition, event, publishers)
	}

	beginner, ok := r.db.(sqlTxBeginner)
	if !ok {
		return fmt.Errorf("outbox publisher progress requires transactional database executor")
	}

	tx, err := beginner.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin publisher progress transaction: %w", err)
	}
	defer tx.Rollback()

	if err := r.markPublished(ctx, tx, publisher, partition, event, publishers); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit publisher progress transaction: %w", err)
	}
	return nil
}

func (r *postgresRepository) markPublished(ctx context.Context, exec sqlProgressExecutor, publisher string, partition int, event *Event, publishers []string) error {
	if err := upsertPublisherProgress(ctx, exec, publisher, partition, event.ID); err != nil {
		return err
	}

	allPublished, err := publisherProgressReached(ctx, exec, partition, event.ID, publishers)
	if err != nil {
		return err
	}
	if !allPublished {
		return nil
	}

	_, err = exec.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = $1, error_message = NULL, updated_at = $2
		WHERE id = $3`, StatusCompleted, time.Now(), event.ID)
	if err != nil {
		return fmt.Errorf("failed to mark event completed: %w", err)
	}
	return nil
}

func upsertPublisherProgress(ctx context.Context, exec sqlProgressExecutor, publisher string, partition int, eventID uuid.UUID) error {
	_, err := exec.ExecContext(ctx, `
		INSERT INTO outbox_publisher_progress (publisher, partition, last_event_id, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (publisher, partition) DO UPDATE SET
			last_event_id = CASE
				WHEN outbox_publisher_progress.last_event_id IS NULL
					OR outbox_publisher_progress.last_event_id < EXCLUDED.last_event_id
					THEN EXCLUDED.last_event_id
				ELSE outbox_publisher_progress.last_event_id
			END,
			updated_at = CASE
				WHEN outbox_publisher_progress.last_event_id IS NULL
					OR outbox_publisher_progress.last_event_id < EXCLUDED.last_event_id
					THEN EXCLUDED.updated_at
				ELSE outbox_publisher_progress.updated_at
			END`, publisher, partition, eventID, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update publisher progress: %w", err)
	}
	return nil
}

func publisherProgressReached(ctx context.Context, exec sqlProgressExecutor, partition int, eventID uuid.UUID, publishers []string) (bool, error) {
	for _, publisher := range publishers {
		var lastID uuid.UUID
		err := exec.QueryRowContext(ctx, `
			SELECT last_event_id
			FROM outbox_publisher_progress
			WHERE publisher = $1 AND partition = $2`, publisher, partition).Scan(&lastID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("failed to read publisher progress: %w", err)
		}
		if lastID.String() < eventID.String() {
			return false, nil
		}
	}
	return true, nil
}

// ListDeadLetteredEvents retrieves dead-lettered (failed) events
func (r *postgresRepository) ListDeadLetteredEvents(limit int) ([]*Event, error) {
	query := `
		SELECT id, tenant_id, event_type, event_data, aggregate_id, aggregate_type,
			   occurred_at, status, retry_count, max_retries, next_retry_at,
			   error_message, created_at, updated_at, version, deduplication_id
		FROM dead_letter_events
		LIMIT $1
	`

	rows, err := r.db.Query(query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list dead-lettered events: %w", err)
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		event, err := r.scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating dead-lettered events: %w", err)
	}

	return events, nil
}

// RequeueEvent resets a failed event to pending for reprocessing
func (r *postgresRepository) RequeueEvent(id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET status = $1, retry_count = 0, next_retry_at = NULL, error_message = NULL
		WHERE id = $2 AND status = $3
	`

	result, err := r.db.Exec(query, StatusPending, id, StatusFailed)
	if err != nil {
		return fmt.Errorf("failed to requeue event: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("event not found or not in failed status")
	}

	return nil
}

const advisoryLockBase = int64(0x0B70) << 32

func lockKeyForPartition(partition int) int64 {
	return advisoryLockBase | int64(partition)
}

// AcquirePartitionLock attempts to acquire a session-level advisory lock for
// the given partition. It returns true on success.
func (r *postgresRepository) AcquirePartitionLock(partition int) (bool, error) {
	var locked bool
	err := r.db.QueryRow(`SELECT pg_try_advisory_lock($1)`, lockKeyForPartition(partition)).Scan(&locked)
	if err != nil {
		return false, fmt.Errorf("failed to acquire advisory lock: %w", err)
	}
	return locked, nil
}

// ReleasePartitionLock releases a previously acquired advisory lock.
func (r *postgresRepository) ReleasePartitionLock(partition int) error {
	_, err := r.db.Exec(`SELECT pg_advisory_unlock($1)`, lockKeyForPartition(partition))
	if err != nil {
		return fmt.Errorf("failed to release advisory lock: %w", err)
	}
	return nil
}
