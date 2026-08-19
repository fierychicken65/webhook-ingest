// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
	wg    sync.WaitGroup
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	exists, err := s.store.EventExists(ctx, evt.EventID)
	if err != nil {
		return err
	}
	if exists {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	if err := s.store.InsertEvent(ctx, rec); err != nil {
		return err
	}
	if err := s.store.UpsertCall(ctx, rec); err != nil {
		return err
	}
	if err := s.store.IncrementAccountStats(ctx, rec.AccountID, rec.DurationSec); err != nil {
		return err
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.processRecording(context.Background(), rec); err != nil {
				s.log.Error("process recording failed", "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// Shutdown gracefully shuts down the service, waiting for any background tasks to finish.
func (s *Service) Shutdown(ctx context.Context) error {
	s.log.Info("shutting down ingest service background tasks")
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.log.Info("all background tasks completed")
		return nil
	case <-ctx.Done():
		s.log.Error("shutdown timed out before background tasks completed")
		return ctx.Err()
	}
}

// LoadStats initializes the stats cache from the database.
func (s *Service) LoadStats(ctx context.Context) error {
	s.log.Info("initializing stats cache from database")
	dbStats, err := s.store.LoadAllStats(ctx)
	if err != nil {
		return err
	}
	for accountID, st := range dbStats {
		s.cache.Set(accountID, st.CallCount, st.TotalDurationSec)
	}
	s.log.Info("stats cache initialization completed", "loaded_accounts", len(dbStats))
	return nil
}


