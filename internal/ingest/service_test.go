package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestIngestRecordingProcessedOnCancelledContext(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	// Wait for background recording work (50ms) to complete
	time.Sleep(100 * time.Millisecond)

	var processed bool
	err := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
	if err != nil {
		t.Fatalf("failed to query calls: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true, but it was false")
	}
}

func TestGracefulShutdownWaits(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), nil, log)

	evt := ingest.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", OccurredAt: time.Now(),
	}

	if err := svc.Ingest(ctx, evt); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Shut down the service immediately
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// When Shutdown returns, all in-flight recording tasks must be complete.
	var processed bool
	err := st.Pool().QueryRow(ctx, `SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
	if err != nil {
		t.Fatalf("failed to query calls: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true after Shutdown completed")
	}
}

func TestCacheInitializationFromDB(t *testing.T) {
	st := testutil.NewStore(t)
	_, _, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// Clean up and set up initial stats in DB
	_, err := st.Pool().Exec(ctx,
		`INSERT INTO account_stats (account_id, call_count, total_duration_sec) VALUES ($1, 5, 150)`,
		accountID)
	if err != nil {
		t.Fatalf("failed to insert mock stats: %v", err)
	}

	// Create a new ingest service (simulating restart)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := stats.NewCache()
	svc := ingest.New(st, cache, nil, log)
	if err := svc.LoadStats(ctx); err != nil {
		t.Fatalf("LoadStats failed: %v", err)
	}

	got := cache.Get(accountID)
	if got.CallCount != 5 || got.TotalDurationSec != 150 {
		t.Fatalf("expected Cache to be initialized with DB values. Got CallCount=%d, TotalDurationSec=%d, want CallCount=5, TotalDurationSec=150", got.CallCount, got.TotalDurationSec)
	}
}

func TestConcurrentDuplicateIngestion(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), nil, log)

	evt := ingest.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 10,
		RecordingURL: "https://example.com/a.wav", OccurredAt: time.Now(),
	}

	const concurrency = 200
	var wg sync.WaitGroup
	errorsChan := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := svc.Ingest(ctx, evt); err != nil {
				errorsChan <- err
			}
		}()
	}
	wg.Wait()
	close(errorsChan)


	// Since we expect all to succeed (idempotent), there should be no errors
	for err := range errorsChan {
		t.Errorf("Ingest failed: %v", err)
	}

	// Only 1 event must be in the DB
	var eventCount int
	err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount)
	if err != nil {
		t.Fatalf("failed to query events: %v", err)
	}
	if eventCount != 1 {
		t.Errorf("expected 1 event, got %d", eventCount)
	}

	// Stats should only reflect 1 call
	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("failed to query account stats: %v", err)
	}
	if stats.CallCount != 1 {
		t.Errorf("expected CallCount to be 1, got %d", stats.CallCount)
	}
}


