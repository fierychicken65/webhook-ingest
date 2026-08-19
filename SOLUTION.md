# Webhook Ingestion Service Solutions

## 1. What Was Broken, and Why

* **Stats Cache Race Condition (Panic):**
  * **Symptom:** Server crashed under load.
  * **Cause:** `stats.Cache` used a plain map `m` to accumulate stats. While `Get` read from the map holding a read lock, `Record` modified the map without holding a write lock. Under concurrency, this triggered a Go runtime map read/write collision panic.
  * **Fix:** Added `c.mu.Lock()` / `defer c.mu.Unlock()` to `Record`.

* **Unprocessed Recordings (Canceled Request Context):**
  * **Symptom:** Recordings never got marked processed, and nothing was logged.
  * **Cause:** The background recording goroutine passed the HTTP request context (`r.Context()`). Since `Ingest` returned immediately, the request completed, canceling the context. All database queries inside `processRecording` failed immediately with `context.Canceled`. The error was ignored (`// TODO: handle`).
  * **Fix:** Detached the context in the goroutine using `context.Background()` and added structured error logging to capture failures.

* **In-Flight Task Loss on Deploy:**
  * **Symptom:** Whatever was in flight disappeared during deployment.
  * **Cause:** The server terminated without waiting for background goroutines to complete.
  * **Fix:** Introduced a `sync.WaitGroup` to track running recording processors and added a `Shutdown` method to the service, which is called during the server's graceful shutdown sequence.

* **Cache Stats Drift (Restart resetting stats to 0):**
  * **Symptom:** In-memory aggregate returned by `GET /accounts/{account_id}/stats` was lower than Postgres.
  * **Cause:** On server startup, the cache was initialized empty. It never loaded previous data from the DB, starting its counts from 0 while Postgres kept incrementing the actual totals.
  * **Fix:** Added a `LoadAllStats` method to the store and populated the cache from Postgres during server startup.

* **Concurrently Duplicate Ingestion (Drifting counts):**
  * **Symptom:** Stats drifted higher due to duplicate events.
  * **Cause:** `EventExists` and `InsertEvent` were separate, non-atomic calls. Under high concurrency, identical events passed the exists check simultaneously, causing duplicate inserts and double-counting stats.
  * **Fix:** Added a UNIQUE constraint on `events(event_id)`. Wrapped all DB updates in `IngestEventTx` using a Postgres transaction. Any unique constraint violation aborts the transaction, returning `ErrDuplicateEvent` which I handle as a successful duplicate ignore.

---

## 2. Choice of Deduplication Strategy

I chose a **Postgres UNIQUE Constraint combined with a DB Transaction** over Redis.

### Alternatives considered:
1. **Redis Lock / Deduplication (SET NX):**
   * *Why rejected:* Redis is not transactional with Postgres. If the application crashes after setting the key in Redis but before committing to Postgres, the event is lost forever (false positive deduplication). Keeping Postgres as the single source of truth prevents data loss.

### Advantages of Postgres UNIQUE constraint + DB Transaction:
* **Strong Consistency (ACID):** Postgres guarantees that either all statements succeed or none do. It blocks concurrent transactions on the same unique key, preventing race conditions out-of-the-box.
* **Simplicity:** It uses the existing database engine without introducing a distributed lock management overhead.

---

## 3. Scaling to 10,000 Webhooks/Second

To scale to 10,000 webhooks/sec, I would make the following changes:

1. **Decouple Ingestion from Processing (Message Queue):**
   * The HTTP handler should do minimal work: parse the JSON, validate it, push the raw payload to a high-throughput message queue (e.g., Apache Kafka, AWS SQS, or RabbitMQ), and immediately respond with `202 Accepted`.

2. **Asynchronous Batch Processing:**
   * Run worker consumers that pull events from the queue and batch-insert them into Postgres (e.g., using COPY or batch INSERT statements). Writing in batches of 100-1000 events dramatically reduces transaction overhead and database lock contention.

3. **Buffer Statistics in Redis:**
   * Instead of updating Postgres stats (`account_stats`) on every webhook, update them atomically in Redis using `HINCRBY` (extremely fast, in-memory). 
   * A background cron job can flush aggregates from Redis to Postgres in bulk every few seconds (e.g., using `INSERT ... ON CONFLICT DO UPDATE`), reconciling the hot cache with durable storage.

---

### All the test files have been updated, I have added a python file to simply test concurrency

To guarantee the reliability and correctness of the deduplication and thread-safety under load, I created a Python integration test script [`test_webhook.py`].

* **Test Mechanism:** 
  * The script uses a `ThreadPoolExecutor` to send **50 identical duplicate webhook requests concurrently** to the server.
  * It verifies that all 50 requests receive `200 OK` (so that duplicated network events are gracefully acknowledged without errors to the provider).
  * It queries the stats endpoint for the target account and asserts that the `call_count` is exactly `1` and `total_duration_sec` is exactly the single call duration, proving that no double-counting or database race conditions occur under heavy concurrent load.

