// File: main.go

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Simple, single-binary Go server that ingests rrweb events from a Next.js frontend
// and writes them into daily DuckDB partition files. It exposes endpoints:
//  - POST /ingest  -> accept JSON { sessionId, appType, events: [ ... ] }
//  - POST /record_user_session -> register user_id against session_id & app_type
//  - GET  /replay?session=... -> stream NDJSON of events for given session
//  - GET  /query_user?user=... -> return metadata/sessions for a user
//
// The design goals:
//  - very fast inserts via frontend batching + a writer goroutine
//  - daily DuckDB partitions: data/duck/events_YYYY_MM_DD.duckdb
//  - user-session mappings stored in separate table
//  - queries database directly (no in-memory caching to avoid OOM)
//  - authentication/authorization left as TODO (add JWT/API keys in production)

const (
	dataDir    = "./data"
	duckDir    = dataDir + "/duck"
	parquetDir = dataDir + "/parquet"
	// checkpointInterval is how often to checkpoint the WAL during idle periods
	checkpointInterval = 30 * time.Second
	// archiveCheckInterval is how often to check for day boundary changes
	archiveCheckInterval = 1 * time.Minute
)

// EventRow holds one rrweb event mapped to a DB row
type EventRow struct {
	Ts           time.Time `json:"ts"`
	SessionID    string    `json:"session_id"`
	AppType      string    `json:"app_type"`
	EventJSON    string    `json:"event_json"`
	EventType    string    `json:"event_type"`    // Parsed "type" field
	ErrorLevel   string    `json:"error_level"`   // Parsed "data.payload.level"
	ErrorPayload string    `json:"error_payload"` // Parsed "data.payload.payload"
}

// payload from frontend
type IngestPayload struct {
	SessionID string            `json:"sessionId"`
	AppType   string            `json:"appType"`
	Events    []json.RawMessage `json:"events"`
}

// payload for registering user against session
type RegisterUserPayload struct {
	SessionID string `json:"sessionId"`
	AppType   string `json:"appType"`
	UserID    string `json:"userId"`
}

// session metadata returned by queries
type SessionMeta struct {
	SessionID string `json:"sessionId"`
	UserID    string `json:"userId,omitempty"`
	AppType   string `json:"appType,omitempty"`
	FirstTS   int64  `json:"firstTs"`
	LastTS    int64  `json:"lastTs"`
	Size      int64  `json:"size"`
	File      string `json:"file"`
}

// error group for aggregated error reporting
type ErrorGroup struct {
	ErrorLevel       string `json:"errorLevel"`
	ErrorPayload     string `json:"errorPayload"`
	Count            int64  `json:"count"`
	FirstSeen        int64  `json:"firstSeen"`
	LastSeen         int64  `json:"lastSeen"`
	AffectedSessions int64  `json:"affectedSessions"`
	AffectedUsers    int64  `json:"affectedUsers"`
	SampleEvent      string `json:"sampleEvent"`
}

var (
	// channel for batched ingestion; each item is a batch (slice) of EventRow
	ingestCh = make(chan []EventRow, 512)

	// waitgroup for background workers
	wg sync.WaitGroup

	// stop signal for archive worker
	archiveStopCh = make(chan struct{})
)

func main() {
	// ensure directories
	if err := os.MkdirAll(duckDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}
	if err := os.MkdirAll(parquetDir, 0o755); err != nil {
		log.Fatalf("failed to create parquet dir: %v", err)
	}

	// initialize user-sessions database
	userSessionsDB := filepath.Join(duckDir, "user_sessions.duckdb")
	if err := initUserSessionsDB(userSessionsDB); err != nil {
		log.Fatalf("failed to init user_sessions db: %v", err)
	}

	// start writer worker
	wg.Add(1)
	go writerWorker()

	// start archive worker for day-end Parquet export
	wg.Add(1)
	go archiveWorker()

	// CORS middleware
	corsMiddleware := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			// Handle preflight requests
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}

			next(w, r)
		}
	}

	// HTTP handlers with CORS
	http.HandleFunc("/ingest", corsMiddleware(ingestHandler))
	http.HandleFunc("/record_user_session", corsMiddleware(recordUserSessionHandler))
	http.HandleFunc("/replay", corsMiddleware(replayHandler))
	http.HandleFunc("/query_user", corsMiddleware(queryUserHandler))
	http.HandleFunc("/debug_errors", corsMiddleware(debugErrorsHandler))

	// Serve HTML page
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			http.ServeFile(w, r, "index.html")
		} else {
			http.NotFound(w, r)
		}
	})

	// basic health
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// Create HTTP server with graceful shutdown support
	server := &http.Server{
		Addr:    ":8080",
		Handler: nil,
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start server in a goroutine
	go func() {
		log.Printf("listening %s\n", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	log.Println("shutting down gracefully...")

	// Shutdown HTTP server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	// Close ingest channel to signal writerWorker to stop
	close(ingestCh)
	// Signal archive worker to stop
	close(archiveStopCh)
	log.Println("waiting for writer worker to finish...")

	// Wait for background tasks to complete
	wg.Wait()
	log.Println("shutdown complete")
}

// ingestHandler accepts gzipped+base64 encoded JSON payloads from the browser.
// It decompresses and decodes the payload, then converts incoming events into
// EventRow objects with the current timestamp and enqueues them as a single batch
// on ingestCh. The handler returns quickly (202 Accepted) so the frontend can continue.
// Note: user_id is not included in ingest payload. Use /record_user_session to register user_id.
func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the request body (base64 string)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request: failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Decode base64 (trim whitespace)
	base64Str := string(bytes.TrimSpace(bodyBytes))
	gzippedData, err := base64.StdEncoding.DecodeString(base64Str)
	if err != nil {
		http.Error(w, "bad request: invalid base64", http.StatusBadRequest)
		return
	}

	// Decompress gzip
	gzReader, err := gzip.NewReader(bytes.NewReader(gzippedData))
	if err != nil {
		http.Error(w, "bad request: invalid gzip data", http.StatusBadRequest)
		return
	}
	defer gzReader.Close()

	decompressedData, err := io.ReadAll(gzReader)
	if err != nil {
		http.Error(w, "bad request: failed to decompress", http.StatusBadRequest)
		return
	}

	// Parse JSON
	var p IngestPayload
	if err := json.Unmarshal(decompressedData, &p); err != nil {
		http.Error(w, "bad request: invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if p.SessionID == "" {
		http.Error(w, "missing sessionId", http.StatusBadRequest)
		return
	}
	if p.AppType == "" {
		http.Error(w, "missing appType", http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	rows := make([]EventRow, 0, len(p.Events))
	for _, ev := range p.Events {
		eventJSON := string(ev)
		eventType, errorLevel, errorPayload := parseEventFields(ev)
		rows = append(rows, EventRow{
			Ts:           now,
			SessionID:    p.SessionID,
			AppType:      p.AppType,
			EventJSON:    eventJSON,
			EventType:    eventType,
			ErrorLevel:   errorLevel,
			ErrorPayload: errorPayload,
		})
	}

	// Log the request
	log.Printf("[POST /ingest] session_id=%s app_type=%s events=%d", p.SessionID, p.AppType, len(rows))

	// enqueue batch (non-blocking). If full, return 429.
	select {
	case ingestCh <- rows:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("queued"))
	default:
		// queue full; reject or fall back to direct write (here: reject)
		log.Printf("[POST /ingest] session_id=%s events=%d ERROR: queue full", p.SessionID, len(rows))
		http.Error(w, "server busy", http.StatusTooManyRequests)
	}
}

// recordUserSessionHandler registers a user_id against a session_id.
func recordUserSessionHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var p RegisterUserPayload
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&p); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if p.SessionID == "" {
		http.Error(w, "missing sessionId", http.StatusBadRequest)
		return
	}
	if p.AppType == "" {
		http.Error(w, "missing appType", http.StatusBadRequest)
		return
	}
	if p.UserID == "" {
		http.Error(w, "missing userId", http.StatusBadRequest)
		return
	}

	// Log the request
	log.Printf("[POST /record_user_session] session_id=%s app_type=%s user_id=%s", p.SessionID, p.AppType, p.UserID)

	// Insert or update in user_sessions table
	userSessionsDB := filepath.Join(duckDir, "user_sessions.duckdb")
	db, err := sql.Open("duckdb", userSessionsDB)
	if err != nil {
		log.Printf("[POST /record_user_session] session_id=%s user_id=%s ERROR: failed to open DB: %v", p.SessionID, p.UserID, err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Delete existing record if it exists, then insert new one
	// _, err = db.Exec("DELETE FROM user_sessions WHERE session_id = ?", p.SessionID)
	// if err != nil {
	// 	log.Printf("[POST /record_user_session] session_id=%s user_id=%s ERROR: delete failed: %v", p.SessionID, p.UserID, err)
	// }

	now := time.Now().UTC()
	_, err = db.Exec("INSERT INTO user_sessions (session_id, app_type, user_id, created_at) VALUES (?, ?, ?, ?)",
		p.SessionID, p.AppType, p.UserID, now.Format(time.RFC3339Nano))
	if err != nil {
		log.Printf("[POST /record_user_session] session_id=%s user_id=%s ERROR: insert failed: %v", p.SessionID, p.UserID, err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(map[string]interface{}{
		"status":    "registered",
		"sessionId": p.SessionID,
		"appType":   p.AppType,
		"userId":    p.UserID,
	})
}

// writerWorker receives batches and writes them into the current day's DuckDB.
// It opens/closes the daily DB as the day changes. Each batch is inserted
// inside a transaction for speed.
// When ingestCh is closed, it will exit after processing remaining batches
// and properly closing the database connection.
// Periodically checkpoints the WAL to prevent it from persisting during idle periods.
func writerWorker() {
	defer wg.Done()

	var currentDay string
	var db *sql.DB
	var err error

	// Ticker for periodic checkpointing during idle periods
	checkpointTicker := time.NewTicker(checkpointInterval)
	defer checkpointTicker.Stop()

	for {
		select {
		case batch, ok := <-ingestCh:
			if !ok {
				// Channel closed, exit
				goto cleanup
			}

			// Log batch info (all events in batch have same session_id)
			if len(batch) > 0 {
				log.Printf("[writerWorker] inserting batch session_id=%s app_type=%s events=%d", batch[0].SessionID, batch[0].AppType, len(batch))
			}

			// determine day for the batch (UTC date)
			day := time.Now().UTC().Format("2006_01_02")
			if db == nil || day != currentDay {
				// close old
				if db != nil {
					db.Close()
				}
				// open new DB file for the day
				fpath := filepath.Join(duckDir, fmt.Sprintf("events_%s.duckdb", day))
				// the DSN for go-duckdb is simply the path
				db, err = sql.Open("duckdb", fpath)
				if err != nil {
					log.Printf("failed to open duckdb %s: %v", fpath, err)
					db = nil
					// backoff and requeue batch later? For simplicity, drop this batch
					continue
				}
				currentDay = day
				// ensure table exists
				if err := ensureTable(db); err != nil {
					log.Printf("failed to ensure table: %v", err)
				}
			}

			if db == nil {
				// if DB unavailable, drop or buffer (we drop here)
				continue
			}

			// write batch in transaction
			tx, err := db.Begin()
			if err != nil {
				log.Printf("begin tx err: %v", err)
				continue
			}
			stmt, err := tx.Prepare("INSERT INTO events (ts, session_id, app_type, event, event_type, error_level, error_payload) VALUES (?, ?, ?, ?, ?, ?, ?)")
			if err != nil {
				log.Printf("prepare err: %v", err)
				tx.Rollback()
				continue
			}

			for _, r := range batch {
				_, err := stmt.Exec(
					r.Ts.Format(time.RFC3339Nano),
					r.SessionID,
					r.AppType,
					r.EventJSON,
					nullString(r.EventType),
					nullString(r.ErrorLevel),
					nullString(r.ErrorPayload),
				)
				if err != nil {
					log.Printf("insert err: %v", err)
					// continue inserting remaining rows
					continue
				}
			}

			stmt.Close()
			if err := tx.Commit(); err != nil {
				if len(batch) > 0 {
					log.Printf("[writerWorker] session_id=%s events=%d ERROR: commit failed: %v", batch[0].SessionID, len(batch), err)
				}
				continue
			}

			// Log successful insert
			if len(batch) > 0 {
				log.Printf("[writerWorker] inserted batch session_id=%s app_type=%s events=%d", batch[0].SessionID, batch[0].AppType, len(batch))
			}

		case <-checkpointTicker.C:
			// Periodic checkpoint during idle periods
			if db != nil {
				if err := checkpointDB(db); err != nil {
					log.Printf("[writerWorker] periodic checkpoint error: %v", err)
				}
			}
		}
	}

cleanup:
	// Final checkpoint before closing
	if db != nil {
		if err := checkpointDB(db); err != nil {
			log.Printf("[writerWorker] final checkpoint error: %v", err)
		}
		log.Println("[writerWorker] closing database connection on shutdown")
		if err := db.Close(); err != nil {
			log.Printf("[writerWorker] error closing database: %v", err)
		}
	}
	log.Println("[writerWorker] worker stopped")
}

func initUserSessionsDB(dbPath string) error {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS user_sessions (
		session_id VARCHAR,
		app_type VARCHAR,
		user_id VARCHAR,
		created_at TIMESTAMP,
		PRIMARY KEY (session_id, app_type)
	);
	`)
	if err != nil {
		return err
	}
	_, _ = db.Exec(`ALTER TABLE user_sessions ADD COLUMN IF NOT EXISTS app_type VARCHAR;`)
	return nil
}

func ensureTable(db *sql.DB) error {
	// DuckDB uses SQL standard types; event stored as JSON (TEXT/JSON)
	// Note: some DuckDB builds have JSON functions; to be portable we store JSON text
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS events (
		ts TIMESTAMP,
		session_id VARCHAR,
		app_type VARCHAR,
		event TEXT,
		event_type VARCHAR,
		error_level VARCHAR,
		error_payload TEXT
	);
	`)
	if err != nil {
		return err
	}
	// Add columns for backward compatibility with existing tables
	_, _ = db.Exec(`ALTER TABLE events ADD COLUMN IF NOT EXISTS app_type VARCHAR;`)
	_, _ = db.Exec(`ALTER TABLE events ADD COLUMN IF NOT EXISTS event_type VARCHAR;`)
	_, _ = db.Exec(`ALTER TABLE events ADD COLUMN IF NOT EXISTS error_level VARCHAR;`)
	_, _ = db.Exec(`ALTER TABLE events ADD COLUMN IF NOT EXISTS error_payload TEXT;`)

	// Create indexes for faster error queries
	_, _ = db.Exec(`CREATE INDEX IF NOT EXISTS idx_error_level ON events(error_level);`)
	_, _ = db.Exec(`CREATE INDEX IF NOT EXISTS idx_error_payload ON events(error_payload);`)
	_, _ = db.Exec(`CREATE INDEX IF NOT EXISTS idx_ts ON events(ts);`)
	_, _ = db.Exec(`CREATE INDEX IF NOT EXISTS idx_session_app ON events(session_id, app_type);`)

	return nil
}

// checkpointDB checkpoints the WAL file, merging it into the main database file.
// This prevents WAL files from persisting during idle periods.
func checkpointDB(db *sql.DB) error {
	_, err := db.Exec("CHECKPOINT")
	return err
}

// parseEventFields extracts event_type, error_level, and error_payload from event JSON
func parseEventFields(eventJSON json.RawMessage) (eventType, errorLevel, errorPayload string) {
	var event map[string]interface{}
	if err := json.Unmarshal(eventJSON, &event); err != nil {
		return "", "", ""
	}

	// Extract "type" field
	if t, ok := event["type"].(string); ok {
		eventType = t
	}

	// Extract "data.payload.level" and "data.payload.payload"
	if data, ok := event["data"].(map[string]interface{}); ok {
		if payload, ok := data["payload"].(map[string]interface{}); ok {
			if level, ok := payload["level"].(string); ok {
				errorLevel = level
			}
			if payloadData, ok := payload["payload"].(string); ok {
				errorPayload = payloadData
			}
		}
	}

	return eventType, errorLevel, errorPayload
}

// nullString returns NULL for empty strings, otherwise returns the string
// This is used for SQL NULL handling
func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// archiveWorker runs periodically to check for day boundary changes and export
// previous day's data from DuckDB to Parquet format
func archiveWorker() {
	defer wg.Done()

	// Check for unarchived days on startup
	if err := archiveUnprocessedDays(); err != nil {
		log.Printf("[archiveWorker] error archiving unprocessed days: %v", err)
	}

	ticker := time.NewTicker(archiveCheckInterval)
	defer ticker.Stop()

	var lastArchivedDay string

	for {
		select {
		case <-archiveStopCh:
			log.Printf("[archiveWorker] stop signal received, exiting")
			return
		case <-ticker.C:
			// Get yesterday's date (previous day to archive)
			yesterday := time.Now().UTC().AddDate(0, 0, -1).Format("2006_01_02")

			// Skip if already archived
			if yesterday == lastArchivedDay {
				continue
			}

			// Check if yesterday's DuckDB file exists
			duckPath := filepath.Join(duckDir, fmt.Sprintf("events_%s.duckdb", yesterday))
			if _, err := os.Stat(duckPath); os.IsNotExist(err) {
				// File doesn't exist, nothing to archive
				continue
			}

			// Export to Parquet
			if err := exportDayToParquet(yesterday); err != nil {
				log.Printf("[archiveWorker] error exporting %s to parquet: %v", yesterday, err)
				continue
			}

			log.Printf("[archiveWorker] successfully archived day %s", yesterday)
			lastArchivedDay = yesterday
		}
	}
}

// archiveUnprocessedDays checks for DuckDB files that haven't been archived yet
func archiveUnprocessedDays() error {
	// Find all DuckDB files
	duckFiles, err := filepath.Glob(filepath.Join(duckDir, "events_*.duckdb"))
	if err != nil {
		return err
	}

	today := time.Now().UTC().Format("2006_01_02")

	for _, duckFile := range duckFiles {
		// Extract date from filename
		baseName := filepath.Base(duckFile)
		day := baseName[len("events_") : len(baseName)-len(".duckdb")]

		// Skip today's file (still being written to)
		if day == today {
			continue
		}

		// Check if Parquet file already exists
		parquetPath := filepath.Join(parquetDir, fmt.Sprintf("events_%s.parquet", day))
		if _, err := os.Stat(parquetPath); err == nil {
			// Parquet already exists, skip
			continue
		}

		// Export to Parquet
		if err := exportDayToParquet(day); err != nil {
			log.Printf("[archiveUnprocessedDays] error exporting %s: %v", day, err)
			continue
		}

		log.Printf("[archiveUnprocessedDays] archived unprocessed day %s", day)
	}

	return nil
}

// exportDayToParquet exports a specific day's data from DuckDB to Parquet format
func exportDayToParquet(day string) error {
	duckPath := filepath.Join(duckDir, fmt.Sprintf("events_%s.duckdb", day))
	parquetPath := filepath.Join(parquetDir, fmt.Sprintf("events_%s.parquet", day))

	// Open DuckDB file
	db, err := sql.Open("duckdb", duckPath)
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	// Check if table exists and has data
	var rowCount int64
	err = db.QueryRow("SELECT COUNT(*) FROM events").Scan(&rowCount)
	if err != nil {
		return fmt.Errorf("failed to check table: %w", err)
	}

	if rowCount == 0 {
		log.Printf("[exportDayToParquet] day %s has no data, skipping", day)
		return nil
	}

	// Export to Parquet using COPY command
	exportSQL := fmt.Sprintf("COPY (SELECT * FROM events) TO '%s' (FORMAT PARQUET)", parquetPath)
	_, err = db.Exec(exportSQL)
	if err != nil {
		return fmt.Errorf("failed to export to Parquet: %w", err)
	}

	// Delete DuckDB file after successful export
	if err := os.Remove(duckPath); err != nil {
		log.Printf("[exportDayToParquet] warning: failed to delete DuckDB file %s: %v", duckPath, err)
		// Don't return error, export was successful
	}

	return nil
}

// replayHandler streams events for a session ordered by ts as NDJSON.
// Queries both DuckDB (current day) and Parquet (historical days) files.
func replayHandler(w http.ResponseWriter, r *http.Request) {
	s := r.URL.Query().Get("session")
	app := r.URL.Query().Get("app")
	if s == "" {
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	}
	if app == "" {
		http.Error(w, "missing app", http.StatusBadRequest)
		return
	}

	log.Printf("[GET /replay] session_id=%s app_type=%s", s, app)

	w.Header().Set("Content-Type", "application/x-ndjson")

	// Query DuckDB files (current day)
	duckFiles, err := filepath.Glob(filepath.Join(duckDir, "events_*.duckdb"))
	if err == nil {
		for _, f := range duckFiles {
			queryEventsFromDuckDB(f, s, app, w)
		}
	}

	// Query Parquet files (historical days)
	parquetFiles, err := filepath.Glob(filepath.Join(parquetDir, "events_*.parquet"))
	if err == nil {
		for _, f := range parquetFiles {
			queryEventsFromParquet(f, s, app, w)
		}
	}
}

// queryEventsFromDuckDB queries events from a DuckDB file
func queryEventsFromDuckDB(dbPath, sessionID, appType string, w http.ResponseWriter) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		log.Printf("open %s err: %v", dbPath, err)
		return
	}
	defer db.Close()

	rows, err := db.Query("SELECT event FROM events WHERE session_id = ? AND app_type = ? ORDER BY ts", sessionID, appType)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ev string
		if err := rows.Scan(&ev); err != nil {
			continue
		}
		w.Write([]byte(ev))
		w.Write([]byte("\n"))
	}
}

// queryEventsFromParquet queries events from a Parquet file using DuckDB's read_parquet function
func queryEventsFromParquet(parquetPath, sessionID, appType string, w http.ResponseWriter) {
	// Use an in-memory DuckDB to query Parquet file
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Printf("failed to open DuckDB for Parquet query: %v", err)
		return
	}
	defer db.Close()

	// Query Parquet file using read_parquet function
	query := fmt.Sprintf(`
		SELECT event 
		FROM read_parquet('%s') 
		WHERE session_id = ? AND app_type = ? 
		ORDER BY ts
	`, parquetPath)

	rows, err := db.Query(query, sessionID, appType)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ev string
		if err := rows.Scan(&ev); err != nil {
			continue
		}
		w.Write([]byte(ev))
		w.Write([]byte("\n"))
	}
}

// queryUserHandler returns metadata for sessions for a given user by querying the database directly.
func queryUserHandler(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	app := r.URL.Query().Get("app")
	if user == "" {
		http.Error(w, "missing user", http.StatusBadRequest)
		return
	}
	if app == "" {
		http.Error(w, "missing app", http.StatusBadRequest)
		return
	}

	log.Printf("[GET /query_user] user_id=%s app_type=%s", user, app)

	// Get all session_ids for this user from user_sessions table
	sessionIDs := make(map[string]bool)
	sessionToUser := make(map[string]string) // session_id -> user_id mapping
	userSessionsDB := filepath.Join(duckDir, "user_sessions.duckdb")
	db, err := sql.Open("duckdb", userSessionsDB)
	if err == nil {
		rows, err := db.Query("SELECT session_id, user_id FROM user_sessions WHERE user_id = ? AND app_type = ?", user, app)
		if err == nil {
			for rows.Next() {
				var sessionID, userID string
				if err := rows.Scan(&sessionID, &userID); err == nil {
					sessionIDs[sessionID] = true
					sessionToUser[sessionID] = userID
				}
			}
			rows.Close()
		}
		db.Close()
	}

	if len(sessionIDs) == 0 {
		log.Printf("[GET /query_user] user_id=%s no sessions found", user)
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.Encode([]*SessionMeta{})
		return
	}

	// Query database directly for session metadata from both DuckDB and Parquet files
	sessionMeta := make(map[string]*SessionMeta)

	// Build placeholders for IN clause
	sessionIDList := make([]interface{}, 0, len(sessionIDs))
	for sid := range sessionIDs {
		sessionIDList = append(sessionIDList, sid)
	}

	// Query DuckDB files
	duckFiles, err := filepath.Glob(filepath.Join(duckDir, "events_*.duckdb"))
	if err == nil {
		for _, f := range duckFiles {
			querySessionMetaFromDuckDB(f, sessionIDList, app, sessionMeta, sessionToUser)
		}
	}

	// Query Parquet files
	parquetFiles, err := filepath.Glob(filepath.Join(parquetDir, "events_*.parquet"))
	if err == nil {
		for _, f := range parquetFiles {
			querySessionMetaFromParquet(f, sessionIDList, app, sessionMeta, sessionToUser)
		}
	}

	// Convert map to slice
	out := make([]*SessionMeta, 0, len(sessionMeta))
	for _, m := range sessionMeta {
		out = append(out, m)
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(out)
}

// querySessionMetaFromDuckDB queries session metadata from a DuckDB file
func querySessionMetaFromDuckDB(dbPath string, sessionIDList []interface{}, app string, sessionMeta map[string]*SessionMeta, sessionToUser map[string]string) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		log.Printf("open %s err: %v", dbPath, err)
		return
	}
	defer db.Close()

	// Build query with IN clause
	placeholders := ""
	for i := 0; i < len(sessionIDList); i++ {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
	}

	query := fmt.Sprintf(`
		SELECT 
			session_id,
			MIN(ts) as first_ts,
			MAX(ts) as last_ts,
			COUNT(*) as event_count,
			SUM(LENGTH(event)) as total_size
		FROM events 
		WHERE session_id IN (%s) AND app_type = ?
		GROUP BY session_id
	`, placeholders)

	args := append(sessionIDList, app)
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("query err: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sessionID string
		var firstTS, lastTS sql.NullTime
		var eventCount int64
		var totalSize sql.NullInt64

		if err := rows.Scan(&sessionID, &firstTS, &lastTS, &eventCount, &totalSize); err != nil {
			continue
		}

		updateSessionMeta(sessionMeta, sessionID, app, sessionToUser, firstTS, lastTS, totalSize, filepath.Base(dbPath))
	}
}

// querySessionMetaFromParquet queries session metadata from a Parquet file
func querySessionMetaFromParquet(parquetPath string, sessionIDList []interface{}, app string, sessionMeta map[string]*SessionMeta, sessionToUser map[string]string) {
	// Use an in-memory DuckDB to query Parquet file
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Printf("failed to open DuckDB for Parquet query: %v", err)
		return
	}
	defer db.Close()

	// Build query with IN clause
	placeholders := ""
	for i := 0; i < len(sessionIDList); i++ {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
	}

	query := fmt.Sprintf(`
		SELECT 
			session_id,
			MIN(ts) as first_ts,
			MAX(ts) as last_ts,
			COUNT(*) as event_count,
			SUM(LENGTH(event)) as total_size
		FROM read_parquet('%s')
		WHERE session_id IN (%s) AND app_type = ?
		GROUP BY session_id
	`, parquetPath, placeholders)

	args := append(sessionIDList, app)
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Printf("query err: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sessionID string
		var firstTS, lastTS sql.NullTime
		var eventCount int64
		var totalSize sql.NullInt64

		if err := rows.Scan(&sessionID, &firstTS, &lastTS, &eventCount, &totalSize); err != nil {
			continue
		}

		updateSessionMeta(sessionMeta, sessionID, app, sessionToUser, firstTS, lastTS, totalSize, filepath.Base(parquetPath))
	}
}

// updateSessionMeta updates or creates session metadata entry
func updateSessionMeta(sessionMeta map[string]*SessionMeta, sessionID, app string, sessionToUser map[string]string, firstTS, lastTS sql.NullTime, totalSize sql.NullInt64, fileName string) {
	if existing, ok := sessionMeta[sessionID]; ok {
		// Update with earliest firstTs and latest lastTs across files
		if existing.UserID == "" && sessionToUser[sessionID] != "" {
			existing.UserID = sessionToUser[sessionID]
		}
		if firstTS.Valid {
			firstTSMs := firstTS.Time.UnixMilli()
			if existing.FirstTS == 0 || firstTSMs < existing.FirstTS {
				existing.FirstTS = firstTSMs
			}
		}
		if lastTS.Valid {
			lastTSMs := lastTS.Time.UnixMilli()
			if lastTSMs > existing.LastTS {
				existing.LastTS = lastTSMs
			}
		}
		if totalSize.Valid {
			existing.Size += totalSize.Int64
		}
	} else {
		// Create new entry
		meta := &SessionMeta{
			SessionID: sessionID,
			AppType:   app,
			UserID:    sessionToUser[sessionID],
			File:      fileName,
		}
		if firstTS.Valid {
			meta.FirstTS = firstTS.Time.UnixMilli()
		}
		if lastTS.Valid {
			meta.LastTS = lastTS.Time.UnixMilli()
		}
		if totalSize.Valid {
			meta.Size = totalSize.Int64
		}
		sessionMeta[sessionID] = meta
	}
}

// debugErrorsHandler queries for error events grouped by error_level and error_payload
// within a date range and returns aggregated counts
func debugErrorsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	app := r.URL.Query().Get("app")
	if app == "" {
		http.Error(w, "missing app", http.StatusBadRequest)
		return
	}

	// Parse date range parameters
	startDateStr := r.URL.Query().Get("start_date")
	endDateStr := r.URL.Query().Get("end_date")

	// Default to last 30 days if not specified
	now := time.Now().UTC()
	var startDate, endDate time.Time
	if startDateStr == "" {
		startDate = now.AddDate(0, 0, -30)
	} else {
		var err error
		startDate, err = time.Parse("2006-01-02", startDateStr)
		if err != nil {
			http.Error(w, "invalid start_date format (use YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
		startDate = startDate.UTC()
	}

	if endDateStr == "" {
		endDate = now
	} else {
		var err error
		endDate, err = time.Parse("2006-01-02", endDateStr)
		if err != nil {
			http.Error(w, "invalid end_date format (use YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
		// Make end_date inclusive by setting to end of day
		endDate = time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 23, 59, 59, 999999999, time.UTC)
	}

	if startDate.After(endDate) {
		http.Error(w, "start_date must be before or equal to end_date", http.StatusBadRequest)
		return
	}

	startTime := time.Now()
	log.Printf("[GET /debug_errors] app_type=%s start_date=%s end_date=%s", app, startDate.Format("2006-01-02"), endDate.Format("2006-01-02"))

	// Load session_id -> user_id mapping from user_sessions table
	sessionToUser := make(map[string]string)
	userSessionsDB := filepath.Join(duckDir, "user_sessions.duckdb")
	db, err := sql.Open("duckdb", userSessionsDB)
	if err == nil {
		mapStart := time.Now()
		rows, err := db.Query("SELECT session_id, user_id FROM user_sessions WHERE app_type = ?", app)
		if err == nil {
			for rows.Next() {
				var sessionID, userID string
				if err := rows.Scan(&sessionID, &userID); err == nil {
					sessionToUser[sessionID] = userID
				}
			}
			rows.Close()
		}
		db.Close()
		log.Printf("[debug_errors] loaded session->user mappings: %d rows in %s", len(sessionToUser), time.Since(mapStart))
	}

	// Aggregate error groups from both DuckDB and Parquet files
	errorGroups := make(map[string]*ErrorGroup) // key: error_level + "|" + error_payload

	// Query DuckDB files
	// duckFiles, err := filepath.Glob(filepath.Join(duckDir, "events_*.duckdb"))
	// if err == nil {
	// 	for _, f := range duckFiles {
	// 		queryErrorGroupsFromDuckDB(f, app, startDate, endDate, errorGroups)
	// 	}
	// }

	// Query Parquet files
	parquetFiles, err := filepath.Glob(filepath.Join(parquetDir, "events_*.parquet"))
	if err == nil {
		log.Printf("[debug_errors] parquet files=%d", len(parquetFiles))
		for _, f := range parquetFiles {
			fileStart := time.Now()
			queryErrorGroupsFromParquet(f, app, startDate, endDate, errorGroups)
			log.Printf("[debug_errors] parquet file=%s processed in %s", filepath.Base(f), time.Since(fileStart))
		}
	}

	// Calculate affected users for each error group
	affectedStart := time.Now()
	calculateAffectedUsers(errorGroups, app, startDate, endDate, sessionToUser)
	log.Printf("[debug_errors] affected users calculated in %s", time.Since(affectedStart))

	// Convert map to slice and sort by count (descending)
	errorGroupList := make([]*ErrorGroup, 0, len(errorGroups))
	for _, group := range errorGroups {
		errorGroupList = append(errorGroupList, group)
	}

	// Simple sort by count (descending)
	for i := 0; i < len(errorGroupList)-1; i++ {
		for j := i + 1; j < len(errorGroupList); j++ {
			if errorGroupList[i].Count < errorGroupList[j].Count {
				errorGroupList[i], errorGroupList[j] = errorGroupList[j], errorGroupList[i]
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(errorGroupList)
	log.Printf("[debug_errors] completed groups=%d total_duration=%s", len(errorGroupList), time.Since(startTime))
}

// calculateAffectedUsers calculates the number of unique users affected by each error group
func calculateAffectedUsers(errorGroups map[string]*ErrorGroup, app string, startDate, endDate time.Time, sessionToUser map[string]string) {
	// Fast path: approximate affectedUsers as affectedSessions to avoid re-scanning files.
	// This keeps the handler responsive. If a precise user count is needed later,
	// we can add an optional "includeUsers=true" flag and perform deeper scans.
	for _, group := range errorGroups {
		group.AffectedUsers = group.AffectedSessions
	}
}

// queryDistinctSessionsForError queries distinct sessions for a specific error from DuckDB
func queryDistinctSessionsForError(dbPath, app string, startDate, endDate time.Time, errorLevel, errorPayload string, userSet map[string]bool, sessionToUser map[string]string) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return
	}
	defer db.Close()

	var rows *sql.Rows
	if errorPayload != "" {
		rows, err = db.Query(`
			SELECT DISTINCT session_id 
			FROM events
			WHERE app_type = ? 
			  AND ts >= ? AND ts <= ?
			  AND error_level = ? AND error_payload = ?
		`, app, startDate, endDate, errorLevel, errorPayload)
	} else {
		rows, err = db.Query(`
			SELECT DISTINCT session_id 
			FROM events
			WHERE app_type = ? 
			  AND ts >= ? AND ts <= ?
			  AND error_level = ? AND (error_payload IS NULL OR error_payload = '')
		`, app, startDate, endDate, errorLevel)
	}

	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			continue
		}
		if userID, ok := sessionToUser[sessionID]; ok && userID != "" {
			userSet[userID] = true
		}
	}
}

// queryDistinctSessionsForErrorFromParquet queries distinct sessions for a specific error from Parquet
func queryDistinctSessionsForErrorFromParquet(parquetPath, app string, startDate, endDate time.Time, errorLevel, errorPayload string, userSet map[string]bool, sessionToUser map[string]string) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return
	}
	defer db.Close()

	var rows *sql.Rows
	if errorPayload != "" {
		rows, err = db.Query(fmt.Sprintf(`
			SELECT DISTINCT session_id 
			FROM read_parquet('%s')
			WHERE app_type = ? 
			  AND ts >= ? AND ts <= ?
			  AND error_level = ? AND error_payload = ?
		`, parquetPath), app, startDate, endDate, errorLevel, errorPayload)
	} else {
		rows, err = db.Query(fmt.Sprintf(`
			SELECT DISTINCT session_id 
			FROM read_parquet('%s')
			WHERE app_type = ? 
			  AND ts >= ? AND ts <= ?
			  AND error_level = ? AND (error_payload IS NULL OR error_payload = '')
		`, parquetPath), app, startDate, endDate, errorLevel)
	}

	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			continue
		}
		if userID, ok := sessionToUser[sessionID]; ok && userID != "" {
			userSet[userID] = true
		}
	}
}

// queryErrorGroupsFromDuckDB queries error groups from a DuckDB file
func queryErrorGroupsFromDuckDB(dbPath, app string, startDate, endDate time.Time, errorGroups map[string]*ErrorGroup) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		log.Printf("open %s err: %v", dbPath, err)
		return
	}
	defer db.Close()

	query := `
		SELECT 
			error_level,
			error_payload,
			COUNT(*) as count,
			MIN(ts) as first_seen,
			MAX(ts) as last_seen,
			COUNT(DISTINCT session_id) as affected_sessions,
			MAX(event) as sample_event
		FROM events
		WHERE app_type = ? 
		  AND ts >= ? AND ts <= ?
		  AND error_level IS NOT NULL
		GROUP BY error_level, error_payload
	`

	rows, err := db.Query(query, app, startDate, endDate)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var errorLevel, errorPayload sql.NullString
		var count, affectedSessions int64
		var firstSeen, lastSeen sql.NullTime
		var sampleEvent sql.NullString

		if err := rows.Scan(&errorLevel, &errorPayload, &count, &firstSeen, &lastSeen, &affectedSessions, &sampleEvent); err != nil {
			continue
		}

		if !errorLevel.Valid {
			continue
		}

		key := errorLevel.String + "|" + nullStringValue(errorPayload)
		if existing, ok := errorGroups[key]; ok {
			existing.Count += count
			existing.AffectedSessions += affectedSessions
			if firstSeen.Valid && (existing.FirstSeen == 0 || firstSeen.Time.UnixMilli() < existing.FirstSeen) {
				existing.FirstSeen = firstSeen.Time.UnixMilli()
			}
			if lastSeen.Valid && lastSeen.Time.UnixMilli() > existing.LastSeen {
				existing.LastSeen = lastSeen.Time.UnixMilli()
			}
		} else {
			group := &ErrorGroup{
				ErrorLevel:       errorLevel.String,
				ErrorPayload:     nullStringValue(errorPayload),
				Count:            count,
				AffectedSessions: affectedSessions,
			}
			if firstSeen.Valid {
				group.FirstSeen = firstSeen.Time.UnixMilli()
			}
			if lastSeen.Valid {
				group.LastSeen = lastSeen.Time.UnixMilli()
			}
			if sampleEvent.Valid {
				group.SampleEvent = sampleEvent.String
			}
			errorGroups[key] = group
		}
	}
}

// queryErrorGroupsFromParquet queries error groups from a Parquet file
func queryErrorGroupsFromParquet(parquetPath, app string, startDate, endDate time.Time, errorGroups map[string]*ErrorGroup) {
	// Use an in-memory DuckDB to query Parquet file
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Printf("failed to open DuckDB for Parquet query: %v", err)
		return
	}
	defer db.Close()

	query := fmt.Sprintf(`
		SELECT 
			error_level,
			error_payload,
			COUNT(*) as count,
			MIN(ts) as first_seen,
			MAX(ts) as last_seen,
			COUNT(DISTINCT session_id) as affected_sessions,
			MAX(event) as sample_event
		FROM read_parquet('%s')
		WHERE app_type = ? 
		  AND ts >= ? AND ts <= ?
		  AND error_level = 'error'
		GROUP BY error_level, error_payload
	`, parquetPath)

	rows, err := db.Query(query, app, startDate, endDate)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var errorLevel, errorPayload sql.NullString
		var count, affectedSessions int64
		var firstSeen, lastSeen sql.NullTime
		var sampleEvent sql.NullString

		if err := rows.Scan(&errorLevel, &errorPayload, &count, &firstSeen, &lastSeen, &affectedSessions, &sampleEvent); err != nil {
			continue
		}

		if !errorLevel.Valid {
			continue
		}

		key := errorLevel.String + "|" + nullStringValue(errorPayload)
		if existing, ok := errorGroups[key]; ok {
			existing.Count += count
			existing.AffectedSessions += affectedSessions
			if firstSeen.Valid && (existing.FirstSeen == 0 || firstSeen.Time.UnixMilli() < existing.FirstSeen) {
				existing.FirstSeen = firstSeen.Time.UnixMilli()
			}
			if lastSeen.Valid && lastSeen.Time.UnixMilli() > existing.LastSeen {
				existing.LastSeen = lastSeen.Time.UnixMilli()
			}
		} else {
			group := &ErrorGroup{
				ErrorLevel:       errorLevel.String,
				ErrorPayload:     nullStringValue(errorPayload),
				Count:            count,
				AffectedSessions: affectedSessions,
			}
			if firstSeen.Valid {
				group.FirstSeen = firstSeen.Time.UnixMilli()
			}
			if lastSeen.Valid {
				group.LastSeen = lastSeen.Time.UnixMilli()
			}
			if sampleEvent.Valid {
				group.SampleEvent = sampleEvent.String
			}
			errorGroups[key] = group
		}
	}
}

// nullStringValue returns empty string for NULL, otherwise returns the string
func nullStringValue(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

// File: go.mod
// module rrweb-duckdb-server

// go 1.20

// require (
//     github.com/duckdb/duckdb-go/v2 v0.0.0-... // use latest
// )

// File: Dockerfile

/*
FROM golang:1.20-alpine AS builder
WORKDIR /app
COPY . .
RUN apk add --no-cache build-base git
RUN go build -o rrweb-duckdb-server ./

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/rrweb-duckdb-server /usr/local/bin/rrweb-duckdb-server
RUN mkdir -p /data/duck
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rrweb-duckdb-server"]
*/

// File: README.md

/*
rrweb-duckdb-go-server

Quickstart:
  - Build: go build -o rrweb-duckdb-server
  - Run:   ./rrweb-duckdb-server
  - Ingest: POST /ingest with JSON { sessionId, appType, events: [...] }
  - Register user: POST /record_user_session with JSON { sessionId, appType, userId }
  - Replay: GET /replay?session=<sessionId>&app=<appType>  (returns NDJSON)
  - Query user: GET /query_user?user=<userId>&app=<appType>

Notes:
  - This is a minimal boilerplate. Add TLS, auth, request validation, metrics, and
    graceful shutdown before productionizing.
  - Tune ingestCh buffer size, batch sizes, and how you determine ts per-event.
  - Consider using hourly partitions if daily files become too large.
  - User-session mappings stored in separate table to avoid storing user_id in events.
*/
