// File: main.go

package main

import (
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Simple, single-binary Go server that ingests rrweb events from a Next.js frontend
// and writes them into daily DuckDB partition files. It exposes endpoints:
//  - POST /ingest  -> accept JSON { sessionId, events: [ ... ] }
//  - POST /record_user_session -> register user_id against session_id
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
	dataDir = "./data"
	duckDir = dataDir + "/duck"
)

// EventRow holds one rrweb event mapped to a DB row
type EventRow struct {
	Ts        time.Time `json:"ts"`
	SessionID string    `json:"session_id"`
	EventJSON string    `json:"event_json"`
}

// payload from frontend
type IngestPayload struct {
	SessionID string            `json:"sessionId"`
	Events    []json.RawMessage `json:"events"`
}

// payload for registering user against session
type RegisterUserPayload struct {
	SessionID string `json:"sessionId"`
	UserID    string `json:"userId"`
}

// session metadata returned by queries
type SessionMeta struct {
	SessionID string `json:"sessionId"`
	UserID    string `json:"userId,omitempty"`
	FirstTS   int64  `json:"firstTs"`
	LastTS    int64  `json:"lastTs"`
	Size      int64  `json:"size"`
	File      string `json:"file"`
}

var (
	// channel for batched ingestion; each item is a batch (slice) of EventRow
	ingestCh = make(chan []EventRow, 512)

	// waitgroup for background workers
	wg sync.WaitGroup
)

func main() {
	// ensure directories
	if err := os.MkdirAll(duckDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// initialize user-sessions database
	userSessionsDB := filepath.Join(duckDir, "user_sessions.duckdb")
	if err := initUserSessionsDB(userSessionsDB); err != nil {
		log.Fatalf("failed to init user_sessions db: %v", err)
	}

	// start writer worker
	wg.Add(1)
	go writerWorker()

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

	addr := ":8080"
	log.Printf("listening %s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("http server failed: %v", err)
	}

	// wait for background tasks if server lifts graceful shutdown (not implemented here)
	wg.Wait()
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

	now := time.Now().UTC()
	rows := make([]EventRow, 0, len(p.Events))
	for _, ev := range p.Events {
		rows = append(rows, EventRow{
			Ts:        now,
			SessionID: p.SessionID,
			EventJSON: string(ev),
		})
	}

	// Log the request
	log.Printf("[POST /ingest] session_id=%s events=%d", p.SessionID, len(rows))

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
	if p.UserID == "" {
		http.Error(w, "missing userId", http.StatusBadRequest)
		return
	}

	// Log the request
	log.Printf("[POST /record_user_session] session_id=%s user_id=%s", p.SessionID, p.UserID)

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
	_, err = db.Exec("INSERT INTO user_sessions (session_id, user_id, created_at) VALUES (?, ?, ?)",
		p.SessionID, p.UserID, now.Format(time.RFC3339Nano))
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
		"userId":    p.UserID,
	})
}

// writerWorker receives batches and writes them into the current day's DuckDB.
// It opens/closes the daily DB as the day changes. Each batch is inserted
// inside a transaction for speed.
func writerWorker() {
	defer wg.Done()

	var currentDay string
	var db *sql.DB
	var err error

	for batch := range ingestCh {
		// Log batch info (all events in batch have same session_id)
		if len(batch) > 0 {
			log.Printf("[writerWorker] inserting batch session_id=%s events=%d", batch[0].SessionID, len(batch))
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
		stmt, err := tx.Prepare("INSERT INTO events (ts, session_id, event) VALUES (?, ?, ?)")
		if err != nil {
			log.Printf("prepare err: %v", err)
			tx.Rollback()
			continue
		}

		for _, r := range batch {
			_, err := stmt.Exec(r.Ts.Format(time.RFC3339Nano), r.SessionID, r.EventJSON)
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
			log.Printf("[writerWorker] inserted batch session_id=%s events=%d", batch[0].SessionID, len(batch))
		}
	}
}


func initUserSessionsDB(dbPath string) error {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS user_sessions (
		session_id VARCHAR PRIMARY KEY,
		user_id VARCHAR,
		created_at TIMESTAMP
	);
	`)
	return err
}

func ensureTable(db *sql.DB) error {
	// DuckDB uses SQL standard types; event stored as JSON (TEXT/JSON)
	// Note: some DuckDB builds have JSON functions; to be portable we store JSON text
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS events (
		ts TIMESTAMP,
		session_id VARCHAR,
		event TEXT
	);
	`)
	return err
}

// replayHandler streams events for a session ordered by ts as NDJSON. To keep
// the implementation simple, we search across all daily files in data/duck.
func replayHandler(w http.ResponseWriter, r *http.Request) {
	s := r.URL.Query().Get("session")
	if s == "" {
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	}

	log.Printf("[GET /replay] session_id=%s", s)

	w.Header().Set("Content-Type", "application/x-ndjson")

	// find all duckdb files
	files, err := filepath.Glob(filepath.Join(duckDir, "events_*.duckdb"))
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	// for each file, open and query matching session rows
	for _, f := range files {
		db, err := sql.Open("duckdb", f)
		if err != nil {
			log.Printf("open %s err: %v", f, err)
			continue
		}
		rows, err := db.Query("SELECT event FROM events WHERE session_id = ? ORDER BY ts", s)
		if err != nil {
			// no table or other error
			db.Close()
			continue
		}
		for rows.Next() {
			var ev string
			if err := rows.Scan(&ev); err != nil {
				continue
			}
			w.Write([]byte(ev))
			w.Write([]byte("\n"))
		}
		rows.Close()
		db.Close()
	}
}

// queryUserHandler returns metadata for sessions for a given user by querying the database directly.
func queryUserHandler(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		http.Error(w, "missing user", http.StatusBadRequest)
		return
	}

	log.Printf("[GET /query_user] user_id=%s", user)

	// Get all session_ids for this user from user_sessions table
	sessionIDs := make(map[string]bool)
	sessionToUser := make(map[string]string) // session_id -> user_id mapping
	userSessionsDB := filepath.Join(duckDir, "user_sessions.duckdb")
	db, err := sql.Open("duckdb", userSessionsDB)
	if err == nil {
		rows, err := db.Query("SELECT session_id, user_id FROM user_sessions WHERE user_id = ?", user)
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

	// Query database directly for session metadata
	files, err := filepath.Glob(filepath.Join(duckDir, "events_*.duckdb"))
	if err != nil {
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	sessionMeta := make(map[string]*SessionMeta)

	// Build placeholders for IN clause
	sessionIDList := make([]interface{}, 0, len(sessionIDs))
	for sid := range sessionIDs {
		sessionIDList = append(sessionIDList, sid)
	}

	for _, f := range files {
		db, err := sql.Open("duckdb", f)
		if err != nil {
			log.Printf("open %s err: %v", f, err)
			continue
		}

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
			WHERE session_id IN (%s)
			GROUP BY session_id
		`, placeholders)

		rows, err := db.Query(query, sessionIDList...)
		if err != nil {
			log.Printf("query err: %v", err)
			db.Close()
			continue
		}

		for rows.Next() {
			var sessionID string
			var firstTS, lastTS sql.NullTime
			var eventCount int64
			var totalSize sql.NullInt64

			if err := rows.Scan(&sessionID, &firstTS, &lastTS, &eventCount, &totalSize); err != nil {
				continue
			}

			if existing, ok := sessionMeta[sessionID]; ok {
				// Update with earliest firstTs and latest lastTs across files
				// Preserve userId if not set
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
					UserID:    sessionToUser[sessionID], // Add user_id from mapping
					File:      filepath.Base(f),
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
		rows.Close()
		db.Close()
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
  - Ingest: POST /ingest with JSON { sessionId, events: [...] }
  - Register user: POST /record_user_session with JSON { sessionId, userId }
  - Replay: GET /replay?session=<sessionId>  (returns NDJSON)
  - Query user: GET /query_user?user=<userId>

Notes:
  - This is a minimal boilerplate. Add TLS, auth, request validation, metrics, and
    graceful shutdown before productionizing.
  - Tune ingestCh buffer size, batch sizes, and how you determine ts per-event.
  - Consider using hourly partitions if daily files become too large.
  - User-session mappings stored in separate table to avoid storing user_id in events.
*/
