// File: main.go

package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Simple, single-binary Go server that ingests rrweb events from a Next.js frontend
// and writes them into daily DuckDB partition files. It exposes endpoints:
//  - POST /ingest  -> accept JSON { sessionId, userId, events: [ ... ] }
//  - GET  /replay?session=... -> stream NDJSON of events for given session
//  - GET  /query_user?user=... -> return metadata/sessions for a user
//
// The design goals:
//  - very fast inserts via frontend batching + a writer goroutine
//  - daily DuckDB partitions: data/duck/events_YYYY_MM_DD.duckdb
//  - simple metadata kept in-memory with optional persistence file (small footprint)
//  - authentication/authorization left as TODO (add JWT/API keys in production)

const (
	dataDir      = "./data"
	duckDir      = dataDir + "/duck"
	metadataFile = dataDir + "/meta.json" // optional small metadata persistence
)

// EventRow holds one rrweb event mapped to a DB row
type EventRow struct {
	Ts        time.Time `json:"ts"`
	SessionID string    `json:"session_id"`
	UserID    string    `json:"user_id"`
	EventJSON string    `json:"event_json"`
}

// payload from frontend
type IngestPayload struct {
	SessionID string            `json:"sessionId"`
	UserID    string            `json:"userId"`
	Events    []json.RawMessage `json:"events"`
}

// simple in-memory metadata we persist on shutdown occasionally
type SessionMeta struct {
	SessionID string `json:"sessionId"`
	UserID    string `json:"userId"`
	FirstTS   int64  `json:"firstTs"`
	LastTS    int64  `json:"lastTs"`
	Size      int64  `json:"size"`
	File      string `json:"file"`
}

var (
	// channel for batched ingestion; each item is a batch (slice) of EventRow
	ingestCh = make(chan []EventRow, 512)

	// simple metadata map
	metaMu sync.RWMutex
	meta   = map[string]*SessionMeta{} // sessionID -> meta

	// waitgroup for background workers
	wg sync.WaitGroup
)

func main() {
	// ensure directories
	if err := os.MkdirAll(duckDir, 0o755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// start writer worker
	wg.Add(1)
	go writerWorker()

	// HTTP handlers
	http.HandleFunc("/ingest", ingestHandler)
	http.HandleFunc("/replay", replayHandler)
	http.HandleFunc("/query_user", queryUserHandler)

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

// ingestHandler accepts JSON payloads from the browser. It converts incoming
// events into EventRow objects with the current timestamp and enqueues them
// as a single batch on ingestCh. The handler returns quickly (202 Accepted)
// so the frontend can continue.
// Ingest handler: user_id may be empty (anonymous).
// Once user signs in, new events include user_id, and session_id links anonymous+auth events.
func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var p IngestPayload
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&p); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
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
			UserID:    p.UserID,
			EventJSON: string(ev),
		})
	}

	// enqueue batch (non-blocking). If full, return 429.
	select {
	case ingestCh <- rows:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("queued"))
	default:
		// queue full; reject or fall back to direct write (here: reject)
		http.Error(w, "server busy", http.StatusTooManyRequests)
	}
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
		stmt, err := tx.Prepare("INSERT INTO events (ts, session_id, user_id, event) VALUES (?, ?, ?, ?)")
		if err != nil {
			log.Printf("prepare err: %v", err)
			tx.Rollback()
			continue
		}

		var bytesWritten int64
		for _, r := range batch {
			_, err := stmt.Exec(r.Ts.Format(time.RFC3339Nano), r.SessionID, r.UserID, r.EventJSON)
			if err != nil {
				log.Printf("insert err: %v", err)
				// continue inserting remaining rows
				continue
			}
			bytesWritten += int64(len(r.EventJSON))

			// update in-memory metadata
			metaMu.Lock()
			m := meta[r.SessionID]
			if m == nil {
				m = &SessionMeta{SessionID: r.SessionID, UserID: r.UserID, FirstTS: r.Ts.UnixMilli(), File: filepath.Base(dbStatsPath(currentDay))}
				meta[r.SessionID] = m
			}
			if m.FirstTS == 0 || r.Ts.UnixMilli() < m.FirstTS {
				m.FirstTS = r.Ts.UnixMilli()
			}
			m.LastTS = r.Ts.UnixMilli()
			m.Size += int64(len(r.EventJSON))
			metaMu.Unlock()
		}

		stmt.Close()
		tx.Commit()

		// optionally persist metadata periodically; omitted here for simplicity
	}
}

func dbStatsPath(day string) string {
	return filepath.Join(duckDir, fmt.Sprintf("events_%s.duckdb", day))
}

func ensureTable(db *sql.DB) error {
	// DuckDB uses SQL standard types; event stored as JSON (TEXT/JSON)
	// Note: some DuckDB builds have JSON functions; to be portable we store JSON text
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS events (
		ts TIMESTAMP,
		session_id VARCHAR,
		user_id VARCHAR,
		event TEXT
	);
	`)
	return err
}

// replayHandler streams events for a session ordered by ts as NDJSON. To keep
// the implementation simple, we search across all daily files in data/duck.
// A production server could use the in-memory metadata map to limit which files
// to query.
func replayHandler(w http.ResponseWriter, r *http.Request) {
	s := r.URL.Query().Get("session")
	if s == "" {
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	}

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

// queryUserHandler returns metadata for sessions for a given user. We look up
// the in-memory meta map and return any sessions where UserID matches.
func queryUserHandler(w http.ResponseWriter, r *http.Request) {
	user := r.URL.Query().Get("user")
	if user == "" {
		http.Error(w, "missing user", http.StatusBadRequest)
		return
	}

	out := make([]*SessionMeta, 0)
	metaMu.RLock()
	for _, m := range meta {
		if m.UserID == user {
			out = append(out, m)
		}
	}
	metaMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(out)
}

// --- Utility (not used in all flows) ---

// For clarity, add a helper to read persisted metadata from disk (optional)
func loadMetaFromDisk() error {
	f, err := os.Open(metadataFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	var mlist []*SessionMeta
	if err := json.NewDecoder(f).Decode(&mlist); err != nil {
		return err
	}
	metaMu.Lock()
	for _, m := range mlist {
		meta[m.SessionID] = m
	}
	metaMu.Unlock()
	return nil
}

// persist metadata (call occasionally or on shutdown)
func persistMetaToDisk() error {
	metaMu.RLock()
	out := make([]*SessionMeta, 0, len(meta))
	for _, m := range meta {
		out = append(out, m)
	}
	metaMu.RUnlock()
	f, err := os.Create(metadataFile)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
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
  - Ingest: POST /ingest with JSON { sessionId, userId, events: [...] }
  - Replay: GET /replay?session=<sessionId>  (returns NDJSON)

Notes:
  - This is a minimal boilerplate. Add TLS, auth, request validation, metrics, and
    graceful shutdown before productionizing.
  - Tune ingestCh buffer size, batch sizes, and how you determine ts per-event.
  - Consider using hourly partitions if daily files become too large.
*/
