
"""
A minimal Python version of the rrweb DuckDB ingestion server.
Features:
  - FastAPI HTTP server
  - Daily DuckDB partition files
  - Batched ingestion via background queue
  - Replay & user queries

Dependencies:
  pip install fastapi uvicorn duckdb
"""

import os
import json
import time
import glob
import duckdb
import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# enable cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path("./data")
DUCK_DIR = DATA_DIR / "duck"
DUCK_DIR.mkdir(parents=True, exist_ok=True)

# ---- Models ----
class IngestPayload(BaseModel):
    sessionId: str
    userId: Optional[str] = ""
    events: List[dict]

# EventRow structure
class EventRow:
    def __init__(self, ts, session_id, user_id, event_json):
        self.ts = ts
        self.session_id = session_id
        self.user_id = user_id
        self.event_json = event_json

# ---- In-memory metadata ----
meta = {}
meta_lock = asyncio.Lock()

# ---- Async ingestion queue ----
ingest_queue = asyncio.Queue(maxsize=1024)

# Background task
@app.on_event("startup")
async def start_worker():
    asyncio.create_task(writer_worker())

async def writer_worker():
    current_day = None
    conn = None

    while True:
        batch = await ingest_queue.get()
        day = datetime.now(timezone.utc).strftime("%Y_%m_%d")

        if conn is None or day != current_day:
            if conn is not None:
                conn.close()
            fpath = DUCK_DIR / f"events_{day}.duckdb"
            conn = duckdb.connect(str(fpath))
            current_day = day
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    ts TIMESTAMP,
                    session_id VARCHAR,
                    user_id VARCHAR,
                    event TEXT
                );
                """
            )

        # Batch insert
        rows = [(
            r.ts,
            r.session_id,
            r.user_id,
            r.event_json,
        ) for r in batch]

        conn.executemany(
            "INSERT INTO events (ts, session_id, user_id, event) VALUES (?, ?, ?, ?)",
            rows,
        )

        # update metadata
        async with meta_lock:
            for r in batch:
                m = meta.get(r.session_id)
                ts_ms = int(r.ts.timestamp() * 1000)
                if not m:
                    m = {
                        "sessionId": r.session_id,
                        "userId": r.user_id,
                        "firstTs": ts_ms,
                        "lastTs": ts_ms,
                        "size": len(r.event_json),
                        "file": f"events_{day}.duckdb",
                    }
                    meta[r.session_id] = m
                else:
                    m["lastTs"] = ts_ms
                    m["size"] += len(r.event_json)

        ingest_queue.task_done()

# ---- Endpoints ----
@app.post("/ingest")
async def ingest(payload: IngestPayload):
    if not payload.sessionId:
        raise HTTPException(400, "missing sessionId")

    now = datetime.now(timezone.utc)
    batch = [
        EventRow(
            ts=now,
            session_id=payload.sessionId,
            user_id=payload.userId or "",
            event_json=json.dumps(ev),
        )
        for ev in payload.events
    ]

    try:
        ingest_queue.put_nowait(batch)
    except asyncio.QueueFull:
        raise HTTPException(429, "server busy")

    return {"status": "queued"}

@app.get("/replay")
async def replay(session: str):
    if not session:
        raise HTTPException(400, "missing session")

    files = glob.glob(str(DUCK_DIR / "events_*.duckdb"))
    results = []

    for f in files:
        try:
            conn = duckdb.connect(f)
            rows = conn.execute(
                "SELECT event FROM events WHERE session_id = ? ORDER BY ts", [session]
            ).fetchall()
            for (ev,) in rows:
                results.append(json.loads(ev))
        except:
            continue

    return results

@app.get("/query_user")
async def query_user(user: str):
    if not user:
        raise HTTPException(400, "missing user")

    async with meta_lock:
        out = [m for m in meta.values() if m["userId"] == user]
    return out


