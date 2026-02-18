import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


class SQLiteStore:
    """Small SQLite persistence layer for signals + rolling window metadata."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )"""
            )

            # NOTE: keep schema simple and backward-compatible with earlier iterations.
            # We store the raw signal JSON (raw_json) for formatter/pipeline flexibility,
            # plus a few indexed columns for queries.
            conn.execute(
                """CREATE TABLE IF NOT EXISTS signals (
                    dedup_key TEXT PRIMARY KEY,
                    source TEXT,
                    type TEXT,
                    title TEXT,
                    description TEXT,
                    url TEXT,
                    timestamp TEXT,
                    chain TEXT,
                    sector TEXT,
                    signal_score REAL,
                    sentiment REAL,
                    raw_json TEXT
                )"""
            )

            self._migrate_signals_table(conn)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
            conn.commit()

    def _migrate_signals_table(self, conn: sqlite3.Connection) -> None:
        """Ensure older DBs get required columns without breaking startup."""
        try:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        except Exception:
            return

        required: Dict[str, str] = {
            "raw_json": "TEXT",
            "signal_score": "REAL",
            "sentiment": "REAL",
            "timestamp": "TEXT",
        }

        for name, coltype in required.items():
            if name in cols:
                continue
            try:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {coltype}")
            except sqlite3.OperationalError:
                # Column might exist in some SQLite versions or migrations already.
                pass

    # -----------------
    # Meta helpers
    # -----------------
    def get_meta(self, key: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            conn.commit()

    # -----------------
    # API expected by pipeline/handlers
    # -----------------
    def get_last_run(self) -> Optional[datetime]:
        v = self.get_meta("last_run_timestamp")
        if not v:
            return None
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return None

    def set_last_run(self, ts: datetime) -> None:
        self.set_meta("last_run_timestamp", ts.isoformat())

    def upsert_signals(self, signals: List[Dict[str, Any]]) -> int:
        """Insert signals by dedup_key; ignore duplicates. Returns inserted count."""
        if not signals:
            return 0

        inserted = 0
        with self._conn() as conn:
            for s in signals:
                dedup_key = s.get("dedup_key") or s.get("id")
                if not dedup_key:
                    # No stable identifier => cannot safely deduplicate.
                    continue

                # Timestamp normalization: accept datetime, ISO string, or fallback now.
                ts_val = s.get("timestamp") or s.get("created_at")
                if hasattr(ts_val, "isoformat"):
                    ts_iso = ts_val.isoformat()
                elif isinstance(ts_val, str) and ts_val:
                    ts_iso = ts_val
                else:
                    ts_iso = datetime.utcnow().isoformat()

                try:
                    conn.execute(
                        """INSERT OR IGNORE INTO signals(
                            dedup_key, source, type, title, description, url,
                            timestamp, chain, sector, signal_score, sentiment, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            dedup_key,
                            s.get("source"),
                            s.get("type"),
                            s.get("title"),
                            s.get("description") or s.get("summary"),
                            s.get("url"),
                            ts_iso,
                            s.get("chain"),
                            s.get("sector"),
                            float(s.get("signal_score") or 0.0),
                            float(s.get("sentiment") or 0.0),
                            json.dumps(s, default=str),
                        ),
                    )
                    # total_changes increments on successful insert.
                    if conn.total_changes:
                        inserted += 1
                except Exception:
                    # Keep ingestion resilient: skip bad rows.
                    continue

            conn.commit()

        return inserted

    def purge_old(self, max_days: int) -> None:
        """Delete signals older than max_days."""
        cutoff = datetime.utcnow() - timedelta(days=max_days)
        with self._conn() as conn:
            conn.execute("DELETE FROM signals WHERE timestamp < ?", (cutoff.isoformat(),))
            conn.commit()

    # Backwards-compatible alias used elsewhere
    def clear_old_signals(self, older_than_ts: datetime) -> int:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM signals WHERE timestamp < ?", (older_than_ts.isoformat(),))
            conn.commit()
            return cur.rowcount or 0

    def get_signals_since(
        self,
        since: datetime,
        source: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        q = "SELECT raw_json FROM signals WHERE timestamp >= ?"
        params: List[Any] = [since.isoformat()]
        if source:
            q += " AND source = ?"
            params.append(source)
        q += " ORDER BY signal_score DESC, timestamp DESC LIMIT ?"
        params.append(int(limit))

        with self._conn() as conn:
            rows = conn.execute(q, tuple(params)).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                out.append(json.loads(r["raw_json"]))
            except Exception:
                continue
        return out
