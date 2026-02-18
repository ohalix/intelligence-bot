import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

class SQLiteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        return c

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS signals (
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
            )""")
            self._migrate_signals_table(conn)

            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp)")
            conn.commit()

    def get_meta(self, key: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
            conn.commit()

    
    def _migrate_signals_table(self, conn: sqlite3.Connection) -> None:
        """Ensure older DBs get new columns without breaking startup."""
        try:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        except Exception:
            return

        required: dict[str, str] = {
            # Older DBs may not have these columns:
            "raw_json": "TEXT",
            "signal_score": "REAL",
            "sentiment": "REAL",
        }

        for name, coltype in required.items():
            if name in cols:
                continue
            try:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {coltype}")
            except sqlite3.OperationalError:
                pass

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
        inserted = 0
        with self._conn() as conn:
            for s in signals:
                try:
                    conn.execute("""INSERT OR IGNORE INTO signals(
                        dedup_key, source, type, title, description, url, timestamp, chain, sector, signal_score, sentiment, raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""", (
                        s.get("dedup_key"),
                        s.get("source"),
                        s.get("type"),
                        s.get("title"),
                        s.get("description"),
                        s.get("url"),
                        (s.get("timestamp").isoformat() if hasattr(s.get("timestamp"), "isoformat") else str(s.get("timestamp"))),
                        s.get("chain"),
                        s.get("sector"),
                        float(s.get("signal_score", 0.0)),
                        float(s.get("sentiment", 0.0)),
                        json.dumps(s, default=str),
                    ))
                    if conn.total_changes:
                        inserted += 1
                except Exception:
                    continue
            conn.commit()
        return inserted

    def purge_old(self, max_days: int) -> None:
        cutoff = datetime.utcnow() - timedelta(days=max_days)
        with self._conn() as conn:
            conn.execute("DELETE FROM signals WHERE timestamp < ?", (cutoff.isoformat(),))
            conn.commit()

    def get_signals_since(self, since: datetime, source: Optional[str]=None, limit: int=50) -> List[Dict[str, Any]]:
        q = "SELECT raw_json FROM signals WHERE timestamp >= ?"
        params = [since.isoformat()]
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