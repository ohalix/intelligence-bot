import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


class SQLiteStore:
    """SQLite-backed rolling 24h store with dedup cache + checkpoints."""

    def __init__(self, config_or_path: Any):
        if isinstance(config_or_path, dict):
            self.config = config_or_path
            db_path = (
                self.config.get("storage", {}).get("db_path")
                or os.getenv("SQLITE_DB_PATH")
                or os.path.join(os.path.dirname(__file__), "..", "data", "signals.db")
            )
        else:
            self.config = {}
            db_path = str(config_or_path)

        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedup_key TEXT UNIQUE,
                    source TEXT,
                    type TEXT,
                    title TEXT,
                    text TEXT,
                    description TEXT,
                    url TEXT,
                    timestamp TEXT,
                    signal_score REAL,
                    payload_json TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoints (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            conn.commit()

    def set_checkpoint(self, key: str, value: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO checkpoints(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            conn.commit()

    def get_checkpoint(self, key: str) -> Optional[str]:
        with self._conn() as conn:
            cur = conn.execute("SELECT value FROM checkpoints WHERE key=?", (key,))
            row = cur.fetchone()
            return row[0] if row else None

    def _to_row(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        ts = signal.get("timestamp")
        if isinstance(ts, datetime):
            ts_str = ts.isoformat()
        elif isinstance(ts, str):
            ts_str = ts
        else:
            ts_str = datetime.utcnow().isoformat()

        return {
            "dedup_key": signal.get("dedup_key") or signal.get("id") or signal.get("url") or json.dumps(signal, sort_keys=True)[:64],
            "source": signal.get("source"),
            "type": signal.get("type"),
            "title": signal.get("title"),
            "text": signal.get("text"),
            "description": signal.get("description"),
            "url": signal.get("url"),
            "timestamp": ts_str,
            "signal_score": float(signal.get("signal_score", 0.0) or 0.0),
            "payload_json": json.dumps(signal, default=str),
        }

    def store_signal(self, signal: Dict[str, Any]) -> bool:
        row = self._to_row(signal)
        with self._conn() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO signals(dedup_key, source, type, title, text, description, url, timestamp, signal_score, payload_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["dedup_key"],
                        row["source"],
                        row["type"],
                        row["title"],
                        row["text"],
                        row["description"],
                        row["url"],
                        row["timestamp"],
                        row["signal_score"],
                        row["payload_json"],
                    ),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_signals(self, limit: int = 10, source: Optional[str] = None) -> List[Dict[str, Any]]:
        q = "SELECT payload_json FROM signals"
        params: List[Any] = []
        if source:
            q += " WHERE source=?"
            params.append(source)
        q += " ORDER BY signal_score DESC, timestamp DESC LIMIT ?"
        params.append(int(limit))

        out: List[Dict[str, Any]] = []
        with self._conn() as conn:
            cur = conn.execute(q, params)
            for (payload,) in cur.fetchall():
                try:
                    out.append(json.loads(payload))
                except Exception:
                    pass
        return out

    def purge_old_signals(self, hours: int = 24) -> int:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        cutoff_iso = cutoff.isoformat()
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM signals WHERE timestamp < ?", (cutoff_iso,))
            conn.commit()
            return cur.rowcount or 0
