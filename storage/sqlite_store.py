import json
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


class SQLiteStore:
    """Tiny SQLite-backed store for signals + last-run checkpoint.

    Contract expected by engine/pipeline.py and bot handlers:
      - get_last_run / set_last_run
      - upsert_signals
      - get_signals_since
      - clear_old_signals (best-effort)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
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

            # Note: keep columns aligned with processing/formatter expectations.
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
        """Ensure older DBs get new columns without breaking startup."""
        try:
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(signals)").fetchall()}
        except Exception:
            return

        # Older DBs may have only a subset of columns. We add missing ones
        # to avoid runtime crashes when handlers query newer fields.
        required: Dict[str, str] = {
            "source": "TEXT",
            "type": "TEXT",
            "title": "TEXT",
            "description": "TEXT",
            "url": "TEXT",
            "timestamp": "TEXT",
            "chain": "TEXT",
            "sector": "TEXT",
            "signal_score": "REAL",
            "sentiment": "REAL",
            "raw_json": "TEXT",
        }

        for name, coltype in required.items():
            if name in cols:
                continue
            try:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {coltype}")
            except sqlite3.OperationalError:
                # Column may exist or table locked; ignore.
                pass

    # --- meta helpers ---

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

    # --- checkpoint ---

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

    # --- signals ---

    def upsert_signals(self, signals: List[Dict[str, Any]]) -> int:
        """Insert/update signals by dedup_key. Returns count written."""
        if not signals:
            return 0

        written = 0
        with self._conn() as conn:
            for s in signals:
                dedup_key = s.get("dedup_key") or s.get("id")
                if not dedup_key:
                    # Skip malformed items.
                    continue

                # Normalize timestamp to ISO string.
                ts = s.get("timestamp")
                if isinstance(ts, datetime):
                    ts_str = ts.isoformat()
                elif isinstance(ts, str):
                    ts_str = ts
                else:
                    ts_str = datetime.utcnow().isoformat()

                raw_json = s.get("raw_json")
                if raw_json is None:
                    try:
                        raw_json = json.dumps(s.get("raw", s), default=str)
                    except Exception:
                        raw_json = "{}"

                conn.execute(
                    """INSERT INTO signals(
                        dedup_key, source, type, title, description, url, timestamp,
                        chain, sector, signal_score, sentiment, raw_json
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(dedup_key) DO UPDATE SET
                        source=excluded.source,
                        type=excluded.type,
                        title=excluded.title,
                        description=excluded.description,
                        url=excluded.url,
                        timestamp=excluded.timestamp,
                        chain=excluded.chain,
                        sector=excluded.sector,
                        signal_score=excluded.signal_score,
                        sentiment=excluded.sentiment,
                        raw_json=excluded.raw_json
                    """,
                    (
                        dedup_key,
                        s.get("source"),
                        s.get("type"),
                        s.get("title"),
                        s.get("description"),
                        s.get("url"),
                        ts_str,
                        s.get("chain"),
                        s.get("sector"),
                        s.get("signal_score"),
                        s.get("sentiment"),
                        raw_json,
                    ),
                )
                written += 1

            conn.commit()

        return written

    def get_signals_since(
        self,
        since_ts: datetime,
        source: Optional[str] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch signals newer than since_ts. If source is None, return all."""
        since_iso = since_ts.isoformat() if isinstance(since_ts, datetime) else str(since_ts)

        q = (
            "SELECT dedup_key, source, type, title, description, url, timestamp, "
            "chain, sector, signal_score, sentiment, raw_json "
            "FROM signals WHERE timestamp >= ?"
        )
        params: List[Any] = [since_iso]
        if source:
            q += " AND source = ?"
            params.append(source)
        q += " ORDER BY timestamp DESC LIMIT ?"
        params.append(int(limit))

        with self._conn() as conn:
            rows = conn.execute(q, tuple(params)).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            # Some downstream expects id-like field.
            d.setdefault("id", d.get("dedup_key"))
            out.append(d)
        return out

    def purge_old(self, days: int = 7) -> int:
        """Compatibility: remove signals older than now - days."""
        cutoff = datetime.utcnow() - timedelta(days=int(days))
        return self.clear_old_signals(cutoff)

    def clear_old_signals(self, older_than: datetime) -> int:
        """Best-effort cleanup to keep DB small."""
        older_iso = older_than.isoformat() if isinstance(older_than, datetime) else str(older_than)
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM signals WHERE timestamp < ?", (older_iso,))
            conn.commit()
            return int(cur.rowcount or 0)
