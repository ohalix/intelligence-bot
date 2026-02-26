import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utcnow_naive() -> datetime:
    """UTC now as naive datetime to preserve existing SQLite string semantics."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

def _normalize_ts(value: Any) -> str:
    if value is None:
        return _utcnow_naive().isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.isoformat()
    except Exception:
        return str(value)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _as_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return json.dumps(str(value), ensure_ascii=False)


class SQLiteStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(Path(db_path).parent).mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()
        self._migrate()

    def _init_db(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                source TEXT NOT NULL,
                description TEXT,
                published_at TEXT,
                score REAL DEFAULT 0,
                sentiment REAL DEFAULT 0,
                ecosystem TEXT,
                tags TEXT,
                raw_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_published_at ON signals(published_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_url ON signals(url)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_source ON signals(source)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_runs (
                run_date TEXT PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self.conn.commit()

    def _migrate(self):
        cur = self.conn.cursor()
        cur.execute("PRAGMA table_info(signals)")
        cols = {row[1] for row in cur.fetchall()}
        if "ecosystem" not in cols:
            cur.execute("ALTER TABLE signals ADD COLUMN ecosystem TEXT")
        if "tags" not in cols:
            cur.execute("ALTER TABLE signals ADD COLUMN tags TEXT")
        if "raw_json" not in cols:
            cur.execute("ALTER TABLE signals ADD COLUMN raw_json TEXT")
        self.conn.commit()

    def insert_signals(self, signals: List[Dict[str, Any]]) -> int:
        cur = self.conn.cursor()
        inserted = 0
        for s in signals:
            title = str(s.get("title", "")).strip()
            url = str(s.get("url", "")).strip()
            source = str(s.get("source", "unknown")).strip() or "unknown"
            if not title or not url:
                continue

            # Prevent exact duplicate URLs (hard guard), while rolling-window dedup handles fuzzy dupes upstream.
            cur.execute("SELECT 1 FROM signals WHERE url = ? LIMIT 1", (url,))
            if cur.fetchone():
                continue

            published_at = _normalize_ts(s.get("published_at"))
            score = _as_float(s.get("score", 0.0), 0.0)

            sentiment_val = s.get("sentiment", 0.0)
            if isinstance(sentiment_val, (int, float)):
                sentiment = float(sentiment_val)
            else:
                # Backward-compatible fallback for label-based sentiment.
                sentiment_map = {
                    "very_bearish": -1.0,
                    "bearish": -0.5,
                    "negative": -0.5,
                    "neutral": 0.0,
                    "positive": 0.5,
                    "bullish": 0.5,
                    "very_bullish": 1.0,
                }
                sentiment = sentiment_map.get(str(sentiment_val).strip().lower(), 0.0)

            ecosystem = str(s.get("ecosystem", "") or "")
            tags = s.get("tags", [])
            if not isinstance(tags, list):
                tags = [str(tags)]
            description = str(s.get("description", "") or "")
            raw_json = _as_json(s)

            cur.execute(
                """
                INSERT INTO signals (title, url, source, description, published_at, score, sentiment, ecosystem, tags, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (title, url, source, description, published_at, score, sentiment, ecosystem, json.dumps(tags, ensure_ascii=False), raw_json),
            )
            inserted += 1

        self.conn.commit()
        return inserted

    def get_signals_since(self, since: datetime, source: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if since.tzinfo is not None:
            since = since.astimezone(timezone.utc).replace(tzinfo=None)
        cur = self.conn.cursor()
        params: list[Any] = [since.isoformat()]
        where = "published_at >= ?"
        if source:
            where += " AND source = ?"
            params.append(str(source))

        q = f"""
            SELECT id, title, url, source, description, published_at, score, sentiment, ecosystem, tags, raw_json
            FROM signals
            WHERE {where}
            ORDER BY COALESCE(score, 0) DESC, published_at DESC
        """
        if limit is not None:
            q += " LIMIT ?"
            params.append(int(limit))

        cur.execute(q, tuple(params))
        rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            try:
                tags = json.loads(r["tags"]) if r["tags"] else []
                if not isinstance(tags, list):
                    tags = [str(tags)]
            except Exception:
                tags = []
            out.append(
                {
                    "id": r["id"],
                    "title": r["title"],
                    "url": r["url"],
                    "source": r["source"],
                    "description": r["description"] or "",
                    "published_at": r["published_at"],
                    "score": r["score"] if r["score"] is not None else 0.0,
                    "sentiment": r["sentiment"] if r["sentiment"] is not None else 0.0,
                    "ecosystem": r["ecosystem"] or "",
                    "tags": tags,
                }
            )
        return out

    # -------------------------
    # Compatibility helpers
    # -------------------------

    def upsert_signals(self, signals: List[Dict[str, Any]]) -> int:
        """Compatibility alias used by developer scripts.

        We keep behavior consistent with insert_signals (skip duplicate URLs).
        """
        return self.insert_signals(signals)

    def set_meta(self, key: str, value: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO meta (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(key), str(value)),
        )
        self.conn.commit()

    def get_meta(self, key: str) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key = ?", (str(key),))
        row = cur.fetchone()
        if not row:
            return None
        return str(row["value"]) if row["value"] is not None else None

    def purge_older_than(self, days: int = 30) -> int:
        cutoff = _utcnow_naive() - timedelta(days=int(days))
        cur = self.conn.cursor()
        cur.execute("DELETE FROM signals WHERE published_at < ?", (cutoff.isoformat(),))
        deleted = cur.rowcount if cur.rowcount is not None else 0
        self.conn.commit()
        return int(deleted)

    def set_last_run(self, dt: datetime):
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO meta (key, value) VALUES ('last_run_timestamp', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (dt.isoformat(),),
        )
        self.conn.commit()

    def get_last_run(self) -> Optional[datetime]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM meta WHERE key='last_run_timestamp'")
        row = cur.fetchone()
        if not row:
            return None
        s = str(row["value"]).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return None

    def get_manual_run_count(self, run_date: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT count FROM manual_runs WHERE run_date = ?", (run_date,))
        row = cur.fetchone()
        if not row:
            return 0
        try:
            return int(row["count"])
        except Exception:
            return 0

    def increment_manual_run_count(self, run_date: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO manual_runs (run_date, count) VALUES (?, 1)
            ON CONFLICT(run_date) DO UPDATE SET count = count + 1
            """,
            (run_date,),
        )
        self.conn.commit()
        return self.get_manual_run_count(run_date)
