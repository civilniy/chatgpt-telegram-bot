import os
import sqlite3
import time
from typing import List, Tuple

class MemoryStore:
    def __init__(self, db_path: str, owner_id: int):
        self.db_path = db_path
        self.owner_id = owner_id
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        with self._conn() as c:
            c.execute("""
            CREATE TABLE IF NOT EXISTS memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL,
                tags TEXT DEFAULT '',
                importance INTEGER DEFAULT 3,
                created_at INTEGER NOT NULL
            )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_owner_kind ON memories(owner_id, kind)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_owner_created ON memories(owner_id, created_at)")

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def add(self, kind: str, content: str, tags: str = "", importance: int = 3) -> int:
        now = int(time.time())
        with self._conn() as c:
            cur = c.execute(
                "INSERT INTO memories(owner_id, kind, content, tags, importance, created_at) VALUES (?,?,?,?,?,?)",
                (self.owner_id, kind, content.strip(), tags.strip(), int(importance), now)
            )
            return int(cur.lastrowid)

    def latest(self, limit: int = 30) -> List[Tuple[int, str, str, int]]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT id, kind, content, importance FROM memories WHERE owner_id=? ORDER BY created_at DESC LIMIT ?",
                (self.owner_id, int(limit))
            ).fetchall()
        return rows

    def format_context(self, limit: int = 20) -> str:
        rows = self.latest(limit)
        if not rows:
            return ""
        lines = []
        for _id, kind, content, imp in rows:
            lines.append(f"- [{kind}][imp:{imp}] {content}")
        return "\n".join(lines)

    def has_profile(self) -> bool:
        with self._conn() as c:
            row = c.execute(
                "SELECT 1 FROM memories WHERE owner_id=? AND kind='profile' LIMIT 1",
                (self.owner_id,)
            ).fetchone()
            return row is not None

    def dedupe_profiles(self) -> tuple[int, int]:
        """
        Remove duplicate 'profile' rows for this owner_id.
        Keeps one copy of each unique content (prefer higher importance, then newer).
        Returns: (kept, deleted)
        """
        with self._conn() as c:
            rows = c.execute(
                """
                SELECT id, content, importance, created_at
                FROM memories
                WHERE owner_id=? AND kind='profile'
                ORDER BY importance DESC, created_at DESC
                """,
                (self.owner_id,)
            ).fetchall()

            seen = set()
            keep_ids = []
            delete_ids = []

            for _id, content, imp, created_at in rows:
                key = (content or "").strip()
                if key in seen:
                    delete_ids.append(_id)
                else:
                    seen.add(key)
                    keep_ids.append(_id)

            if delete_ids:
                placeholders = ",".join(["?"] * len(delete_ids))
                c.execute(f"DELETE FROM memories WHERE id IN ({placeholders})", delete_ids)

            return (len(keep_ids), len(delete_ids))

    def delete_profiles(self) -> int:
        """Delete all profile rows for this owner. Returns deleted count."""
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM memories WHERE owner_id=? AND kind='profile'",
                (self.owner_id,)
            )
            return int(cur.rowcount or 0)

    
    def delete_duplicates(self, kind: str | None = None) -> int:
        """
        Удаляет дубликаты (одинаковые kind+content+tags), оставляя самую свежую запись.
        Возвращает количество удалённых строк.
        """
        with self._conn() as c:
            if kind:
                cur = c.execute("""
                    DELETE FROM memories
                    WHERE id NOT IN (
                        SELECT MAX(id)
                        FROM memories
                        WHERE owner_id = ?
                          AND kind = ?
                        GROUP BY owner_id, kind, content, tags
                    )
                    AND owner_id = ?
                      AND kind = ?
                """, (self.owner_id, kind, self.owner_id, kind))
            else:
                cur = c.execute("""
                    DELETE FROM memories
                    WHERE id NOT IN (
                        SELECT MAX(id)
                        FROM memories
                        WHERE owner_id = ?
                        GROUP BY owner_id, kind, content, tags
                    )
                    AND owner_id = ?
                """, (self.owner_id, self.owner_id))
            return cur.rowcount

    def delete_exact(self, kind: str, content: str) -> int:
        """
        Удаляет ВСЕ записи с exact совпадением kind+content у текущего owner.
        """
        with self._conn() as c:
            cur = c.execute("""
                DELETE FROM memories
                WHERE owner_id = ?
                  AND kind = ?
                  AND content = ?
            """, (self.owner_id, kind, content))
            return cur.rowcount

