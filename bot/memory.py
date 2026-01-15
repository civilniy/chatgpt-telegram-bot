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
