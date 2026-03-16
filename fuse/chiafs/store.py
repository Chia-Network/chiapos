# Copyright 2025 Chia Network Inc
# Backing store for chiafs: metadata (SQLite) + file data in _chiafs/data/

import os
import sqlite3
import stat
import time
import errno
from pathlib import Path

# Root directory: rwxr-xr-x so listing and traversal work.
ROOT_MODE = stat.S_IFDIR | 0o755
from typing import Optional, List, Tuple, Iterator

META_DIR = "_chiafs"
DATA_DIR = "_chiafs/data"
DB_PATH = "_chiafs/meta.db"


def _get_ino_path(base: Path, ino: int) -> Path:
    return base / DATA_DIR / str(ino)


class Store:
    """Stores FUSE filesystem metadata and file data under backing_dir."""

    def __init__(self, backing_dir: str):
        self.backing = Path(backing_dir).resolve()
        self.meta_dir = self.backing / META_DIR
        self.data_dir = self.backing / DATA_DIR
        self.db_path = self.backing / DB_PATH
        self._db: Optional[sqlite3.Connection] = None
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _get_db(self) -> sqlite3.Connection:
        if self._db is None:
            self._db = sqlite3.connect(str(self.db_path))
            self._db.execute("""
                CREATE TABLE IF NOT EXISTS inodes (
                    ino INTEGER PRIMARY KEY,
                    parent_ino INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    mode INTEGER NOT NULL,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    ctime_ns INTEGER NOT NULL,
                    is_dir INTEGER NOT NULL,
                    UNIQUE(parent_ino, name)
                )
            """)
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS next_ino (next INTEGER NOT NULL)"
            )
            cur = self._db.execute("SELECT next FROM next_ino")
            if cur.fetchone() is None:
                self._db.execute(
                    "INSERT INTO next_ino (next) VALUES (2)"
                )  # 1 = root
            self._db.commit()
        return self._db

    @property
    def conn(self) -> sqlite3.Connection:
        return self._get_db()

    def next_ino(self) -> int:
        cur = self.conn.execute("SELECT next FROM next_ino")
        row = cur.fetchone()
        n = row[0]
        self.conn.execute("UPDATE next_ino SET next = ?", (n + 1,))
        self.conn.commit()
        return n

    def get_root(self) -> Optional[Tuple[int, int, str, int, int, int, int, bool]]:
        cur = self.conn.execute(
            "SELECT ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir FROM inodes WHERE ino = 1"
        )
        row = cur.fetchone()
        if row is not None:
            return row
        # Create root
        now = time.time_ns()
        self.conn.execute(
            "INSERT INTO inodes (ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir) VALUES (1, 1, '', ?, 0, ?, ?, 1)",
            (ROOT_MODE, now, now),
        )
        self.conn.commit()
        return (1, 1, "", ROOT_MODE, 0, now, now, True)

    def lookup(self, parent_ino: int, name: str) -> Optional[Tuple[int, int, str, int, int, int, int, bool]]:
        cur = self.conn.execute(
            "SELECT ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir FROM inodes WHERE parent_ino = ? AND name = ?",
            (parent_ino, name),
        )
        return cur.fetchone()

    def get_attr(self, ino: int) -> Optional[Tuple[int, int, str, int, int, int, int, bool]]:
        if ino == 1:
            return self.get_root()
        cur = self.conn.execute(
            "SELECT ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir FROM inodes WHERE ino = ?",
            (ino,),
        )
        return cur.fetchone()

    def list_dir(self, parent_ino: int) -> List[Tuple[str, int, int]]:
        cur = self.conn.execute(
            "SELECT name, ino, is_dir FROM inodes WHERE parent_ino = ? ORDER BY name",
            (parent_ino,),
        )
        return [(row[0], row[1], row[2]) for row in cur.fetchall()]

    def create_file(self, parent_ino: int, name: str, mode: int) -> int:
        ino = self.next_ino()
        now = time.time_ns()
        self.conn.execute(
            "INSERT INTO inodes (ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir) VALUES (?, ?, ?, ?, 0, ?, ?, 0)",
            (ino, parent_ino, name, mode, now, now),
        )
        self.conn.commit()
        path = _get_ino_path(self.backing, ino)
        path.touch()
        return ino

    def create_dir(self, parent_ino: int, name: str, mode: int) -> int:
        ino = self.next_ino()
        now = time.time_ns()
        self.conn.execute(
            "INSERT INTO inodes (ino, parent_ino, name, mode, size, mtime_ns, ctime_ns, is_dir) VALUES (?, ?, ?, ?, 0, ?, ?, 1)",
            (ino, parent_ino, name, mode, now, now),
        )
        self.conn.commit()
        return ino

    def unlink(self, parent_ino: int, name: str) -> Optional[int]:
        row = self.lookup(parent_ino, name)
        if row is None:
            return None
        ino, _, _, _, size, _, _, is_dir = row
        if is_dir:
            return None
        self.conn.execute("DELETE FROM inodes WHERE ino = ?", (ino,))
        self.conn.commit()
        path = _get_ino_path(self.backing, ino)
        if path.exists():
            path.unlink()
        return ino

    def rmdir(self, parent_ino: int, name: str) -> Optional[int]:
        row = self.lookup(parent_ino, name)
        if row is None:
            return None
        ino, _, _, _, size, _, _, is_dir = row
        if not is_dir:
            return None
        cur = self.conn.execute("SELECT COUNT(*) FROM inodes WHERE parent_ino = ?", (ino,))
        if cur.fetchone()[0] != 0:
            return None
        self.conn.execute("DELETE FROM inodes WHERE ino = ?", (ino,))
        self.conn.commit()
        return ino

    def set_size(self, ino: int, size: int) -> None:
        now = time.time_ns()
        self.conn.execute(
            "UPDATE inodes SET size = ?, mtime_ns = ? WHERE ino = ?",
            (size, now, ino),
        )
        self.conn.commit()

    def get_data_path(self, ino: int) -> Path:
        return _get_ino_path(self.backing, ino)

    def get_file_size(self, ino: int) -> int:
        path = self.get_data_path(ino)
        if path.exists():
            return path.stat().st_size
        return 0

    def total_data_size(self) -> int:
        total = 0
        for p in self.data_dir.iterdir():
            if p.is_file():
                total += p.stat().st_size
        return total

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None
