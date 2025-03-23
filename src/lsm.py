import os

from threading import RLock
from pathlib import Path
from typing import Iterator, List, Any, Optional, Tuple

from error import DatabaseError 
from wal import WALStore
from memtable import MemTable
from sstable import SSTable


class LSMTree:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        try:
            if self.base_path.exists() and self.base_path.is_file():
                raise DatabaseError(f"Cannot create database: '{base_path}' is a file")

            self.base_path.mkdir(parents=True, exist_ok=True)

        except (OSError, FileExistsError) as e:
            raise DatabaseError(f"Failed to initialize database as '{base_path}': {str(e)}")

        self.memtable = MemTable(max_size=1000)
        self.sstables: List[SSTable] = []
        self.max_sstables = 5
        self.lock = RLock()

        self.wal = WALStore(
            str(self.base_path / "data.db"), str(self.base_path / "wal.log")
        )

        self._load_sstables()
        if len(self.sstables) > self.max_sstables:
            self._compact()

    def _load_sstables(self):
        self.sstables.clear()

        for file in sorted(self.base_path.glob("sstable_*.db")):
            self.sstables.append(SSTable(str(file)))

    def set(self, key: str, value: Any):
        with self.lock:
            if not isinstance(key, str):
                raise ValueError("Key must be a string")

            self.wal.set(key, value)

            self.memtable.add(key, value)

            if self.memtable.is_full():
                self._flush_memtable()

    def _flush_memtable(self):
        if not self.memtable.entries:
            return

        sstable = SSTable(str(self.base_path / f"sstable_{len(self.sstables)}.db"))
        sstable.write_memtable(self.memtable)
        self.sstables.append(sstable)

        self.memtable = MemTable()

        self.wal.checkpoint()

        if len(self.sstables) > self.max_sstables:
            self._compact()

    def get(self, key: str) -> Optional[Any]:
        with self.lock:
            if not isinstance(key, str):
                raise ValueError("Key must be a string")

            value = self.memtable.get(key)
            if value is not None:
                return value

            for sstable in reversed(self.sstables):
                value = sstable.get(key)
                if value is not None:
                    return value

            return None

    def range_query(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        with self.lock:
            for key, value in self.memtable.range_scan(start_key, end_key):
                yield (key, value)

            seen_keys = set()
            for sstable in reversed(self.sstables):
                for key, value in sstable.range_scan(start_key, end_key):
                    if key not in seen_keys:
                        seen_keys.add(key)
                        if value is not None:
                            yield (key, value)

    def _compact(self):
        try:
            merged = MemTable(max_size=float("inf"))

            for sstable in self.sstables:
                for key, value in sstable.range_scan("", "~"):
                    merged.add(key, value)

            new_sstable = SSTable(str(self.base_path / "sstable_compacted.db"))
            new_sstable.write_memtable(merged)

            old_files = [sst.filename for sst in self.sstables]
            self.sstables = [new_sstable]

            for file in old_files:
                try:
                    os.remove(file)
                except OSError:
                    pass

        except Exception as e:
            raise DatabaseError(f"Compaction failed: {e}")

    def delete(self, key: str):
        with self.lock:
            self.wal.delete(key)
            self.set(key, None)

    def close(self):
        with self.lock:
            if self.memtable.entries:
                self._flush_memtable()

            self.wal.checkpoint()

if __name__ == "__main__":
    import random
    db = LSMTree("./mydb")

    # Store some user data
    db.set("user:1", {
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    })

    # Read it back
    user = db.get("user:1")
    print(user['name'])  # Prints: Alice

    # Store many items
    for i in range(1000):
        db.set(f"item:{i}", {
            "name": f"Item {i}",
            "price": random.randint(1, 100)
        })

    # Range query example
    print("\nItems 10-15:")
    for key, value in db.range_query("item:10", "item:15"):
        print(f"{key}: {value}")
