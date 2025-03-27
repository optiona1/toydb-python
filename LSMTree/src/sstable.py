import os
import pickle
import shutil

from typing import Dict, Optional, Any, Iterator, Tuple

from error import DatabaseError
from memtable import MemTable


class SSTable:
    def __init__(self, filename: str):
        self.filename = filename
        self.index: Dict[str, int] = {}

        if os.path.exists(filename):
            self._load_index()

    def _load_index(self):
        try:
            with open(self.filename, "rb") as f:
                f.seek(0)
                index_pos = int.from_bytes(f.read(8), "big")

                f.seek(index_pos)
                self.index = pickle.load(f)
        except (IOError, pickle.PickleError) as e:
            raise DatabaseError(f"Failed to load SSTable index: {e}")

    def write_memtable(self, memtable: MemTable):
        temp_file = f"{self.filename}.tmp"
        try:
            with open(temp_file, "wb") as f:
                index_pos = f.tell()
                f.write(b"\0" * 8)

                for key, value in memtable.entries:
                    offset = f.tell()
                    self.index[key] = offset
                    entry = pickle.dumps((key, value))
                    f.write(len(entry).to_bytes(4, "big"))
                    f.write(entry)

                index_offset = f.tell()
                pickle.dump(self.index, f)

                f.seek(index_pos)
                f.write(index_offset.to_bytes(8, "big"))

                f.flush()
                os.fsync(f.fileno())

            shutil.move(temp_file, self.filename)
        except IOError as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise DatabaseError(f"Failed to write SSTable: {e}")

    def get(self, key: str) -> Optional[Any]:
        if key not in self.index:
            return None

        try:
            with open(self.filename, "rb") as f:
                f.seek(self.index[key])
                size = int.from_bytes(f.read(4), "big")
                entry = pickle.loads(f.read(size))
                return entry[1]

        except (IOError, pickle.PickleError) as e:
            raise DatabaseError(f"Failed to read from SSTable: {e}")

    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        keys = sorted(k for k in self.index.keys() if start_key <= k <= end_key)
        for key in keys:
            value = self.get(key)
            if value is not None:
                yield (key, value)
