import os
import json
import pickle
import shutil
from datetime import datetime, timezone

from typing import Any, Dict


class WALEntry:
    def __init__(self, operation: str, key: str, value: Any) -> None:
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.operation = operation
        self.key = key
        self.value = value

    def serialize(self) -> str:
        return json.dumps({
            'timestamp': self.timestamp,
            'operation': self.operation,
            'key': self.key,
            'value': self.value
        })


class WALStore:
    def __init__(self, data_file: str, wal_file: str) -> None:
        self.data_file = data_file
        self.wal_file = wal_file
        self.data: Dict[str, Any] = {}
        self._recover()

    def _append_wal(self, entry: WALEntry):
        try:
            with open(self.wal_file, "a") as f:
                f.write(entry.serialize() + "\n")
                f.flush()
                os.fsync(f.fileno())
        except IOError as e:
            raise DatabaseError(f"Failed to write to WAL: {e}")

    def _recover(self):
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, "rb") as f:
                    self.data =  pickle.load(f)

            if os.path.exists(self.wal_file):
                with open(self.wal_file, "r") as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            if entry["operation"] == "set":
                                self.data[entry["key"]] = entry["value"]
                            elif entry["operation"] == "delete":
                                self.data.pop(entry["key"], None)
        except (IOError, json.JSONDecodeError, pickle.PickleError) as e:
            raise DatabaseError(f"Recovery failed: {e}")

    def set(self, key: str, value: Any):
        entry = WALEntry("set", key, value)
        self._append_wal(entry)
        self.data[key] = value

    def delete(self, key: str):
        entry = WALEntry("delete", key, None)
        self._append_wal(entry)
        self.data.pop(key, None)

    def checkpoint(self):
        temp_file = f"{self.data_file}.tmp"
        try:
            with open(temp_file, "wb") as f:
                pickle.dump(self.data, f)
                f.flush()
                os.fsync(f.fileno())

            shutil.move(temp_file, self.data_file)

            if os.path.exists(self.wal_file):
                with open(self.wal_file, "r+") as f:
                    f.truncate(0)
                    f.flush()
                    os.fsync(f.fileno())
        except IOError as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise DatabaseError(f"Checkpoint failed: {e}")
