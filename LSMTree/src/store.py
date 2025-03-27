import os
import pickle
from typing import Any, Dict, Optional


class SimpleStore:
    def __init__(self, filename: str):
        self.filename = filename
        self.data: Dict[str, Any] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.filename):
            self.data = pickle.load(f)

    def _save(self):
        with open(self.filename, 'wb') as f:
            pickle.dump(self.data, f)

    def set(self, key: str, value: Any):
        self.data[key] = value
        self._save()

    def get(self, key: str):
        return self.data.get(key)

