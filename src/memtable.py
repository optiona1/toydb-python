import bisect
from typing import List, Optional, Tuple, Any, Iterator


class MemTable:
    def __init__(self, max_size: int = 1000):
        self.entries: List[Tuple[str, Any]] = []
        self.max_size = max_size

    def add(self, key: str, value: Any):
        idx = bisect.bisect_left([k for k, _ in self.entries], key)
        if idx < len(self.entries) and self.entries[idx][0] == key:
            self.entries[idx] = (key, value)
        else:
            self.entries.insert(idx, (key, value))

    def get(self, key: str) -> Optional[Any]:
        idx = bisect.bisect_left([k for k, _ in self.entries], key)
        if idx < len(self.entries) and self.entries[idx][0] == key:
            return self.entries[idx][1]
        return None

    def is_full(self) -> bool:
        return len(self.entries) >= self.max_size

    def range_scan(self, start_key: str, end_key: str) -> Iterator[Tuple[str, Any]]:
        start_idx = bisect.bisect_left([k for k, _ in self.entries], start_key)
        end_idx = bisect.bisect_right([k for k, _ in self.entries], end_key)
        return iter(self.entries[start_idx:end_idx])
