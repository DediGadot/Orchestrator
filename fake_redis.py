"""Minimal Redis replacement for local testing without external dependencies."""

from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional, Tuple


class SimpleFakeRedis:
    def __init__(self, decode_responses: bool = True):
        self.decode_responses = decode_responses
        self._store: Dict[str, Deque[str]] = defaultdict(deque)

    def ping(self) -> bool:
        return True

    def lpush(self, key: str, value: str) -> int:
        queue = self._store[key]
        queue.appendleft(value)
        return len(queue)

    def rpush(self, key: str, value: str) -> int:
        queue = self._store[key]
        queue.append(value)
        return len(queue)

    def rpop(self, key: str) -> Optional[str]:
        queue = self._store.get(key)
        if not queue:
            return None
        value = queue.pop()
        if not queue:
            self._store.pop(key, None)
        return value

    def lpop(self, key: str) -> Optional[str]:
        queue = self._store.get(key)
        if not queue:
            return None
        value = queue.popleft()
        if not queue:
            self._store.pop(key, None)
        return value

    def brpop(self, key: str, timeout: Optional[int] = None) -> Optional[Tuple[str, str]]:
        value = self.rpop(key)
        if value is None:
            return None
        return key, value

    def blpop(self, keys, timeout: Optional[int] = None) -> Optional[Tuple[str, str]]:
        if isinstance(keys, (list, tuple)):
            for key in keys:
                value = self.lpop(key)
                if value is not None:
                    return key, value
            return None

        key = keys
        value = self.lpop(key)
        if value is None:
            return None
        return key, value

    def llen(self, key: str) -> int:
        queue = self._store.get(key)
        return len(queue) if queue else 0

    def expire(self, key: str, ttl: int) -> bool:
        # No-op for in-memory implementation
        return True

    def delete(self, key: str) -> int:
        existed = key in self._store
        self._store.pop(key, None)
        return 1 if existed else 0

    def lrem(self, key: str, count: int, value: str) -> int:
        queue = self._store.get(key)
        if not queue:
            return 0

        removed = 0
        if count == 0:
            items = list(queue)
            queue.clear()
            for item in items:
                if item == value:
                    removed += 1
                    continue
                queue.append(item)
        elif count > 0:
            buffer: Deque[str] = deque()
            while queue and removed < count:
                item = queue.popleft()
                if item == value:
                    removed += 1
                    continue
                buffer.append(item)
            queue.extendleft(reversed(buffer))
        else:  # count < 0 remove from tail
            buffer: Deque[str] = deque()
            while queue and removed < abs(count):
                item = queue.pop()
                if item == value:
                    removed += 1
                    continue
                buffer.appendleft(item)
            queue.extend(buffer)

        if not queue:
            self._store.pop(key, None)

        return removed

    # Compatibility alias for redis-py ``del`` method used via getattr
    def __getattr__(self, name):
        if name == 'del':
            return self.delete
        raise AttributeError(name)

    def close(self) -> None:
        self._store.clear()

    def flushdb(self) -> None:
        self._store.clear()

    def lrange(self, key: str, start: int, end: int) -> List[str]:
        queue = list(self._store.get(key, []))
        if end == -1:
            end = len(queue)
        else:
            end += 1
        return queue[start:end]

    def llen_keys(self) -> Dict[str, int]:
        return {k: len(v) for k, v in self._store.items()}
