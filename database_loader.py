#!/usr/bin/env python3
"""
Database Loader with Lazy Loading and Caching
==============================================

Provides efficient access to the rijal database with:
- Full database loaded into memory once (not re-read on every lookup)
- LRU caching: Keeps frequently accessed entries in memory
- Batch loading: Efficient bulk operations
- Memory-efficient iteration
"""

import json
import threading
from pathlib import Path
from collections import OrderedDict
from typing import Optional, Dict, List, Any, Iterator
from rijal_resolver import DATABASE_FILE


class LRUCache:
    """Thread-safe LRU cache implementation."""

    def __init__(self, maxsize: int = 1000):
        self.maxsize = maxsize
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Get item from cache, promoting to most recent."""
        with self.lock:
            if key not in self.cache:
                return None
            # Move to end (most recent)
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: Any):
        """Put item in cache, evicting oldest if at capacity."""
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.maxsize:
                self.cache.popitem(last=False)  # Remove oldest

    def clear(self):
        """Clear all cached items."""
        with self.lock:
            self.cache.clear()

    def __len__(self):
        return len(self.cache)


class DatabaseLoader:
    """
    Database accessor that loads the full database into memory once.

    CRITICAL FIX: The previous implementation re-read the entire JSON file
    from disk on EVERY get_entry() cache miss, causing extreme slowness
    (e.g., ~100 full file reads for a single chain analysis). Now the full
    database is loaded once and served from memory.

    Usage:
        loader = DatabaseLoader()
        entry = loader.get_entry("123")
        for entry in loader.iter_entries():
            # Process entry
            pass
    """

    def __init__(self, db_path: Optional[Path] = None, cache_size: int = 1000):
        self.db_path = db_path or DATABASE_FILE
        self.cache = LRUCache(maxsize=cache_size)
        self._lock = threading.Lock()
        self._full_db: Optional[Dict[str, Dict]] = None  # In-memory full database
        # Load the database into memory once
        self._load_db()

    def _load_db(self):
        """Load the entire database into memory once."""
        if self._full_db is not None:
            return
        with self._lock:
            if self._full_db is not None:
                return
            with open(self.db_path, 'r', encoding='utf-8') as f:
                self._full_db = json.load(f)

    def get_entry(self, entry_key: str) -> Optional[Dict]:
        """Get a single entry by key with caching."""
        # Check cache first
        cached = self.cache.get(entry_key)
        if cached is not None:
            return cached

        # Ensure database is loaded
        self._load_db()

        # Lookup in memory
        entry = self._full_db.get(entry_key) if self._full_db else None
        if entry is not None:
            self.cache.put(entry_key, entry)
        return entry

    def get_entries(self, entry_keys: List[str]) -> Dict[str, Dict]:
        """Get multiple entries by keys."""
        result = {}
        missing_keys = []

        # Check cache for each key
        for key in entry_keys:
            cached = self.cache.get(key)
            if cached is not None:
                result[key] = cached
            else:
                missing_keys.append(key)

        # Load missing entries from the in-memory database
        if missing_keys:
            self._load_db()
            for key in missing_keys:
                entry = self._full_db.get(key) if self._full_db else None
                if entry is not None:
                    result[key] = entry
                    self.cache.put(key, entry)

        return result

    def iter_entries(self) -> Iterator[Dict]:
        """Iterate over all entries in the in-memory database."""
        self._load_db()
        if self._full_db is None:
            return

        for key, entry in self._full_db.items():
            # Cache each entry as we iterate
            self.cache.put(key, entry)
            yield entry

    def search_by_name(self, name_ar: str) -> List[Dict]:
        """Search for entries by Arabic name."""
        results = []
        norm_name = self._normalize_ar(name_ar)

        self._load_db()
        if self._full_db is None:
            return results

        for entry in self._full_db.values():
            entry_name = entry.get('name_ar', '')
            if self._normalize_ar(entry_name) == norm_name:
                results.append(entry)

        return results

    def _normalize_ar(self, text: str) -> str:
        """Normalize Arabic text for comparison."""
        import re
        # Simple normalization (should match rijal_resolver.normalize_ar)
        text = re.sub(r'[أإآٱ]', 'ا', text)
        text = re.sub(r'ـ', '', text)
        text = re.sub(r'[\u064B-\u065F\u0670]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def clear_cache(self):
        """Clear the cache (but keep the in-memory database loaded)."""
        self.cache.clear()

    def get_cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            'size': len(self.cache),
            'maxsize': self.cache.maxsize,
        }

    def close(self):
        """Close the database loader."""
        self.cache.clear()
        # Keep _full_db in memory for reuse - it doesn't consume file handles
        # If you want to truly clear everything:
        # self._full_db = None


# Global instance for backward compatibility
_loader_instance = None
_loader_lock = threading.Lock()


def get_loader() -> DatabaseLoader:
    """Get the global database loader instance."""
    global _loader_instance
    with _loader_lock:
        if _loader_instance is None:
            _loader_instance = DatabaseLoader()
        return _loader_instance


def get_entry(entry_key: str) -> Optional[Dict]:
    """Convenience function to get a single entry."""
    return get_loader().get_entry(entry_key)


def clear_cache():
    """Clear the global cache."""
    global _loader_instance
    with _loader_lock:
        if _loader_instance is not None:
            _loader_instance.clear_cache()


if __name__ == "__main__":
    # Test the loader
    loader = DatabaseLoader()

    print(f"Database path: {loader.db_path}")
    print(f"Cache stats: {loader.get_cache_stats()}")

    # Test getting a single entry
    entry = loader.get_entry("0")
    if entry:
        print(f"\nEntry 0: {entry.get('name_ar')}")

    # Test cache
    entry2 = loader.get_entry("0")
    print(f"\nCache stats after 2 accesses: {loader.get_cache_stats()}")

    # Test iteration
    print("\nFirst 5 entries:")
    for i, entry in enumerate(loader.iter_entries()):
        if i >= 5:
            break
        print(f"  {entry.get('_entry_idx')}: {entry.get('name_ar')}")

    print(f"\nFinal cache stats: {loader.get_cache_stats()}")