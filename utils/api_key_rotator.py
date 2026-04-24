"""Thread-safe API key rotator for avoiding rate limits.

Loads multiple API keys from a text file (one per line) and provides
round-robin rotation with automatic cooldown for rate-limited keys.
"""

import threading
import time
from pathlib import Path
from typing import List, Optional

from utils.logging import log_message


class ApiKeyRotator:
    """Round-robin API key rotator with rate-limit cooldown tracking.

    Thread-safe: multiple batch workers can call get_key() / mark_rate_limited()
    concurrently without corruption.
    """

    def __init__(self, keys: List[str], cooldown_seconds: float = 60.0):
        """
        Args:
            keys: List of API key strings.
            cooldown_seconds: How long to avoid a rate-limited key (default 60s).
        """
        if not keys:
            raise ValueError("At least one API key is required")

        # Deduplicate while preserving order
        seen = set()
        self._keys: List[str] = []
        for k in keys:
            k = k.strip()
            if k and k not in seen:
                seen.add(k)
                self._keys.append(k)

        if not self._keys:
            raise ValueError("No valid API keys provided (all empty or duplicates)")

        self._index = 0
        self._cooldowns: dict[str, float] = {}  # key -> earliest reuse time
        self._cooldown_seconds = cooldown_seconds
        self._lock = threading.Lock()
        self._total_rotations = 0

    @property
    def key_count(self) -> int:
        """Number of unique keys in the pool."""
        return len(self._keys)

    @property
    def total_rotations(self) -> int:
        """Total number of times we rotated to a different key."""
        return self._total_rotations

    def get_key(self) -> str:
        """Get the next available API key, skipping cooled-down ones.

        If all keys are cooling down, returns the one with the shortest
        remaining cooldown (the "least recently limited" key).
        """
        with self._lock:
            now = time.time()
            best_key = None
            best_cooldown_end = float("inf")

            # Try each key starting from current index
            for offset in range(len(self._keys)):
                idx = (self._index + offset) % len(self._keys)
                key = self._keys[idx]
                cooldown_end = self._cooldowns.get(key, 0)

                if now >= cooldown_end:
                    # Key is available
                    self._index = (idx + 1) % len(self._keys)
                    return key

                # Track key with shortest remaining cooldown as fallback
                if cooldown_end < best_cooldown_end:
                    best_cooldown_end = cooldown_end
                    best_key = key

            # All keys are cooling down — return the one closest to being ready
            return best_key if best_key else self._keys[0]

    def mark_rate_limited(self, key: str) -> Optional[str]:
        """Mark a key as rate-limited and return the next available key.

        Args:
            key: The API key that received a 429 error.

        Returns:
            The next available key (may be the same key if it's the only one),
            or None if rotation is not possible.
        """
        with self._lock:
            self._cooldowns[key] = time.time() + self._cooldown_seconds
            self._total_rotations += 1

            # Find next available key
            now = time.time()
            for offset in range(len(self._keys)):
                idx = (self._index + offset) % len(self._keys)
                candidate = self._keys[idx]
                if candidate != key and now >= self._cooldowns.get(candidate, 0):
                    self._index = (idx + 1) % len(self._keys)
                    masked = f"{candidate[:8]}...{candidate[-4:]}"
                    log_message(
                        f"API key rotated -> {masked} "
                        f"(key {idx + 1}/{len(self._keys)})",
                        always_print=True,
                    )
                    return candidate

            # All other keys are also cooling down — return least-cooled one
            best_key = None
            best_end = float("inf")
            for k in self._keys:
                if k == key:
                    continue
                end = self._cooldowns.get(k, 0)
                if end < best_end:
                    best_end = end
                    best_key = k

            if best_key:
                wait_time = max(0, best_end - now)
                if wait_time > 0:
                    log_message(
                        f"All keys cooling down. Waiting {wait_time:.1f}s for next key...",
                        always_print=True,
                    )
                    time.sleep(wait_time)
                return best_key

            # Only one key in the pool
            return key


# --- Module-level singleton ---
_global_rotator: Optional[ApiKeyRotator] = None
_global_lock = threading.Lock()


def load_api_keys(
    keys_file: str = "api_keys.txt",
    fallback_key: str = "",
    cooldown_seconds: float = 60.0,
) -> ApiKeyRotator:
    """Load API keys from a file and create/update the global rotator.

    Args:
        keys_file: Path to the text file with one API key per line.
        fallback_key: Single key to use if the file doesn't exist or is empty.
        cooldown_seconds: Cooldown period for rate-limited keys.

    Returns:
        The ApiKeyRotator instance.
    """
    global _global_rotator
    keys = []

    keys_path = Path(keys_file)
    if keys_path.exists():
        with open(keys_path, "r") as f:
            for line in f:
                key = line.strip()
                if key and not key.startswith("#"):
                    keys.append(key)
        if keys:
            log_message(
                f"Loaded {len(keys)} API keys from {keys_file}",
                always_print=True,
            )

    # Add fallback key if not already in the list
    if fallback_key and fallback_key.strip():
        fb = fallback_key.strip()
        if fb not in keys:
            keys.append(fb)

    if not keys:
        raise ValueError(
            f"No API keys found. Provide keys in {keys_file} or via the UI."
        )

    with _global_lock:
        _global_rotator = ApiKeyRotator(keys, cooldown_seconds)

    return _global_rotator


def get_rotator() -> Optional[ApiKeyRotator]:
    """Get the global API key rotator (None if not initialized)."""
    return _global_rotator
