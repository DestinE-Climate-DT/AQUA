import os
import time
import threading
from filelock import SoftFileLock, Timeout

from aqua.core.logger import log_configure

class SafeFileLock:
    """
    A resilient soft lock with:
      - Timeout on acquire
      - Heartbeat updates (refreshes lock timestamp periodically)
      - Stale lock cleanup
    """

    def __init__(self, lock_path, timeout=60, stale_timeout=120,
                 heartbeat_interval=10, loglevel: str = 'WARNING'):
        """
        :param lock_path: Path to the .lock file (must be on shared filesystem)
        :param timeout: Seconds to wait to acquire lock before raising Timeout
        :param stale_timeout: Lock older than this (in seconds) is considered stale
        :param heartbeat_interval: How often to refresh lock file's mtime (seconds)
        :param loglevel: Logging level for the lock (DEBUG, INFO, WARNING, ERROR)
        """
        self.lock_path = lock_path
        self.timeout = timeout
        self.stale_timeout = stale_timeout
        self.heartbeat_interval = heartbeat_interval

        self.logger = log_configure(log_level=loglevel, log_name='FileLock')

        self.lock = SoftFileLock(lock_path, timeout=timeout)
        self._stop_event = threading.Event()
        self._heartbeat_thread = None

    def _is_stale(self):
        """Return True if lock file is older than stale_timeout."""
        if not os.path.exists(self.lock_path):
            return False
        age = time.time() - os.path.getmtime(self.lock_path)
        return age > self.stale_timeout

    def _remove_stale(self):
        """Remove stale lock file if too old."""
        if self._is_stale():
            try:
                os.remove(self.lock_path)
                self.logger.debug("Removed stale lock: %s", self.lock_path)
            except OSError:
                pass

    def _write_metadata(self):
        """Write PID + timestamp inside the lock file."""
        try:
            with open(self.lock_path, "w") as f:
                f.write(f"pid={os.getpid()} time={time.time()}\n")
        except Exception:
            pass  # not fatal

    def _heartbeat(self):
        """Refresh mtime periodically to indicate lock holder is alive."""
        while not self._stop_event.is_set():
            if os.path.exists(self.lock_path):
                try:
                    now = time.time()
                    os.utime(self.lock_path, (now, now))
                except Exception:
                    pass
            time.sleep(self.heartbeat_interval)

    def acquire(self):
        """Acquire the lock with timeout, stale cleanup, and start heartbeat."""
        self._remove_stale()

        try:
            self.lock.acquire(timeout=self.timeout)
            self._write_metadata()
            self.logger.debug("Acquired: %s", self.lock_path)
        except Timeout:
            raise Timeout(f"Timeout waiting for lock: {self.lock_path}")

        # Start heartbeat thread
        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat, daemon=True
        )
        self._heartbeat_thread.start()

    def release(self):
        """Stop heartbeat and release the lock."""
        self._stop_event.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2)

        try:
            self.lock.release()
            self.logger.debug("Released: %s", self.lock_path)
        except Exception:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()