"""Tests for the SafeFileLock class"""

import os
import threading
import time
from unittest.mock import call, patch

import pytest
from filelock import Timeout
from utils_tests import LOGLEVEL

from aqua.core.lock import SafeFileLock

pytestmark = pytest.mark.aqua

# Contenders that are expected to fail wait only this long (seconds)
SHORT_TIMEOUT = 0.2


def _read_bytes(path):
    """Content of a file as bytes."""
    with open(path, "rb") as file:
        return file.read()


def _make_lock_file(path, age):
    """Create an empty lock file whose mtime is `age` seconds in the past.

    An empty marker is what filelock before 3.22 writes: filelock never breaks it by itself,
    so only SafeFileLock's own stale check can remove it.
    """
    open(path, "w").close()
    past = time.time() - age
    os.utime(path, (past, past))


@pytest.fixture
def lock_file(tmp_path):
    """Path of a lock file in a temporary directory."""
    return os.path.join(tmp_path, "test.lock")


@pytest.mark.parametrize("use_context", [False, True])
def test_acquire_creates_and_release_removes_lock(lock_file, use_context):
    """The lock file exists only while the lock is held, also on a second acquisition: fails if release leaves it."""
    lock = SafeFileLock(lock_file, timeout=5)
    for _ in range(2):
        assert not os.path.exists(lock_file)
        if use_context:
            with lock:
                assert os.path.exists(lock_file)
        else:
            lock.acquire()
            assert os.path.exists(lock_file)
            lock.release()
        assert not os.path.exists(lock_file)


def test_held_lock_times_out(lock_file):
    """A second lock on the same file, also from the same thread, times out: fails if the lock is not exclusive."""
    with SafeFileLock(lock_file, timeout=5):
        with pytest.raises(Timeout, match="Timeout waiting for lock"):
            SafeFileLock(lock_file, timeout=SHORT_TIMEOUT).acquire()
    # the file is still free for the next holder
    with SafeFileLock(lock_file, timeout=SHORT_TIMEOUT):
        assert os.path.exists(lock_file)


def test_stale_lock_removed(lock_file):
    """A lock file older than stale_timeout is removed by SafeFileLock: fails if the stale check is skipped."""
    _make_lock_file(lock_file, age=200)
    lock = SafeFileLock(lock_file, timeout=SHORT_TIMEOUT, stale_timeout=120)
    with patch.object(lock.logger, "debug") as debug:
        with lock:
            assert os.path.exists(lock_file)
    assert call("Removed stale lock: %s", lock_file) in debug.call_args_list


def test_recent_lock_not_removed(lock_file):
    """A lock file younger than stale_timeout is respected: fails if every existing lock is treated as stale."""
    _make_lock_file(lock_file, age=60)
    with pytest.raises(Timeout, match="Timeout waiting for lock"):
        SafeFileLock(lock_file, timeout=SHORT_TIMEOUT, stale_timeout=120).acquire()
    assert os.path.exists(lock_file)


def test_heartbeat_refreshes_mtime(lock_file):
    """The heartbeat refreshes the mtime of the held lock file: fails if the heartbeat does not run."""
    lock = SafeFileLock(lock_file, timeout=5, heartbeat_interval=0.01)
    utime = os.utime
    refreshed = []
    two_refreshes = threading.Event()

    def utime_spy(path, *args, **kwargs):
        """Record the refreshed path and signal after two refreshes."""
        utime(path, *args, **kwargs)
        refreshed.append(path)
        if len(refreshed) >= 2:
            two_refreshes.set()

    with lock:
        # make the marker look old, the heartbeat must bring it back to now
        past = time.time() - 200
        utime(lock_file, (past, past))
        with patch("aqua.core.lock.safelock.os.utime", side_effect=utime_spy):
            assert two_refreshes.wait(timeout=5)
            assert time.time() - os.path.getmtime(lock_file) < 100
    assert set(refreshed) == {lock_file}


def test_marker_not_overwritten(lock_file):
    """Preserve filelock's marker, including older versions that leave it empty."""
    lock = SafeFileLock(lock_file, timeout=5)
    acquire = lock.lock.acquire
    markers = []

    def acquire_and_capture(*args, **kwargs):
        """Capture the marker before SafeFileLock resumes acquisition."""
        proxy = acquire(*args, **kwargs)
        markers.append(_read_bytes(lock_file))
        return proxy

    with patch.object(lock.lock, "acquire", side_effect=acquire_and_capture):
        with lock:
            assert _read_bytes(lock_file) == markers[0]


def test_held_lock_not_broken(lock_file):
    """A held lock is not broken by a contender (#3190): fails if the marker can be judged stale or malformed."""
    lock1 = SafeFileLock(lock_file, timeout=5, heartbeat_interval=60)
    # filelock 4.2 breaks a malformed marker it has seen unchanged for 2 s: the contender waits longer than that
    lock2 = SafeFileLock(lock_file, timeout=2.5)

    with lock1:
        with pytest.raises(Timeout, match="Timeout waiting for lock"), lock2:
            pass


def test_release_stops_heartbeat(lock_file):
    """Release stops an active heartbeat before its next scheduled refresh."""
    lock = SafeFileLock(lock_file, timeout=5, heartbeat_interval=60)
    refreshed = threading.Event()
    utime = os.utime

    def refresh_and_signal(*args, **kwargs):
        """Signal after the heartbeat starts refreshing its marker."""
        utime(*args, **kwargs)
        refreshed.set()

    with patch("aqua.core.lock.safelock.os.utime", side_effect=refresh_and_signal):
        with lock:
            assert refreshed.wait(timeout=5)
            heartbeat = lock._heartbeat_thread

        assert not heartbeat.is_alive()
        assert not os.path.exists(lock_file)


def test_threads_increment_sequentially(lock_file, tmp_path):
    """Read-modify-write under the lock from three threads loses no update: fails if the lock does not exclude."""
    counter = os.path.join(tmp_path, "counter.txt")
    with open(counter, "w") as f:
        f.write("0\n")

    def increment(iterations=5):
        """Increment the counter in the file under the lock."""
        lock = SafeFileLock(lock_file, timeout=30)
        for _ in range(iterations):
            with lock:
                with open(counter) as f:
                    value = int(f.read().strip())
                # widen the window between read and write, so that a missing lock loses updates
                time.sleep(0.002)
                with open(counter, "w") as f:
                    f.write(f"{value + 1}\n")

    threads = [threading.Thread(target=increment) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with open(counter) as f:
        assert int(f.read().strip()) == 15


def test_exception_releases_lock(lock_file):
    """An exception inside the context releases the lock: fails if __exit__ does not release."""
    with pytest.raises(ValueError, match="Test exception"):
        with SafeFileLock(lock_file, timeout=5):
            raise ValueError("Test exception")

    assert not os.path.exists(lock_file)
    with SafeFileLock(lock_file, timeout=SHORT_TIMEOUT):
        assert os.path.exists(lock_file)


def test_different_lock_files(tmp_path):
    """Locks on different files can be held at the same time: fails if one lock blocks the other."""
    paths = [os.path.join(tmp_path, f"lock{i}.lock") for i in (1, 2)]
    with SafeFileLock(paths[0], timeout=SHORT_TIMEOUT), SafeFileLock(paths[1], timeout=SHORT_TIMEOUT):
        assert all(os.path.exists(path) for path in paths)


def test_debug_log_names_lock(lock_file):
    """Acquire and release are logged at debug level with the lock path."""
    lock = SafeFileLock(lock_file, timeout=5, loglevel=LOGLEVEL)

    with patch.object(lock.logger, "debug") as debug:
        lock.acquire()
        lock.release()

    assert debug.call_args_list == [call("Acquired: %s", lock_file), call("Released: %s", lock_file)]
