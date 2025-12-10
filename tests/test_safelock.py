"""Tests for SafeFileLock class with concurrent access"""

import os
import time
import pytest
import threading
from unittest.mock import patch
from filelock import Timeout

from aqua.core.lock import SafeFileLock


@pytest.fixture
def lock_file(tmp_path):
    """Fixture that provides a temporary lock file path."""
    lock_path = tmp_path / "test.lock"
    yield str(lock_path)
    # Cleanup
    if lock_path.exists():
        lock_path.unlink()


@pytest.fixture
def shared_file(tmp_path):
    """Fixture that provides a shared file for concurrent write tests."""
    file_path = tmp_path / "shared.txt"
    file_path.write_text("0\n")
    return str(file_path)


class TestSafeFileLock:
    """Test suite for SafeFileLock"""

    @pytest.mark.aqua
    def test_basic_acquire_release(self, lock_file):
        """Test basic lock acquisition and release."""
        lock = SafeFileLock(lock_file, timeout=5)
        
        # Lock should not exist initially
        assert not os.path.exists(lock_file)
        
        # Acquire lock
        lock.acquire()
        assert os.path.exists(lock_file)
        
        # Release lock
        lock.release()
        # Lock file might still exist after release (SoftFileLock behavior)

    @pytest.mark.aqua
    def test_context_manager(self, lock_file):
        """Test lock usage as context manager."""
        with SafeFileLock(lock_file, timeout=5):
            assert os.path.exists(lock_file)
        # After exiting context, heartbeat should be stopped

    @pytest.mark.aqua
    def test_timeout(self, lock_file):
        """Test that lock acquisition times out when held by another process."""
        lock1 = SafeFileLock(lock_file, timeout=2)
        lock2 = SafeFileLock(lock_file, timeout=2)
        
        lock1.acquire()
        
        # Second lock should timeout
        with pytest.raises(Timeout):
            lock2.acquire()
        
        lock1.release()

    @pytest.mark.aqua
    def test_stale_lock_cleanup(self, lock_file):
        """Test that stale locks are cleaned up."""
        # Create a stale lock file
        with open(lock_file, 'w') as f:
            f.write(f"pid=99999 time={time.time() - 200}\n")
        
        # Set mtime to old value
        old_time = time.time() - 200
        os.utime(lock_file, (old_time, old_time))
        
        # Try to acquire with stale_timeout of 120 seconds
        lock = SafeFileLock(lock_file, timeout=5, stale_timeout=120)
        lock.acquire()  # Should succeed by removing stale lock
        
        lock.release()

    @pytest.mark.aqua
    def test_heartbeat_updates_mtime(self, lock_file):
        """Test that heartbeat updates lock file mtime."""
        lock = SafeFileLock(lock_file, timeout=5, heartbeat_interval=1)
        
        lock.acquire()
        initial_mtime = os.path.getmtime(lock_file)
        
        # Wait for heartbeat to update
        time.sleep(2)
        
        updated_mtime = os.path.getmtime(lock_file)
        assert updated_mtime > initial_mtime
        
        lock.release()

    @pytest.mark.aqua
    def test_metadata_written(self, lock_file):
        """Test that lock file contains PID and timestamp."""
        lock = SafeFileLock(lock_file, timeout=5)
        
        lock.acquire()
        
        with open(lock_file, 'r') as f:
            content = f.read()
        
        assert f"pid={os.getpid()}" in content
        assert "time=" in content
        
        lock.release()

    @pytest.mark.aqua
    def test_sequential_access(self, lock_file, shared_file):
        """Test that multiple locks access file sequentially."""
        def increment_file(lock_path, file_path, iterations=5):
            """Increment counter in file under lock."""
            lock = SafeFileLock(lock_path, timeout=30)
            for _ in range(iterations):
                with lock:
                    with open(file_path, 'r') as f:
                        value = int(f.read().strip())
                    
                    # Simulate some work
                    time.sleep(0.05)
                    
                    with open(file_path, 'w') as f:
                        f.write(f"{value + 1}\n")
        
        # Run two threads
        threads = []
        for _ in range(2):
            t = threading.Thread(target=increment_file, args=(lock_file, shared_file, 5))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Final value should be 10 (2 threads * 5 increments)
        with open(shared_file, 'r') as f:
            final_value = int(f.read().strip())
        
        assert final_value == 10

    @pytest.mark.aqua
    def test_concurrent_write_protection(self, lock_file, shared_file):
        """Test that lock prevents concurrent writes from corrupting data."""
        def write_pattern(lock_path, file_path, pattern, count=10):
            """Write a pattern to file multiple times under lock."""
            lock = SafeFileLock(lock_path, timeout=60)
            for _ in range(count):
                with lock:
                    with open(file_path, 'a') as f:
                        f.write(f"{pattern}\n")
                    time.sleep(0.001)
        
        # Clear file
        with open(shared_file, 'w') as f:
            f.write("")
        
        # Run multiple threads with different patterns
        threads = []
        patterns = ['AAA', 'BBB', 'CCC']
        for pattern in patterns:
            t = threading.Thread(target=write_pattern, args=(lock_file, shared_file, pattern, 10))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # Verify all lines are complete (no interleaved writes)
        with open(shared_file, 'r') as f:
            lines = f.readlines()
        
        assert len(lines) == 30  # 3 threads * 10 writes each
        for line in lines:
            line = line.strip()
            assert line in patterns, f"Corrupted line: {line}"

    @pytest.mark.aqua
    def test_lock_reentrant_same_thread(self, lock_file):
        """Test that same thread cannot acquire lock twice (not reentrant)."""
        lock1 = SafeFileLock(lock_file, timeout=2)
        lock2 = SafeFileLock(lock_file, timeout=2)
        
        lock1.acquire()
        
        # Second acquisition in same thread should timeout
        with pytest.raises(Timeout):
            lock2.acquire()
        
        lock1.release()

    @pytest.mark.aqua
    def test_multiple_contexts(self, lock_file, shared_file):
        """Test multiple context manager entries/exits."""
        for i in range(5):
            with SafeFileLock(lock_file, timeout=5):
                with open(shared_file, 'w') as f:
                    f.write(f"{i}\n")
        
        with open(shared_file, 'r') as f:
            value = int(f.read().strip())
        
        assert value == 4

    @pytest.mark.aqua
    def test_exception_releases_lock(self, lock_file):
        """Test that lock is released even when exception occurs."""
        lock1 = SafeFileLock(lock_file, timeout=5)
        lock2 = SafeFileLock(lock_file, timeout=5)
        
        try:
            with lock1:
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Second lock should be able to acquire after exception
        lock2.acquire()
        lock2.release()

    @pytest.mark.aqua
    def test_different_lock_files(self, tmp_path):
        """Test that different lock files don't interfere."""
        lock1_path = tmp_path / "lock1.lock"
        lock2_path = tmp_path / "lock2.lock"
        
        lock1 = SafeFileLock(str(lock1_path), timeout=5)
        lock2 = SafeFileLock(str(lock2_path), timeout=5)
        
        # Should be able to hold both locks simultaneously
        lock1.acquire()
        lock2.acquire()
        
        assert os.path.exists(lock1_path)
        assert os.path.exists(lock2_path)
        
        lock1.release()
        lock2.release()


@pytest.mark.aqua
@pytest.mark.parametrize("worker_id", ["worker1", "worker2", "worker3"])
def test_concurrent_pytest_xdist(tmp_path_factory, worker_id):
    """
    Test concurrent access across pytest-xdist workers.
    
    This test is designed to run with pytest-xdist (pytest -n auto).
    Each worker will try to increment a shared counter file.
    
    Note: This test uses a longer timeout (120s) to handle high contention
    when multiple workers compete for the same lock.
    """
    # Use a shared temp directory that all workers can access
    shared_tmp = tmp_path_factory.getbasetemp().parent
    shared_file = shared_tmp / "xdist_counter.txt"
    lock_file = shared_tmp / "xdist_counter.lock"
    init_lock_file = shared_tmp / "xdist_init.lock"
    
    # Initialize file with lock to prevent race conditions
    with SafeFileLock(str(init_lock_file), timeout=60):
        if not shared_file.exists():
            try:
                with open(shared_file, 'w') as f:
                    f.write("0\n")
            except FileExistsError:
                pass  # Another worker created it
    
    # Each worker increments 5 times (reduced from 10 to reduce contention)
    # Use longer timeout (120s) to handle high contention with multiple workers
    for i in range(5):
        # Add small random delay to reduce contention spikes
        if i > 0:
            time.sleep(0.01 * (i % 3))
        
        with SafeFileLock(str(lock_file), timeout=120):
            with open(shared_file, 'r') as f:
                value = int(f.read().strip())
            
            time.sleep(0.001)  # Simulate some work
            
            with open(shared_file, 'w') as f:
                f.write(f"{value + 1}\n")
    
    # Note: We can't assert the final value here because we don't know
    # how many workers pytest-xdist will spawn. The test passes if no
    # exceptions are raised and the file isn't corrupted.


@pytest.mark.aqua
def test_logging_levels(lock_file):
    """Test that logging works at different levels."""
    lock = SafeFileLock(lock_file, timeout=5, loglevel='DEBUG')

    # Use patch to spy on the logger's debug method
    with patch.object(lock.logger, 'debug') as mock_debug:
        lock.acquire()
        lock.release()

    # Check that the logger's debug method was called with the expected messages
    assert mock_debug.call_count >= 2
    
    # Get all the calls made to the mock
    all_calls = [call.args[0] for call in mock_debug.call_args_list]
    
    assert any("Acquired" in call for call in all_calls)
    assert any("Released" in call for call in all_calls)


@pytest.mark.aqua
def test_long_running_lock(lock_file):
    """Test lock held for extended period with heartbeat."""
    lock = SafeFileLock(lock_file, timeout=5, heartbeat_interval=1)
    
    lock.acquire()
    initial_mtime = os.path.getmtime(lock_file)
    
    # Hold lock for several heartbeat intervals
    time.sleep(3)
    
    final_mtime = os.path.getmtime(lock_file)
    
    # Mtime should have been updated multiple times
    assert final_mtime > initial_mtime
    
    lock.release()


if __name__ == "__main__":
    # Run with: pytest tests/test_safelock.py -v
    # Run with xdist: pytest tests/test_safelock.py -v -n auto
    pytest.main([__file__, "-v"])
