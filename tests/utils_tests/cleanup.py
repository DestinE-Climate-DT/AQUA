"""
Test cleanup utilities for AQUA test suite.

This module provides utilities for cleaning up files created during tests,
preventing race conditions in parallel test execution.
"""
import os
from aqua.core.logger import log_configure

# Files created by tests that need cleanup. Paths are relative to configdir (e.g. ~/.aqua/)
FILES_TO_CLEAN = [
    # Grid files
    'grids/regular.yaml',
    'grids/nemo-curvilinear.yaml',
    'grids/ifs-unstructured.yaml'
    # Add other files here
    ]

class TestCleanupRegistry:
    """
    Registry for tracking and cleaning up files created by tests.
    
    This class provides a simple way to clean up test artifacts
    that are created in the AQUA configuration directory (~/.aqua).
    """
        
    def __init__(self, configdir: str, loglevel: str = 'WARNING'):
        """
        Initialize the cleanup registry.
        
        Args:
            configdir: Path to the AQUA configuration directory (~/.aqua)
            loglevel: Logging level for the cleanup operations. Defaults to 'WARNING'.
        """
        self.configdir = configdir
        self.logger = log_configure(log_level=loglevel, log_name='TestCleanupRegistry')
        
    def cleanup(self):
        """
        Remove test-generated files after all tests complete.
        """
        FILELOCK_TO_CLEAN = [s+'.lock' for s in FILES_TO_CLEAN]

        for filename in FILES_TO_CLEAN + FILELOCK_TO_CLEAN:
            file_path = os.path.join(self.configdir, filename)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError as e:
                    self.logger.warning('Could not remove %s: %s', file_path, e)
            else:
                self.logger.debug('File %s does not exist', file_path)
