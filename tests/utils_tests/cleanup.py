"""
Test cleanup utilities for AQUA test suite.

This module provides utilities for cleaning up files created during tests,
preventing race conditions in parallel test execution.
"""
import os
import glob
from aqua.core.configurer import ConfigPath
from aqua.core.logger import log_configure
from conftest import LOGLEVEL

loglevel = LOGLEVEL

class TestCleanupRegistry:
    """
    Registry for tracking and cleaning up files created by tests.
    
    This class provides an extensible way to track and clean up test artifacts
    that are created in the AQUA configuration directory (~/.aqua).
    
    also_remove_new_yaml_files (bool): If True, remove any new .yaml/.yml files 
        created during tests in addition to known_test_files. 
        This catches unexpected files that might be created by tests.
    
    To add new cleanup categories:
        1. Add a new entry to CLEANUP_FILES below.
        2. Specify the subdirectory 'subdir' and which files to clean 'known_test_files'.
    """
    
    # Cleanup configuration dictionary for files created by tests that need cleanup
    CLEANUP_FILES = {
        'grids': {
            # Directory within ~/.aqua/ where test files are created
            'subdir': 'grids',
            
            # Specific files created by tests that should be removed
            'known_test_files': [
                'regular.yaml',              # from test_grid_regular
                'nemo-curvilinear.yaml',     # from test_grid_curvilinear
                'ifs-unstructured.yaml',     # from test_grid_unstructured
            ],
            'also_remove_new_yaml_files': True,
        },
        
        'fixes': {
            'subdir': 'fixes',
            'known_test_files': [],
            'also_remove_new_yaml_files': True,
        },
        
        # Example: Add more categories as needed
        # 'weights': {
        #     'subdir': 'weights',
        #     'known_test_files': ['test_weights_*.nc'],
        #     'also_remove_new_yaml_files': False,
        # },
    }
    
    def __init__(self, configdir: str):
        """
        Initialize the cleanup registry.
        
        Args:
            configdir: Path to the AQUA configuration directory (~/.aqua)
        """
        self.configdir = configdir
        # Store the initial state of each directory before tests run
        self.initial_files = {}
        self.logger = log_configure(log_level=loglevel, log_name='TestCleanupRegistry')
        
    def snapshot_initial_state(self):
        """
        Record the files that exist before tests, to identify new files created during tests execution.
        """
        for category, config in self.CLEANUP_FILES.items():
            if config.get('also_remove_new_yaml_files', False):
                subdir_path = os.path.join(self.configdir, config['subdir'])
                if os.path.exists(subdir_path):
                    # Store list of files that existed before tests
                    self.initial_files[category] = set(os.listdir(subdir_path))
                else:
                    self.initial_files[category] = set()
    
    def cleanup(self):
        """
        Remove test-generated files after all tests complete.
        
        For each cleanup category:
        1. Removes known test files (from 'known_test_files' list)
        2. Optionally removes any new .yaml/.yml files created during tests
           (if 'also_remove_new_yaml_files' is True)
        """
        for category, config in self.CLEANUP_FILES.items():
            subdir_path = os.path.join(self.configdir, config['subdir'])
            
            if not os.path.exists(subdir_path):
                continue
            
            # Step 1: Remove known test files
            known_files = config.get('known_test_files', [])
            for pattern in known_files:
                # Support both exact filenames and glob patterns
                if '*' in pattern or '?' in pattern:
                    for filepath in glob.glob(os.path.join(subdir_path, pattern)):
                        self._remove_file(filepath, category)
                else:
                    filepath = os.path.join(subdir_path, pattern)
                    self._remove_file(filepath, category)
            
            # Step 2: Remove new .yaml/.yml files created during tests (if enabled)
            if config.get('also_remove_new_yaml_files', False):
                initial = self.initial_files.get(category, set())
                current_files = set(os.listdir(subdir_path))
                new_files = current_files - initial
                
                for filename in new_files:
                    # Only remove YAML files (not other file types)
                    if filename.endswith(('.yaml', '.yml')):
                        filepath = os.path.join(subdir_path, filename)
                        self._remove_file(filepath, category)
    
    def _remove_file(self, filepath: str, category: str):
        """
        Safely remove a file or symlink.
        
        Errors are logged but don't cause test failures.
        
        Args:
            filepath: Full path to the file to remove
            category: Category name (for logging purposes)
        """
        if not os.path.exists(filepath):
            return
        
        try:
            if os.path.isfile(filepath) or os.path.islink(filepath):
                if os.path.islink(filepath):
                    os.unlink(filepath)  # Remove symlink
                else:
                    os.remove(filepath)  # Remove regular file
        except OSError as e:
            self.logger.warning('Could not remove %s: %s', filepath, e)