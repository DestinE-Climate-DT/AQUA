import os
import pytest
from pathlib import Path
import tempfile
import subprocess
from aqua.core.util.checksum import compute_md5, generate_checksums, verify_checksums


@pytest.mark.aqua
class TestChecksumModule:
    def test_compute_md5(self, tmp_path):
        """Test MD5 checksum computation using tmp_path."""
        file = tmp_path / "test.txt"
        file.write_text("Test content")

        md5 = compute_md5(str(file))
        
        assert md5 is not None
        assert len(md5) == 32  # MD5 checksum should be 32 characters long

    def test_compute_md5_file_not_found(self):
        """Test compute_md5 for a non-existent file."""
        md5 = compute_md5("non_existent_file.txt")
        assert md5 is None

    def test_full_checksums(self, tmp_path, capsys):
        """Test checksum generation."""
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("Sample content 1")
        file2.write_text("Sample content 2")

        checksum_file = Path("checksums.md5")
        generate_checksums(tmp_path, ["."],str(checksum_file))

        assert checksum_file.exists()
        with checksum_file.open("r", encoding='utf8') as f:
            content = f.read()

        verify_checksums(tmp_path, ["."], str(checksum_file))
        captured = capsys.readouterr()
        assert "All files are verified successfully" in captured.out

        os.remove(checksum_file)



