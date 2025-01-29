
"""checksum verification module"""

import os
import sys
from datetime import datetime
import hashlib
from aqua.util import to_list


def compute_md5(file_path):
    """Compute the MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        return None

def generate_checksums(folder, grids, output_file):
    """
    Generate MD5 checksums for all files in a folder.
    Will scan the main folder and the subfolder list and will store the data
    in a output_file
    """

    print(f"Generating datachecker to {output_file}...")
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(output_file, "w", encoding='utf8') as f:
        f.write("# MD5 checksum file for AQUA\n")
        f.write(f"# Folder: {folder}\n")
        f.write(f"# Generated by AQUA checksum checker on {current_date} \n\n")

        for grid_path in sorted(grids):
            subdir_path = os.path.join(folder, grid_path)
            if os.path.isdir(subdir_path):
                for root, _, files in os.walk(subdir_path):
                    for file in sorted(files):
                        file_path = os.path.join(root, file)
                        print(f"Computing checksum for {file_path}" )
                        md5_checksum = compute_md5(file_path)
                        if md5_checksum:
                            relative_path = os.path.relpath(file_path, folder)
                            f.write(f"{md5_checksum} {relative_path}\n")
    print(f"Checksum file created at {output_file}")

def verify_checksums(folder, grids, checksum_file):
    """Verify files against MD5 checksums in the checksum file."""

    if isinstance(grids, str):
        if not os.path.exists(os.path.join(folder, grids)):
            raise FileNotFoundError(f'No {grids} directory found in {folder}!')

    try:
        with open(checksum_file, "r", encoding='utf8') as f:
            print(f"Starting verification against {checksum_file}. It will take a while...")
            all_good = True
            for line in f:
                if line.startswith("#") or not line.strip():
                    continue
                md5_checksum, relative_path = line.strip().split(" ", 1)
                relative_dir = os.path.dirname(relative_path)
                if relative_dir in to_list(grids):
                    file_path = os.path.join(folder, relative_path)
                    print(file_path)
                    if not os.path.exists(file_path):
                        print(f"Missing file: {relative_path}!!")
                        all_good = False
                    else:
                        computed_md5 = compute_md5(file_path)
                        if computed_md5 != md5_checksum:
                            print(f"Checksum mismatch for {relative_path}")
                            all_good = False
            if all_good:
                print("All files are verified successfully.")
            else:
                sys.exit("Verification failed.")
    except FileNotFoundError:
        sys.exit(f"Checksum file {checksum_file} not found.")