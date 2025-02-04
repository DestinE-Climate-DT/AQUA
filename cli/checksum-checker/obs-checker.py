#!/usr/bin/env python3
"""Simple command line tool to execute a md5 checksum on grid files"""

import os
import argparse
from aqua.util import ConfigPath, load_yaml
from aqua.util.checksum import generate_checksums, verify_checksums

# default grids to be scanned by the grids-checker tool
OBS_FOLDERS = ['CERES', 'EN4', 'ERA5', 'ESA-CCI-L4',
               'OSI-SA', 'PHC3', 'PSC', 'WOA18']

def main():
    parser = argparse.ArgumentParser(description="MD5 checksum utility for folders.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    config = ConfigPath()
    obs_file = config.base_available['obs']['reader']['machine']
    mainpath = load_yaml(obs_file)[config.machine]['intake']['DATA_PATH']
    output_path = os.path.join(config.configdir, "datachecker", "obs-datacheck.md5")

    # Subcommand for generating checksums
    parser_generate = subparsers.add_parser("generate", help="Generate a checksum file.")
    parser_generate.add_argument("-o", "--output", default=output_path, help="Output checksum file.")

    # Subcommand for verifying checksums
    parser_verify = subparsers.add_parser("verify", help="Verify files using a checksum file.")
    parser_verify.add_argument("-c", "--checksum", default=output_path, help="Checksum file to verify against.")
    parser_verify.add_argument("-s", "--subdir", default=OBS_FOLDERS, help="Observations subfolder to verify against.")
   
    args = parser.parse_args()


    if args.command == "generate":
        generate_checksums(folder=mainpath, grids=OBS_FOLDERS, output_file=args.output)
    elif args.command == "verify":
        verify_checksums(folder=mainpath, grids=args.subdir, checksum_file=args.checksum)

if __name__ == "__main__":
    main()
