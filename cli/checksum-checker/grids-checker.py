#!/usr/bin/env python3
"""Simple command line tool to execute a md5 checksum on grid files"""

import os
import argparse
from aqua.util import ConfigPath, load_yaml
from aqua.util.checksum import generate_checksums, verify_checksums

# default grids to be scanned by the grids-checker tool
GRIDS_FOLDERS = ['EN4', 'ERA5', 'FESOM', 'HealPix', 'ICON', 'IFS', 'lonlat',
                 'NEMO', 'OSI-SAF', 'PSC', 'WOA18']



def main():
    parser = argparse.ArgumentParser(description="MD5 checksum utility for folders.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    config = ConfigPath()
    try: 
        # case when you have a catalog installed with grids defined for the machine
        mainpath = load_yaml(config.machine_file)[config.machine]['paths']['grids']
    except (KeyError, ValueError):
        # try to get it from climatedt catalog
        climatedt_file = config.base_available['climatedt-phase1']['reader']['machine']
        mainpath = load_yaml(climatedt_file)[config.machine]['paths']['grids']
    
    output_path = os.path.join(config.configdir, "datachecker", "grids-datacheck.md5")

    # Subcommand for generating checksums
    parser_generate = subparsers.add_parser("generate", help="Generate a checksum file.")
    parser_generate.add_argument("-o", "--output", default=output_path, help="Output checksum file.")

    # Subcommand for verifying checksums
    parser_verify = subparsers.add_parser("verify", help="Verify files using a checksum file.")
    parser_verify.add_argument("-c", "--checksum", default=output_path, help="Checksum file to verify against.")
    parser_verify.add_argument("-s", "--subdir", default=GRIDS_FOLDERS, help="Grids subfolder where to verify checksums.")
   
    args = parser.parse_args()


    if args.command == "generate":
        generate_checksums(folder=mainpath, grids=GRIDS_FOLDERS, output_file=args.output)
    elif args.command == "verify":
        verify_checksums(folder=mainpath, grids=args.subdir, checksum_file=args.checksum)

if __name__ == "__main__":
    main()
