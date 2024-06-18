import os
import shutil
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: post_install.py <path-to-config-file>")
        sys.exit(1)

    config_file = sys.argv[1]
    if not os.path.exists(config_file):
        print(f"The configuration file {config_file} does not exist.")
        sys.exit(1)

    config_dest = os.path.join(os.path.dirname(__file__), 'tropical_rainfall', 'config', 'current_config.yml')
    os.makedirs(os.path.dirname(config_dest), exist_ok=True)
    shutil.copy(config_file, config_dest)
    print(f"Configuration file {config_file} copied to {config_dest}")

if __name__ == "__main__":
    main()
