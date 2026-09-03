"""module to get the cdsapi key from environment variable or .cdsapirc file"""

import os


def get_cdsapi_key(cdsapi_key: str = None) -> str:
    """Get the CDSAPI key from environment variable or .cdsapirc file.

    Args:
        cdsapi_key (str, optional): The CDSAPI key. Defaults to None.
    """

    if cdsapi_key is None:
        cdsapi_key = os.getenv("CDSAPI_KEY")
    if cdsapi_key is None and os.path.exists(os.path.expanduser("~/.cdsapirc")):
        with open(os.path.expanduser("~/.cdsapirc"), "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("key:"):
                    cdsapi_key = line.split(":")[1].strip()
                    break
    if cdsapi_key is None:
        raise ValueError("CDSAPI key not found. Please set the CDSAPI_KEY environment variable or create a ~/.cdsapirc file.")

    return cdsapi_key
