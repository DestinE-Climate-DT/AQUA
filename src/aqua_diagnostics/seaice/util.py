"""Utility for the sea ice plotting module"""

from collections import defaultdict

def defaultdict_to_dict(d):
    """Recursively converts a defaultdict to a normal dict."""
    if isinstance(d, defaultdict):
        return {k: defaultdict_to_dict(v) for k, v in d.items()}
    return d