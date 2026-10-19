#!/usr/bin/env python3
"""Standalone entry point for aqua-web content generation."""

from aqua.core.console.web_contents import (
    cli_main,
    convert_to_new_structure,
    deep_merge_dicts,
    has_valid_key,
    main,
    make_content,
    parse_arguments,
)

__all__ = (
    "cli_main",
    "convert_to_new_structure",
    "deep_merge_dicts",
    "has_valid_key",
    "main",
    "make_content",
    "parse_arguments",
)

if __name__ == "__main__":
    raise SystemExit(cli_main())
