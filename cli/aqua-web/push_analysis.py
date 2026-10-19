#!/usr/bin/env python3
"""Standalone entry point for publishing AQUA analysis figures."""

from aqua.core.console.web_push import main, push_analysis

__all__ = ("main", "push_analysis")

if __name__ == "__main__":
    raise SystemExit(main())
