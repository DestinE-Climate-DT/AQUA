#!/usr/bin/env python3
"""Standalone entry point for aqua-web S3 transfers."""

from aqua.core.console.web_s3 import (
    S3TransferError,
    cli_main,
    create_client,
    get_file_from_s3,
    main,
    upload_directory_to_s3,
    upload_file_to_s3,
)

__all__ = (
    "S3TransferError",
    "cli_main",
    "create_client",
    "get_file_from_s3",
    "main",
    "upload_directory_to_s3",
    "upload_file_to_s3",
)

if __name__ == "__main__":
    raise SystemExit(cli_main())
