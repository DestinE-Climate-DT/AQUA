#!/usr/bin/env python3
"""Transfer files to and from the LUMI-O S3 endpoint.

``python push_s3.py aqua-web figures.png -d content/png/figures.png``
remains available; the functions below can also be called by other tools.
"""

import argparse
import logging
from pathlib import Path

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError, PartialCredentialsError

logger = logging.getLogger(__name__)


class S3TransferError(Exception):
    """An S3 transfer failure with the CLI-compatible exit code."""

    def __init__(self, message, exit_code=3):
        super().__init__(message)
        self.exit_code = exit_code


def create_client(access_key=None, secret_key=None, endpoint_url="https://lumidata.eu"):
    """Create a client using explicit credentials or the standard AWS credential chain."""
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version="s3"),
    )


def _transfer_error(exc):
    """Translate boto3 failures into the transfer command's exit codes."""
    if isinstance(exc, (NoCredentialsError, PartialCredentialsError)):
        return S3TransferError(str(exc), 1)
    code = exc.response.get("Error", {}).get("Code", "") if isinstance(exc, ClientError) else ""
    if not code and isinstance(exc, S3UploadFailedError):
        code = next((item for item in ("AccessDenied", "NoSuchBucket") if item in str(exc)), "")
    if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
        return S3TransferError(str(exc), 1)
    if code == "NoSuchBucket":
        return S3TransferError(str(exc), 2)
    return S3TransferError(str(exc))


def get_file_from_s3(client, bucket_name, object_name, dest_path):
    """Download an object; return False if the object does not exist."""
    try:
        client.download_file(bucket_name, object_name, str(dest_path))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            logger.warning("Object %s does not exist in bucket %s", object_name, bucket_name)
            return False
        raise _transfer_error(exc) from exc
    except BotoCoreError as exc:
        raise _transfer_error(exc) from exc
    logger.info("Downloaded %s from %s to %s", object_name, bucket_name, dest_path)
    return True


def upload_file_to_s3(client, bucket_name, file_path, object_name):
    """Upload a single file, raising S3TransferError on failure."""
    try:
        client.upload_file(str(file_path), bucket_name, object_name)
    except (BotoCoreError, ClientError, S3UploadFailedError) as exc:
        raise _transfer_error(exc) from exc
    logger.info("Uploaded %s to %s/%s", file_path, bucket_name, object_name)


def upload_directory_to_s3(client, bucket_name, source, dest=None):
    """Upload all files in a directory under the requested S3 prefix."""
    source = Path(source)
    for file_path in source.rglob("*"):
        if file_path.is_file():
            key = (Path(dest) / file_path.relative_to(source)).as_posix() if dest else file_path.as_posix()
            upload_file_to_s3(client, bucket_name, file_path, key)


def transfer(client, bucket_name, source, destination=None, get=False):
    """Download one object or upload a file or directory.

    In download mode, ``source`` is the local path and ``destination`` is
    the object key, matching the command-line interface.
    """
    if get:
        return get_file_from_s3(client, bucket_name, destination or Path(source).name, source)
    if Path(source).is_dir():
        upload_directory_to_s3(client, bucket_name, source, destination)
    else:
        upload_file_to_s3(client, bucket_name, source, destination or Path(source).name)
    return True


def main(argv=None):
    """Run the standalone S3 command and return an exit code."""
    parser = argparse.ArgumentParser(description="Upload a file or directory to an S3 bucket.")
    parser.add_argument("bucket_name", help="The name of the S3 bucket.")
    parser.add_argument("source", help="The path to the directory or file to upload.")
    parser.add_argument("-d", "--destination", help="Remote object key or prefix; in get mode, the remote key.")
    parser.add_argument("-k", "--aws_access_key_id", help="AWS access key ID.")
    parser.add_argument("-s", "--aws_secret_access_key", help="AWS secret access key.")
    parser.add_argument("--endpoint_url", default="https://lumidata.eu", help="Custom S3 endpoint URL.")
    parser.add_argument("-g", "--get", action="store_true", help="Download one remote object to source.")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    try:
        client = create_client(args.aws_access_key_id, args.aws_secret_access_key, args.endpoint_url)
        transfer(client, args.bucket_name, args.source, args.destination, args.get)
    except S3TransferError as exc:
        logger.error("S3 transfer failed: %s", exc)
        return exc.exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
