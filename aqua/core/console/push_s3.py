#!/usr/bin/env python3

# This script uploads the contents of a directory or a single file to an S3 bucket.
# The AWS credentials can be stored in the ~/.aws/credentials file or in environment
# variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or passed as arguments.

# The script uses the boto3 library to interact with the S3 service.
# The script can be run as follows:
# python -m aqua.core.console.push_s3 <bucket_name> <source> [-d <destination>]
# [--aws_access_key_id <aws_access_key_id>] [--aws_secret_access_key <aws_secret_access_key>] [--endpoint_url <endpoint_url>]

import argparse
import os

import boto3
from botocore.client import Config

from aqua.core.logger import log_configure

logger = log_configure(log_level="INFO", log_name="push_s3")


class S3TransferError(Exception):
    """Transfer failure carrying the existing command's exit code."""

    def __init__(self, message, exit_code):
        super().__init__(message)
        self.exit_code = exit_code


def _transfer_error(error):
    """Map an S3 failure to the existing command's exit codes."""
    message = str(error)
    if "AccessDenied" in message:
        return S3TransferError(message, 1)
    if "NoSuchBucket" in message:
        return S3TransferError(message, 2)
    return S3TransferError(message, 3)


def create_client(aws_access_key_id=None, aws_secret_access_key=None, endpoint_url="https://lumidata.eu"):
    """Create the same S3 client used by the standalone command."""
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version="s3"),
    )


def get_file_from_s3(client, bucket_name, object_name, dest_path):
    """Try to download a single file from an S3 bucket."""
    try:
        client.download_file(bucket_name, object_name, dest_path)
        logger.debug("Downloaded %s from bucket %s to %s", object_name, bucket_name, dest_path)
        return True
    except Exception as e:
        if "NoSuchKey" in str(e):
            # A missing index is expected for the first published experiment.
            logger.warning("Object %s does not exist in bucket %s", object_name, bucket_name)
            return False
        logger.error("Error downloading %s from bucket %s: %s", object_name, bucket_name, e)
        raise _transfer_error(e) from e


def upload_file_to_s3(client, bucket_name, file_path, object_name):
    """Upload a single file to an S3 bucket."""
    try:
        client.upload_file(file_path, bucket_name, object_name)
        logger.debug("Uploaded %s to bucket %s as %s", file_path, bucket_name, object_name)
    except Exception as e:
        logger.error("Error uploading %s to bucket %s: %s", file_path, bucket_name, e)
        raise _transfer_error(e) from e


def upload_directory_to_s3(client, bucket_name, source, dest):
    """Upload the contents of a directory to an S3 bucket."""

    for root, dirs, files in os.walk(source):
        for file in files:
            file_path = os.path.join(root, file)
            if dest:
                dest_path = os.path.relpath(file_path, source)
                dest_path = os.path.join(dest, dest_path)
            else:
                dest_path = file_path
            upload_file_to_s3(client, bucket_name, file_path, dest_path)


def main(argv=None):
    """Parse command-line arguments and upload a directory or single file to an S3 bucket.
    Store the AWS credentials in the ~/.aws/credentials file or in environment
    variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or pass them as arguments.
    """

    parser = argparse.ArgumentParser(description="Upload the contents of a directory or a single file to a S3 bucket.")
    parser.add_argument("bucket_name", help="The name of the S3 bucket.")
    parser.add_argument("source", help="The path to the directory or file to upload.")
    parser.add_argument("-d", "--destination", help="Optional destination path.")
    parser.add_argument("-k", "--aws_access_key_id", help="AWS access key ID.")
    parser.add_argument("-s", "--aws_secret_access_key", help="AWS secret access key.")
    parser.add_argument("--endpoint_url", help="Custom endpoint URL for S3.", default="https://lumidata.eu")
    parser.add_argument(
        "-g",
        "--get",
        action="store_true",
        help=(
            "Flag to download a single file from the S3 bucket instead of uploading. "
            "Destination is intended as a path on the remote."
        ),
    )

    args = parser.parse_args(argv)

    s3 = create_client(args.aws_access_key_id, args.aws_secret_access_key, args.endpoint_url)

    if args.get:  # Download a single file from the S3 bucket
        if args.destination:
            dest_path = args.destination
        else:
            dest_path = os.path.basename(args.source)
        get_file_from_s3(s3, args.bucket_name, dest_path, args.source)  # source and destination are flipped in meaning
    else:
        if os.path.isdir(args.source):
            upload_directory_to_s3(s3, args.bucket_name, args.source, args.destination)
        else:
            if args.destination:
                dest_path = args.destination
            else:
                dest_path = os.path.basename(args.source)

            upload_file_to_s3(s3, args.bucket_name, args.source, dest_path)


def cli_main(argv=None):
    """Run the standalone S3 command and return its exit status."""
    try:
        main(argv)
    except S3TransferError as error:
        return error.exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
