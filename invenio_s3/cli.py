# SPDX-FileCopyrightText: 2026 California Institute of Technology.
# SPDX-License-Identifier: MIT
"""S3 storage CLI commands."""

import click
import s3fs
from flask import current_app
from flask.cli import with_appcontext


@click.group()
def s3():
    """S3 storage commands."""


@s3.command("create-bucket")
@click.argument("bucket")
@with_appcontext
def create_bucket(bucket):
    """Create an S3 bucket if it does not exist yet.

    The connection is configured the same way as the S3 file storage, i.e.
    from the ``S3_*`` configuration variables. The CORS rules from
    ``S3_BUCKET_CORS_RULES`` are applied to the bucket, also when it already
    exists, so the command can be rerun if setting them failed.
    """
    info = current_app.extensions["invenio-s3"].init_s3fs_info
    fs = s3fs.S3FileSystem(**info)

    try:
        fs.mkdir(bucket)
    except FileExistsError:
        click.secho(f"Bucket {bucket} already exists.", fg="yellow")
    except (OSError, ValueError) as error:
        raise click.ClickException(f"Could not create bucket {bucket}: {error}")
    else:
        click.secho(f"Bucket {bucket} created.", fg="green")

    cors_rules = current_app.config.get("S3_BUCKET_CORS_RULES")
    if not cors_rules:
        return

    try:
        fs.call_s3(
            "put_bucket_cors",
            Bucket=bucket,
            CORSConfiguration={"CORSRules": cors_rules},
        )
    except (OSError, ValueError) as error:
        raise click.ClickException(
            f"Could not set CORS configuration on bucket {bucket}: {error}"
        )

    click.secho(f"CORS configuration set on bucket {bucket}.", fg="green")
