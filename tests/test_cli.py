# SPDX-FileCopyrightText: 2026 California Institute of Technology.
# SPDX-License-Identifier: MIT
"""CLI tests."""

import sys

if sys.version_info >= (3, 10):
    from importlib.metadata import entry_points
else:
    # This can be removed when we are no longet testing on Python 3.9
    from importlib_metadata import entry_points


def load_s3_cli():
    """Load the ``s3`` command group the way ``invenio`` does."""
    (entry_point,) = entry_points(group="flask.commands", name="s3")
    return entry_point.load()


def test_create_bucket(appctx, s3fs):
    """Test bucket creation through the registered ``s3`` command group."""
    runner = appctx.test_cli_runner()
    cli = load_s3_cli()
    bucket = "test-create-bucket"

    try:
        result = runner.invoke(cli, ["create-bucket", bucket])
        assert result.exit_code == 0, result.output
        s3fs.invalidate_cache()
        assert s3fs.exists(bucket)

        cors = s3fs.call_s3("get_bucket_cors", Bucket=bucket)
        assert cors["CORSRules"] == [
            {"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}
        ]

        # Running it again should not fail, and should reapply the CORS rules
        s3fs.call_s3("delete_bucket_cors", Bucket=bucket)
        result = runner.invoke(cli, ["create-bucket", bucket])
        assert result.exit_code == 0, result.output
        assert "already exists" in result.output
        cors = s3fs.call_s3("get_bucket_cors", Bucket=bucket)
        assert cors["CORSRules"] == [
            {"AllowedMethods": ["GET"], "AllowedOrigins": ["*"]}
        ]
    finally:
        if s3fs.exists(bucket):
            s3fs.rmdir(bucket)
