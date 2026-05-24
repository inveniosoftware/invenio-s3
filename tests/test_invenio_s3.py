# SPDX-FileCopyrightText: 2018, 2019 Esteban J. G. Gabancho.
# SPDX-FileCopyrightText: 2024 KTH Royal Institute of Technology.
# SPDX-License-Identifier: MIT
"""Module tests."""


def test_version():
    """Test version import."""
    from invenio_s3 import __version__

    assert __version__


def test_init(appctx):
    """Test extension initialization."""
    assert "invenio-s3" in appctx.extensions

    appctx.config["S3_ENDPOINT_URL"] = "https://example.com:1234"
    appctx.config["S3_REGION_NAME"] = "eu-west-1"
    s3_connection_info = appctx.extensions["invenio-s3"].init_s3fs_info
    assert (
        s3_connection_info["client_kwargs"]["endpoint_url"]
        == "https://example.com:1234"
    )
    assert s3_connection_info["client_kwargs"]["region_name"] == "eu-west-1"
