# SPDX-FileCopyrightText: 2018, 2019 Esteban J. G. Gabancho.
# SPDX-FileCopyrightText: 2024 KTH Royal Institute of Technology.
# SPDX-License-Identifier: MIT
"""Module tests."""

from unittest.mock import patch


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


def test_external_init_s3fs_info(appctx):
    """Test S3FS configuration for the external endpoint."""
    extension = appctx.extensions["invenio-s3"]
    with (
        patch.dict(
            appctx.config,
            {"S3_EXTERNAL_ENDPOINT_URL": "https://external.example.com"},
        ),
        patch.dict(extension.__dict__),
    ):
        extension.__dict__.pop("init_s3fs_info", None)
        extension.__dict__.pop("external_init_s3fs_info", None)

        s3_connection_info = extension.external_init_s3fs_info

        assert (
            s3_connection_info["client_kwargs"]["endpoint_url"]
            == "https://external.example.com"
        )


def test_external_init_s3fs_info_falls_back_to_internal_endpoint(appctx):
    """Test S3FS configuration falls back to the internal endpoint."""
    extension = appctx.extensions["invenio-s3"]
    with patch.dict(appctx.config), patch.dict(extension.__dict__):
        appctx.config.pop("S3_EXTERNAL_ENDPOINT_URL", None)
        extension.__dict__.pop("init_s3fs_info", None)
        extension.__dict__.pop("external_init_s3fs_info", None)

        assert extension.external_init_s3fs_info == extension.init_s3fs_info
