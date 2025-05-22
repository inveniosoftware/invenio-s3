# -*- coding: utf-8 -*-
#
# Copyright (C) 2018, 2019 Esteban J. G. Gabancho.
# Copyright (C) 2024 KTH Royal Institute of Technology.
#
# Invenio-S3 is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""Pytest configuration."""

import hashlib
import os

import pytest
import s3fs as _s3fs
from invenio_app.factory import create_api

from invenio_s3 import S3FSFileStorage


@pytest.fixture(scope="module")
def app_config(app_config):
    """Customize application configuration."""
    app_config["THEME_FRONTPAGE"] = False
    app_config["FILES_REST_STORAGE_FACTORY"] = "invenio_s3.s3fs_storage_factory"
    app_config["S3_ENDPOINT_URL"] = os.environ["S3_ENDPOINT_URL"]
    app_config["S3_ACCESS_KEY_ID"] = os.environ["S3_ACCESS_KEY_ID"]
    app_config["S3_SECRET_ACCESS_KEY"] = os.environ["S3_SECRET_ACCESS_KEY"]
    return app_config


@pytest.fixture(scope="module")
def create_app():
    """Application factory fixture."""
    return create_api


@pytest.fixture(scope="module")
def s3fs(app_config):
    """S3 client."""
    return _s3fs.S3FileSystem(
        endpoint_url=app_config["S3_ENDPOINT_URL"],
        key=app_config["S3_ACCESS_KEY_ID"],
        secret=app_config["S3_SECRET_ACCESS_KEY"],
    )


@pytest.fixture()
def s3_bucket():
    """S3 test path."""
    return "s3://default"


@pytest.fixture()
def s3_path():
    """S3 test path."""
    return "s3://default/file.txt"


@pytest.fixture(scope="function")
def s3_storage(appctx, s3_path, s3fs):
    """Instance of S3FSFileStorage."""
    s3_storage = S3FSFileStorage(s3_path)
    yield s3_storage
    try:
        s3fs.rm(s3_path, recursive=True)
    except FileNotFoundError:
        # Some times we delete the file in the tests
        pass


@pytest.fixture
def file_instance_mock(appctx, s3_path):
    """Mock of a file instance."""

    class FileInstance(object):
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    return FileInstance(
        id="deadbeef-65bd-4d9b-93e2-ec88cc59aec5",
        uri=s3_path,
        size=4,
        updated=None,
    )


@pytest.fixture()
def get_md5():
    """Get MD5 of data."""

    def inner(data, prefix=True):
        m = hashlib.md5()
        m.update(data)
        return "md5:{0}".format(m.hexdigest()) if prefix else m.hexdigest()

    return inner
