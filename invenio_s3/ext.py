# -*- coding: utf-8 -*-
#
# Copyright (C) 2018, 2019 Esteban J. G. Gabancho.
# Copyright (C) 2024 KTH Royal Institute of Technology.
#
# Invenio-S3 is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""S3 file storage support for Invenio."""

from flask import current_app
from werkzeug.utils import cached_property

from . import config


class InvenioS3(object):
    """Invenio-S3 extension."""

    def __init__(self, app=None):
        """Extension initialization."""
        if app:
            self.init_app(app)

    @cached_property
    def init_s3fs_info(self):
        """Gather all the information needed to start the S3FSFileSystem."""
        s3_config_extra = current_app.config.get("S3_CONFIG_EXTRA", {})
        info = dict(
            key=current_app.config.get("S3_ACCESS_KEY_ID", ""),
            secret=current_app.config.get("S3_SECRET_ACCESS_KEY", ""),
            client_kwargs={},
            config_kwargs={
                "s3": {
                    "addressing_style": "path",
                },
                "signature_version": current_app.config.get(
                    "S3_SIGNATURE_VERSION", "s3v4"
                ),
                **s3_config_extra,
            },
        )

        s3_endpoint = current_app.config.get("S3_ENDPOINT_URL", None)
        if s3_endpoint:
            info["client_kwargs"]["endpoint_url"] = s3_endpoint

        region_name = current_app.config.get("S3_REGION_NAME", None)
        if region_name:
            info["client_kwargs"]["region_name"] = region_name

        return info

    def init_app(self, app):
        """Flask application initialization."""
        self.init_config(app)
        app.extensions["invenio-s3"] = self

    def init_config(self, app):
        """Initialize configuration."""
        for k in dir(config):
            if k.startswith("S3_"):
                app.config.setdefault(k, getattr(config, k))
