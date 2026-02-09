# SPDX-FileCopyrightText: 2026 California Institute of Technology.
# SPDX-FileCopyrightText: 2018, 2019 Esteban J. G. Gabancho.
# SPDX-FileCopyrightText: 2024 Graz University of Technology.
# SPDX-License-Identifier: MIT

"""File serving helpers for S3 files."""

import mimetypes
import unicodedata
from time import time
from urllib.parse import quote

from flask import current_app
from invenio_files_rest.helpers import sanitize_mimetype
from werkzeug.datastructures import Headers


def redirect_stream(
    s3_url_builder,
    filename,
    mimetype=None,
    restricted=True,
    as_attachment=False,
    trusted=False,
):
    """Redirect to URL to serve the file directly from there.

    :param url: redirection URL

    :return: Flask response.
    """
    # Guess mimetype from filename if not provided.
    if mimetype is None and filename:
        mimetype = mimetypes.guess_type(filename)[0]
    if mimetype is None:
        mimetype = "application/octet-stream"

    # Construct headers
    headers = Headers()

    if not trusted:
        # Sanitize MIME type
        mimetype = sanitize_mimetype(mimetype, filename=filename)
        # See https://www.owasp.org/index.php/OWASP_Secure_Headers_Project
        # Prevent JavaScript execution
        headers["Content-Security-Policy"] = "default-src 'none';"
        # Prevent MIME type sniffing for browser.
        headers["X-Content-Type-Options"] = "nosniff"
        # Prevent opening of downloaded file by IE
        headers["X-Download-Options"] = "noopen"
        # Prevent cross domain requests from Flash/Acrobat.
        headers["X-Permitted-Cross-Domain-Policies"] = "none"
        # Prevent files from being embedded in frame, iframe and object tags.
        headers["X-Frame-Options"] = "deny"
        # Enable XSS protection (IE, Chrome, Safari)
        headers["X-XSS-Protection"] = "1; mode=block"

    # Force Content-Disposition for application/octet-stream to prevent
    # Content-Type sniffing.
    if as_attachment or mimetype == "application/octet-stream":
        # see https://github.com/pallets/werkzeug/blob/main/src/werkzeug/utils.py#L456-L465
        try:
            filename.encode("ascii")
        except UnicodeEncodeError:
            simple = unicodedata.normalize("NFKD", filename)
            simple = simple.encode("ascii", "ignore").decode("ascii")
            # safe = RFC 5987 attr-char
            quoted = quote(filename, safe="!#$&+-.^_`|~")
            filenames = {"filename": simple, "filename*": f"UTF-8''{quoted}"}
        else:
            filenames = {"filename": filename}
        headers.add("Content-Disposition", "attachment", **filenames)
    else:
        headers.add("Content-Disposition", "inline")

    url = s3_url_builder(
        ResponseContentType=mimetype,
        ResponseContentDisposition=headers.get("Content-Disposition"),
    )
    headers["Location"] = url

    # Construct response object.
    rv = current_app.response_class(
        url,
        status=302,
        headers=headers,
        mimetype=mimetype,
        direct_passthrough=True,
    )

    # Cache control: if the file is not restricted, we set caching to the
    # presigned url expiration time
    if not restricted:
        rv.cache_control.public = True
        cache_timeout = current_app.config["S3_URL_EXPIRATION"]
        if cache_timeout is not None:
            rv.cache_control.max_age = cache_timeout
            rv.expires = int(time() + cache_timeout)
    else:
        rv.cache_control.no_cache = True

    return rv
