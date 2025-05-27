# -*- coding: utf-8 -*-
#
# Copyright (C) 2018, 2019, 2020 Esteban J. G. Gabancho.
# Copyright (C) 2024 KTH Royal Institute of Technology.
#
# Invenio-S3 is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
"""Storage tests."""

import errno
import os
import shutil
import tempfile
from io import BytesIO
from unittest.mock import patch

import pytest
from invenio_files_rest.errors import (
    FileSizeError,
    StorageError,
    UnexpectedFileSizeError,
)
from invenio_files_rest.limiters import FileSizeLimit
from invenio_files_rest.storage import PyFSFileStorage
from s3fs import S3File, S3FileSystem

from invenio_s3 import S3FSFileStorage, s3fs_storage_factory


def test_factory(file_instance_mock):
    """Test factory creation."""
    assert isinstance(
        s3fs_storage_factory(fileinstance=file_instance_mock), S3FSFileStorage
    )

    s3fs = s3fs_storage_factory(fileinstance=file_instance_mock)
    assert s3fs.fileurl == file_instance_mock.uri

    file_instance_mock.uri = None
    s3fs = s3fs_storage_factory(
        fileinstance=file_instance_mock, default_location="s3://test"
    )
    assert s3fs.fileurl == "s3://test/de/ad/beef-65bd-4d9b-93e2-ec88cc59aec5/data"


def test_non_s3_path(tmpdir):
    non_s3_path = os.path.join(tmpdir.dirname, "test.txt")
    s3fs = S3FSFileStorage(non_s3_path)
    fs, _ = s3fs._get_fs()
    assert not isinstance(fs, S3FileSystem)


@pytest.mark.parametrize(
    "file_size",
    (
        1,
        S3FileSystem.default_block_size,
        S3FileSystem.default_block_size + 1,
        S3FileSystem.default_block_size * 2,
        (S3FileSystem.default_block_size * 2) + 1,
    ),
)
def test_initialize(s3_storage, file_size, s3fs, s3_bucket):
    """Test init of files."""
    _, size, checksum = s3_storage.initialize(size=file_size)

    assert size == file_size
    assert checksum is None

    objs = s3fs.listdir(s3_bucket, detail=True, refresh=True)
    assert len(objs) == 1
    assert objs[0]["Size"] == size

    _, size, checksum = s3_storage.initialize()
    assert size == 0

    objs = s3fs.listdir(s3_bucket, detail=True, refresh=True)
    assert len(objs) == 1
    assert objs[0]["Size"] == size


def test_initialize_failcleanup(monkeypatch, s3_storage, s3fs, s3_bucket):
    """Test basic cleanup on fail."""
    monkeypatch.setattr(S3File, "write", lambda x: x, raising=True)
    pytest.raises(Exception, s3_storage.initialize, size=100)

    fs, path = s3_storage._get_fs()
    assert not fs.exists(path)

    objs = s3fs.listdir(s3_bucket, detail=True)
    assert len(objs) == 0


def test_delete(s3_storage, s3_bucket, s3fs, s3_path):
    """Test delete."""
    s3_storage.save(BytesIO(b"test"))

    objs = s3fs.listdir(s3_bucket, detail=True, refresh=True)
    assert len(objs) == 1

    assert s3_storage.delete()
    fs, path = s3_storage._get_fs()
    assert not fs.exists(path)
    assert not fs.exists(s3_path)

    objs = s3fs.listdir(s3_bucket, detail=True, refresh=True)
    assert len(objs) == 0


@pytest.mark.parametrize(
    "data",
    (
        b"test",
        os.urandom((S3FileSystem.default_block_size)),
        os.urandom((S3FileSystem.default_block_size + 1)),
        os.urandom((S3FileSystem.default_block_size * 2)),
        os.urandom(((S3FileSystem.default_block_size * 2) + 1)),
    ),
)
def test_save(s3_bucket, s3_path, s3_storage, s3fs, get_md5, data):
    """Test save."""
    uri, size, checksum = s3_storage.save(BytesIO(data))
    assert uri == s3_path
    assert size == len(data)
    assert checksum == get_md5(data)

    objs = s3fs.listdir(s3_bucket, detail=True, refresh=True)
    assert len(objs) == 1
    assert objs[0]["Key"] == "default/file.txt"
    assert objs[0]["Size"] == size

    fs, path = s3_storage._get_fs()
    assert fs.exists(path)
    assert fs.exists(s3_path)
    assert fs.open(path).read() == data


def test_save_failcleanup(s3_storage, s3_path):
    """Test basic cleanup on fail."""
    data = b"somedata"

    def fail_callback(total, size):
        assert fs.exists(s3_path)
        raise Exception("Something bad happened")

    pytest.raises(
        Exception,
        s3_storage.save,
        BytesIO(data),
        chunk_size=4,
        progress_callback=fail_callback,
    )
    fs, path = s3_storage._get_fs()
    assert not fs.exists(path)
    assert not fs.exists(s3_path)


def test_save_callback(s3_storage):
    """Test save progress callback."""
    data = b"somedata"

    counter = dict(size=0)

    def callback(total, size):
        counter["size"] = size

    _ = s3_storage.save(BytesIO(data), progress_callback=callback)

    assert counter["size"] == len(data)


def test_save_limits(s3_storage):
    """Test save limits."""
    data = b"somedata"
    _, size, _ = s3_storage.save(BytesIO(data), size=len(data))
    assert size == len(data)

    _, size, _ = s3_storage.save(BytesIO(data), size_limit=len(data))
    assert size == len(data)

    # Size doesn't match
    pytest.raises(
        UnexpectedFileSizeError, s3_storage.save, BytesIO(data), size=len(data) - 1
    )
    pytest.raises(
        UnexpectedFileSizeError, s3_storage.save, BytesIO(data), size=len(data) + 1
    )

    # Exceeds size limits
    pytest.raises(
        FileSizeError,
        s3_storage.save,
        BytesIO(data),
        size_limit=FileSizeLimit(len(data) - 1, "bla"),
    )


@pytest.mark.parametrize(
    "file_size",
    (
        100,
        S3FileSystem.default_block_size,
        S3FileSystem.default_block_size + 1,
        S3FileSystem.default_block_size * 2,
        (S3FileSystem.default_block_size * 2) + 1,
    ),
)
def test_update(s3_storage, get_md5, file_size):
    """Test update file."""
    s3_storage.initialize(size=file_size)

    # Write at the beginning of the file
    s3_storage.update(BytesIO(b"cd"), seek=2, size=2)
    s3_storage.update(BytesIO(b"ab"), seek=0, size=2)

    fs, path = s3_storage._get_fs()
    content = fs.open(path).read()
    assert content[0:4] == b"abcd"
    assert len(content) == file_size

    # Write at the middle of the file
    init_position = int(file_size / 2)
    s3_storage.update(BytesIO(b"cd"), seek=(init_position + 2), size=2)
    s3_storage.update(BytesIO(b"ab"), seek=init_position, size=2)

    fs, path = s3_storage._get_fs()
    content = fs.open(path).read(file_size)
    assert content[init_position : (init_position + 4)] == b"abcd"
    assert len(content) == file_size

    # Write at the end of the file
    init_position = file_size - 4
    s3_storage.update(BytesIO(b"cd"), seek=(init_position + 2), size=2)
    s3_storage.update(BytesIO(b"ab"), seek=init_position, size=2)

    fs, path = s3_storage._get_fs()
    content = fs.open(path).read()
    assert content[init_position : (init_position + 4)] == b"abcd"
    assert len(content) == file_size

    # Assert return parameters from update.
    size, checksum = s3_storage.update(BytesIO(b"ef"), seek=4, size=2)
    assert size == 2
    assert get_md5(b"ef") == checksum


def test_update_fail(s3_storage, s3_path):
    """Test update of file."""

    def fail_callback(total, size):
        assert fs.exists(s3_path)
        raise Exception("Something bad happened")

    s3_storage.initialize(size=100)
    s3_storage.update(BytesIO(b"ab"), seek=0, size=2)
    pytest.raises(
        Exception,
        s3_storage.update,
        BytesIO(b"cdef"),
        seek=2,
        size=4,
        chunk_size=2,
        progress_callback=fail_callback,
    )

    # Partial file can be written to disk!
    fs, path = s3_storage._get_fs()
    content = fs.open(path).read()
    assert content[0:4] == b"abcd"
    assert content[4:6] != b"ef"


def test_checksum(s3_storage, get_md5):
    """Test fixity."""
    # Compute checksum of license file
    with open("LICENSE", "rb") as fp:
        data = fp.read()
        checksum = get_md5(data)

    counter = dict(size=0)

    def callback(total, size):
        counter["size"] = size

    # Now do it with storage interface
    with open("LICENSE", "rb") as fp:
        _, size, save_checksum = s3_storage.save(fp, size=os.path.getsize("LICENSE"))
    assert checksum == save_checksum
    assert checksum == s3_storage.checksum(chunk_size=2, progress_callback=callback)
    assert counter["size"] == size
    assert counter["size"] == os.path.getsize("LICENSE")

    # No size provided, means progress callback isn't called
    counter["size"] = 0
    s = S3FSFileStorage(s3_storage.fileurl)
    assert checksum == s.checksum(chunk_size=2, progress_callback=callback)
    assert counter["size"] == 0


def test_checksum_fail(s3_storage):
    """Test fixity problems."""

    # Raise an error during checksum calculation
    def callback(total, size):
        raise OSError(errno.EPERM, "Permission")

    s3_storage.save(open("LICENSE", "rb"), size=os.path.getsize("LICENSE"))

    pytest.raises(StorageError, s3_storage.checksum, progress_callback=callback)


def test_copy(s3_bucket, s3_storage):
    """Test copy file."""
    data = b"test"
    s3_storage.save(BytesIO(data))

    s3_copy_path = f"{s3_bucket}/path/to/copy/data"
    s3fs_copy = S3FSFileStorage(s3_copy_path)
    uri, _, _ = s3fs_copy.copy(s3_storage)

    assert uri == s3_copy_path
    assert s3fs_copy.open().read() == data

    tmppath = tempfile.mkdtemp()

    s = PyFSFileStorage(os.path.join(tmppath, "anotherpath/data"))
    data = b"othertest"
    s.save(BytesIO(data))
    uri, _, _ = s3fs_copy.copy(s)

    assert uri == s3_copy_path
    assert s3fs_copy.open().read() == data

    shutil.rmtree(tmppath)


def test_send_file(base_app, s3_storage):
    """Test send file."""
    data = b"sendthis"
    _ = s3_storage.save(BytesIO(data))

    with base_app.test_request_context():
        res = s3_storage.send_file("test.txt", mimetype="text/plain")
        assert res.status_code == 302
        h = res.headers
        assert "Location" in h

        res = s3_storage.send_file("myfilename.txt", mimetype="text/plain")
        assert res.status_code == 302


def test_send_file_fail(base_app, s3_storage):
    """Test send file."""
    s3_storage.save(BytesIO(b"content"))

    with patch("invenio_s3.storage.redirect_stream") as redirect_stream:
        redirect_stream.side_effect = OSError(errno.EPERM, "Permission problem")
        with base_app.test_request_context():
            pytest.raises(StorageError, s3_storage.send_file, "test.txt")


def test_non_unicode_filename(base_app, s3_storage):
    """Test sending the non-unicode filename in the header."""
    data = b"HelloWorld"
    _, _, checksum = s3_storage.save(BytesIO(data))

    with base_app.test_request_context():
        res = s3_storage.send_file(
            "żółć.dat", mimetype="application/octet-stream", checksum=checksum
        )
        assert res.status_code == 302
        assert set(res.headers["Content-Disposition"].split("; ")) == set(
            [
                "attachment",
                "filename=zoc.dat",
                "filename*=UTF-8''%C5%BC%C3%B3%C5%82%C4%87.dat",
            ]
        )

    with base_app.test_request_context():
        res = s3_storage.send_file("żółć.txt", mimetype="text/plain", checksum=checksum)
        assert res.status_code == 302
        assert res.headers["Content-Disposition"] == "inline"


def test_block_size(appctx, s3_path, s3_storage, get_md5):
    """Test block size update on the S3FS client."""
    # Make file bigger than max number of parts * block size
    data = b"a" * appctx.config["S3_DEFAULT_BLOCK_SIZE"] * 5
    # Set max number of parts that size(data)/num parts > block size
    # 3 parts makes a division result with a floating value smaller than .5
    appctx.config["S3_MAXIMUM_NUMBER_OF_PARTS"] = 3
    uri, size, checksum = s3_storage.save(BytesIO(data), size=len(data))

    assert (
        len(data) / s3_storage.block_size <= appctx.config["S3_MAXIMUM_NUMBER_OF_PARTS"]
    )
    assert uri == s3_path
    assert size == len(data)
    assert checksum == get_md5(data)
