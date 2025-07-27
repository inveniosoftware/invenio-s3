from concurrent.futures import ThreadPoolExecutor

import pytest

MB = 2**20


def test_multipart_flow(base_app, s3_storage):
    part_size = 7 * MB
    last_part_size = 5 * MB

    # initialize the upload
    upload_metadata = dict(
        parts=2, part_size=part_size, size=part_size + last_part_size
    )
    upload_metadata |= s3_storage.multipart_initialize_upload(**upload_metadata) or {}

    # can not commit just now because no parts were uploaded
    with pytest.raises(ValueError):
        s3_storage.multipart_commit_upload(**upload_metadata)

    # check that links are generated

    links = s3_storage.multipart_links(**upload_metadata)["parts"]
    assert len(links) == 2
    assert links[0]["part"] == 1
    assert "url" in links[0]
    assert links[1]["part"] == 2
    assert "url" in links[1]

    # upload the first part manually
    multipart_file = s3_storage.multipart_file(upload_metadata["uploadId"])
    multipart_file.upload_part(1, b"0" * part_size)
    assert len(multipart_file.get_parts(2)) == 1

    # still can not commit because not all parts were uploaded
    with pytest.raises(ValueError):
        s3_storage.multipart_commit_upload(**upload_metadata)

    # upload the second part
    multipart_file.upload_part(2, b"1" * last_part_size)
    assert len(multipart_file.get_parts(2)) == 2

    s3_storage.multipart_commit_upload(**upload_metadata)

    assert s3_storage.open("rb").read() == b"0" * part_size + b"1" * last_part_size


def test_multipart_abort(base_app, s3_storage):
    part_size = 7 * MB
    last_part_size = 5 * MB

    # initialize the upload
    upload_metadata = dict(
        parts=2, part_size=part_size, size=part_size + last_part_size
    )
    upload_metadata |= s3_storage.multipart_initialize_upload(**upload_metadata) or {}

    s3_storage.multipart_abort_upload(**upload_metadata)


def test_set_content_not_supported(base_app, s3_storage):
    part_size = 7 * MB
    last_part_size = 5 * MB

    # initialize the upload
    upload_metadata = dict(
        parts=2, part_size=part_size, size=part_size + last_part_size
    )
    upload_metadata |= s3_storage.multipart_initialize_upload(**upload_metadata) or {}

    with pytest.raises(NotImplementedError):
        s3_storage.multipart_set_content(
            1, b"0" * part_size, part_size, **upload_metadata
        )


# Test for large number of parts
#
# This test is marked as manual because it must be run against an S3 instance
# such as AWS S3 or CEPH RADOS Gateway that has a ListParts limit of 1000.
# It is not suitable for local testing with MinIO as it does not have this limit
# and will return all parts in a single request.
#
# To run this test, set the following environment variables:
#
# ```bash
# export S3_ENDPOINT_URL=https://<your-s3-endpoint>
# export S3_ACCESS_KEY_ID=<your-access-key>
# export S3_SECRET_ACCESS_KEY=<your-secret-key>
# export S3_BUCKET=<your-bucket-name>
# export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
# export AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
# ```
# Note: the bucket must exist before running the test.
#
# and run the test with:
#
# ```bash
# pytest tests/test_multipart.py::test_multipart_flow_large_number_of_parts
# ```
#
# The test will create a multipart upload with 1560 parts, each of size 5 MB,
# totalling upload size of 7.8 GB. Might take a long time to run on slow networks.
#
@pytest.mark.manual()
def test_multipart_flow_large_number_of_parts(base_app, s3_storage):
    part_size = 5 * MB
    parts = 1560

    # initialize the upload
    upload_metadata = dict(parts=parts, part_size=part_size, size=parts * part_size)
    upload_metadata |= s3_storage.multipart_initialize_upload(**upload_metadata) or {}

    # check that links are generated
    links = s3_storage.multipart_links(**upload_metadata)["parts"]
    assert len(links) == parts
    assert links[0]["part"] == 1
    assert "url" in links[0]
    assert links[-1]["part"] == parts
    assert "url" in links[-1]

    # upload the parts manually
    part_data = b"0" * part_size

    def upload_part(part_number):
        # running in a different thread, so we need to initialize the app context
        with base_app.app_context():
            # upload the part
            multipart_file = s3_storage.multipart_file(upload_metadata["uploadId"])
            multipart_file.upload_part(part_number + 1, part_data)

    executor = ThreadPoolExecutor(10)
    uploaded_count = 0
    for _ in executor.map(upload_part, range(parts)):
        uploaded_count += 1
    executor.shutdown(wait=True)
    assert uploaded_count == parts
    s3_storage.multipart_commit_upload(**upload_metadata)
