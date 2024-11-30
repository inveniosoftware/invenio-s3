..
    Copyright (C) 2018, 2019, 2020 Esteban J. G. Gabancho.
    Copyright (C) 2024 Graz University of Technology.
    Invenio-S3 is free software; you can redistribute it and/or modify it
    under the terms of the MIT License; see LICENSE file for more details.

Changes
=======

Version 2.0.0 (release 2024-12-10)

- filename: replace encoding/decoding
- setup: bump major dependencies

Version 1.0.7 (release 2024-11-30)

- setup: change to reusable workflows
- setup: pin dependencies
- Update GitHub Actions versions
- Update Python versions in workflows
- remove future imports
- Update dependencies and fix import in conftest.py
- global: fix revision id due to rebase
- fix docs compatibilty problem with Sphinx>=5.0.0
- global: clean test infrastructure
- add .git-blame-ignore-revs
- migrate to use black as opinionated auto formater
- migrate setup.py to setup.cfg
- global: fix ci

Version 1.0.6 (released 2021-10-21)

- Unpin boto3 and s3fs

Version 1.0.5 (released 2021-10-20)

- Bump versions to support Flask v2.

Version 1.0.4 (released 2021-07-30)

- Fix number of parts calculations.
- Removed Python 2.7 support.

Version 1.0.3 (released 2020-04-25)

- Allow for dynamic part size for multipart uploads.
- Adds new configuration variables to define default part size and maximum
  number of parts.

Version 1.0.2 (released 2020-02-17)

- Fixes typos on configuration variables and cached properties.
- Adds AWS region name and signature version to configuration.

Version 1.0.1 (released 2019-01-23)

- New configuration variable for URL expiration.
- Enhances file serving.
- Unpins Boto3 library.
- Fixes test suit configuration.

Version 1.0.0 (released 2018-09-19)

- Initial public release.
