# Third-Party Software Licenses

This document contains the licenses for third-party software included in or used by this project.

## Python Packages

The following Python packages are used by this project. Each package is distributed under its respective license.

### Core Dependencies

#### Polars
- **License:** MIT License
- **Homepage:** https://www.pola.rs/
- **Description:** Fast multi-threaded DataFrame library

#### Delta Lake (deltalake-python)
- **License:** Apache License 2.0
- **Homepage:** https://delta.io/
- **Description:** Delta Lake Python bindings

#### PyYAML
- **License:** MIT License
- **Homepage:** https://pyyaml.org/
- **Description:** YAML parser and emitter for Python

#### python-dotenv
- **License:** BSD-3-Clause License
- **Homepage:** https://github.com/theskumar/python-dotenv
- **Description:** Read key-value pairs from .env file

#### requests
- **License:** Apache License 2.0
- **Homepage:** https://requests.readthedocs.io/
- **Description:** HTTP library for Python

#### PyArrow
- **License:** Apache License 2.0
- **Homepage:** https://arrow.apache.org/
- **Description:** Python library for Apache Arrow

#### psutil
- **License:** BSD-3-Clause License
- **Homepage:** https://github.com/giampaolo/psutil
- **Description:** Cross-platform process and system utilities

#### cryptography
- **License:** Apache License 2.0 / BSD License (dual-licensed)
- **Homepage:** https://cryptography.io/
- **Description:** Cryptographic recipes and primitives

#### coolname
- **License:** BSD-3-Clause License
- **Homepage:** https://github.com/alexanderlukanin13/coolname
- **Description:** Random name generator


### Azure SDK

#### azure-kusto-data
- **License:** MIT License
- **Homepage:** https://github.com/Azure/azure-kusto-python
- **Description:** Azure Kusto (Azure Data Explorer) Python client

#### azure-identity
- **License:** MIT License
- **Homepage:** https://github.com/Azure/azure-sdk-for-python
- **Description:** Azure Active Directory authentication for Azure SDK

#### azure-storage-file-datalake
- **License:** MIT License
- **Homepage:** https://github.com/Azure/azure-sdk-for-python
- **Description:** Azure Data Lake Storage Gen2 client library

### Async HTTP & Kafka

#### aiohttp
- **License:** Apache License 2.0
- **Homepage:** https://docs.aiohttp.org/
- **Description:** Async HTTP client/server framework

#### aiokafka
- **License:** Apache License 2.0
- **Homepage:** https://github.com/aio-libs/aiokafka
- **Description:** Async Kafka client for Python

### Telemetry

#### prometheus-client
- **License:** Apache License 2.0
- **Homepage:** https://github.com/prometheus/client_python
- **Description:** Prometheus instrumentation library for Python

### Validation

#### pydantic
- **License:** MIT License
- **Homepage:** https://docs.pydantic.dev/
- **Description:** Data validation using Python type annotations

### Testing

#### pytest
- **License:** MIT License
- **Homepage:** https://pytest.org/
- **Description:** Testing framework for Python

#### pytest-asyncio
- **License:** Apache License 2.0
- **Homepage:** https://github.com/pytest-dev/pytest-asyncio
- **Description:** Pytest support for asyncio

#### pytest-cov
- **License:** MIT License
- **Homepage:** https://github.com/pytest-dev/pytest-cov
- **Description:** Coverage plugin for pytest

#### testcontainers
- **License:** Apache License 2.0
- **Homepage:** https://github.com/testcontainers/testcontainers-python
- **Description:** Python library for Docker-based integration tests

#### docker
- **License:** Apache License 2.0
- **Homepage:** https://github.com/docker/docker-py
- **Description:** Docker SDK for Python

---

## License Summary

### Apache License 2.0 Packages
- Delta Lake (deltalake-python)
- requests
- PyArrow
- cryptography (dual-licensed)
- tenacity
- aiohttp
- aiokafka
- prometheus-client
- pytest-asyncio
- testcontainers
- docker

### MIT License Packages
- Polars
- PyYAML
- python-dotenv
- azure-kusto-data
- azure-identity
- azure-storage-file-datalake
- pydantic
- pytest
- pytest-cov

### BSD-3-Clause License Packages
- psutil
- coolname
- cryptography (dual-licensed)

---

## Compliance Notes

1. **Apache 2.0 Packages:** Require NOTICE file inclusion and preservation of copyright notices
2. **MIT Packages:** Require copyright notice and permission notice in all copies
3. **BSD-3-Clause Packages:** Require copyright notice and disclaimer in binary distributions

All dependencies are compatible with proprietary software. No GPL/LGPL/AGPL licenses detected.

---

## Full License Texts

### Apache License 2.0
See: https://www.apache.org/licenses/LICENSE-2.0

### MIT License
See: https://opensource.org/licenses/MIT

### BSD-3-Clause License
See: https://opensource.org/licenses/BSD-3-Clause

---

**Note:** This document was generated on 2026-01-22. For the most up-to-date license information, 
please refer to each package's official repository or PyPI page.

**License Audit Recommendation:** Run `pip-licenses --format=markdown --with-urls` periodically 
to verify license compliance of all dependencies and transitive dependencies.
