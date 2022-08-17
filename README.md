# Redash-API-Client

[![PyPI version fury.io](https://badge.fury.io/py/redash-api-client.svg)](https://pypi.org/project/redash-api-client/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/redash-api-client.svg)](https://pypi.python.org/pypi/redash-api-client/)
[![PyPI license](https://img.shields.io/pypi/l/redash-api-client.svg)](https://pypi.python.org/pypi/redash-api-client/)
[![Downloads](https://pepy.tech/badge/redash-api-client)](https://pepy.tech/project/redash-api-client)

Redash API Client written in Python.

## Dependencies

* Python3.6+

## Installation

    pip install git+https://github.com/UnitedTraders/redash-api-client-ut.git

## Getting Started

```python
from redashAPI import RedashAPIClient

# Create API client instance
"""
    :args:
    API_KEY
    REDASH_HOST (optional): `http://localhost:5000` by default
"""
Redash = RedashAPIClient(API_KEY, REDASH_HOST)
```

### Redash's RESTful API

| URI                | Supported Methods              |
| ------------------ | ------------------------------ |
| *users*            | **GET**, **POST**              |
| *users/1*          | **GET**, **POST**              |
| *groups*           | **GET**, **POST**              |
| *groups/1*         | **GET**, **POST**, **DELETE**  |
| *data_sources*     | **GET**, **POST**              |
| *data_sources/1*   | **GET**, **POST**, **DELETE**  |

Some examples you can see in `redashAPI/test_client.py`. I know that the shared state between tests is pure evil, but I want to quickly test my code.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
