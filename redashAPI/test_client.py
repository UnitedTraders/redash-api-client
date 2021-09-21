from .client import RedashAPIClient, AlreadyExistsException
import pytest

REDASH_HOST = "http://redash:5000"
REDASH_API_KEY = "put-your-key-here"

redash = RedashAPIClient(REDASH_API_KEY, REDASH_HOST)

# remove old data
redash.delete_data_source("_datasource-test")
redash.delete_group("_group-test")

@pytest.fixture(scope='module')
def global_data():
    return {'ds_id': 0, 'gr_id': 0}

def test_create_data_source(global_data):
    res = redash.create_data_source("pg", "_datasource-test", options={
        "dbname": "test_ds",
        "host": "test_host",
        "password": "test_pwd",
        "port": 35432,
        "user": "test_user"}).json()
    assert res["type"] == "pg"
    assert res["options"]["dbname"] == "test_ds"
    assert res["options"]["port"] == 35432
    assert res["options"]["password"] == "--------"
    global_data['ds_id'] = res["id"]

def test_create_duplicate_data_source():
    with pytest.raises(AlreadyExistsException):
        res = redash.create_data_source("pg", "_datasource-test", options={
            "dbname": "test_ds",
            "host": "test_host",
            "password": "test_pwd",
            "port": 35432,
            "user": "test_user"}).json()

def test_get_data_source_by_name(global_data):
    res = redash.get_data_source_by_name("_datasource-test")
    assert res["type"] == "pg"
    assert global_data['ds_id'] == res["id"]

def test_create_or_update_datasource(global_data):
    res = redash.create_or_update_datasource("pg", "_datasource-test", options={
        "dbname": "test_ds_2",
        "host": "test_host",
        "password": "test_pwd",
        "port": 35432,
        "user": "test_user"}).json()
    assert res["type"] == "pg"
    assert res["options"]["dbname"] == "test_ds_2"
    assert res["options"]["port"] == 35432
    assert res["options"]["password"] == "--------"
    assert global_data['ds_id'] == res["id"]

def test_get_data_source_by_name_after_update(global_data):
    res = redash.get_data_source_by_name("_datasource-test")
    assert res["type"] == "pg"
    assert global_data['ds_id'] == res["id"]

def test_delete_data_source():
    # if ds existed - return 204
    res = redash.delete_data_source("_datasource-test")
    assert res.status_code == 204
    # if not - 404
    res = redash.delete_data_source("_datasource-test")
    assert res.status_code == 404

def test_create_group(global_data):
    res = redash.create_group("_group-test").json()
    assert res["name"] == "_group-test"
    assert res["type"] == "regular"
    global_data['gr_id'] = res["id"]

def test_delete_group():
    # if group existed - return 204
    res = redash.delete_group("_group-test")
    assert res.status_code == 200
    # if group - 404
    res = redash.delete_group("_group-test")
    assert res.status_code == 404
