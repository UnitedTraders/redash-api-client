from .client import RedashAPIClient, AlreadyExistsException, EntityNotFoundException
import pytest

REDASH_HOST = "http://redash:5000"
REDASH_API_KEY = "put-your-key-here"

redash = RedashAPIClient(REDASH_API_KEY, REDASH_HOST)

# remove old data
redash.delete_data_source("_datasource-test")
redash.delete_data_source("_datasource-test2")
redash.delete_group("_group-test")
redash.delete_user("_user1-test")

@pytest.fixture(scope='module')
def global_data():
    return {'ds_id': 0, 'gr_id': 0, 'user_id': 0}

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

def test_create_data_source_via_create_or_update(global_data):
    res = redash.create_or_update_datasource("pg", "_datasource-test2", options={
        "dbname": "test_ds2",
        "host": "test_host",
        "password": "test_pwd",
        "port": 35432,
        "user": "test_user"}).json()
    assert res["type"] == "pg"
    assert res["options"]["dbname"] == "test_ds2"
    assert res["options"]["port"] == 35432
    assert res["options"]["password"] == "--------"

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

def test_create_group(global_data):
    res = redash.create_group("_group-test").json()
    assert res["name"] == "_group-test"
    assert res["type"] == "regular"
    global_data['gr_id'] = res["id"]

def test_create_user(global_data):
    res = redash.create_user("_user1-test", "test1@example.com").json()
    assert res["name"] == "_user1-test"
    assert res["auth_type"] == "external"
    with pytest.raises(Exception):
       redash.create_user("_user1-test", "test1@example.com").json()
    global_data['user_id'] = res["id"]

def test_add_user_to_group(global_data):
    res = redash.add_user_to_group("_user1-test", "_group-test").json()
    assert res["id"] == global_data["user_id"]
    assert global_data["gr_id"] in res["groups"]
    with pytest.raises(EntityNotFoundException):
        redash.add_user_to_group("THAT-USER-DOESNT-EXIST", "_group-test")
    with pytest.raises(EntityNotFoundException):
        redash.add_user_to_group("_user1-test", "THAT-GROUP-DOESNT-EXIST")
    with pytest.raises(EntityNotFoundException):
        redash.add_user_to_group("THAT-USER-DOESNT-EXIST", "THAT-GROUP-DOESNT-EXIST")
    res = redash.get_group_users_by_id(global_data["gr_id"])
    assert next((usr for usr in res if usr['id'] == global_data["user_id"]), None) is not None

def test_delete_user_from_group(global_data):
    res = redash.delete_user_from_group("_user1-test", "_group-test")
    assert res.status_code == 200
    res = redash.get_group_users_by_id(global_data["gr_id"])
    assert next((usr for usr in res if usr['id'] == global_data["user_id"]), None) is None
    res = redash.delete_user_from_group("_user1-test", "_group-test")
    assert res.status_code == 404

def test_add_data_source_to_group(global_data):
    res = redash.add_data_source_to_group("_datasource-test", "_group-test").json()
    assert res["id"] == global_data["ds_id"]
    with pytest.raises(EntityNotFoundException):
        redash.add_data_source_to_group("THAT-DS-DOESNT-EXIST", "_group-test")
    with pytest.raises(EntityNotFoundException):
        redash.add_data_source_to_group(
            "_datasource-test", "THAT-GROUP-DOESNT-EXIST")
    with pytest.raises(EntityNotFoundException):
        redash.add_data_source_to_group("THAT-DS-DOESNT-EXIST", "THAT-GROUP-DOESNT-EXIST")
    res = redash.get_group_data_sources_by_id(global_data["gr_id"])
    assert next((ds for ds in res if ds['id'] == global_data["ds_id"]), None) is not None

def test_delete_data_source_from_group(global_data):
    res = redash.delete_data_source_from_group("_datasource-test", "_group-test")
    assert res.status_code == 200
    res = redash.get_group_data_sources_by_id(global_data["gr_id"])
    assert next((ds for ds in res if ds['id'] == global_data["ds_id"]), None) is None
    res = redash.delete_data_source_from_group("_datasource-test", "_group-test")
    assert res.status_code == 404

def test_delete_user():
    # if user existed - return 200
    res = redash.delete_user("_user1-test")
    assert res.status_code == 200
    # if not  - 404
    res = redash.delete_user("_user1-test")
    assert res.status_code == 404

def test_delete_group():
    # if group existed - return 200
    res = redash.delete_group("_group-test")
    assert res.status_code == 200
    # if not - 404
    res = redash.delete_group("_group-test")
    assert res.status_code == 404

def test_delete_data_source():
    # if ds existed - return 204
    res = redash.delete_data_source("_datasource-test")
    assert res.status_code == 204
    res = redash.delete_data_source("_datasource-test2")
    assert res.status_code == 204
    # if not - 404
    res = redash.delete_data_source("_datasource-test")
    assert res.status_code == 404
