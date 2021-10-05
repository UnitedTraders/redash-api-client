import json
import requests
import time
from datetime import datetime


class AlreadyExistsException(Exception):
    """Raised when the object already exists"""
    pass

class EntityNotFoundException(Exception):
    """Raised when the object not found"""
    pass

class RedashAPIClient:
    def __init__(self, api_key: str, host: str="http://localhost:5000"):
        self.api_key = api_key
        self.host = host

        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Key {api_key}"})

    def get(self, uri: str):
        res = self.s.get(f"{self.host}/api/{uri}")

        if res.status_code != 200:
            raise Exception(f"[GET] /api/{uri} ({res.status_code})")

        return res

    def post(self, uri: str, payload: dict=None):
        if payload is None or not isinstance(payload, dict):
            payload = {}

        data = json.dumps(payload)

        self.s.headers.update({"Content-Type": "application/json"})
        res = self.s.post(f"{self.host}/api/{uri}", data=data)

        if res.status_code != 200:
            raise Exception(f"[POST] /api/{uri} ({res.status_code})")

        return res

    def delete(self, uri: str):
        res = self.s.delete(f"{self.host}/api/{uri}")

        if res.status_code != 200 and res.status_code != 204:
            raise Exception(f"[DELETE] /api/{uri} ({res.status_code})")

        return res

    def create_data_source(self, _type: str, name: str, options: dict=None):

        if self.get_data_source_by_name(name) is None:
            if options is None or not isinstance(options, dict):
                options = {}

            payload = {
                "type": _type,
                "name": name,
                "options": options
            }

            return self.post('data_sources', payload)
        raise AlreadyExistsException(f"Datasource {name} already exists!")

    def get_data_sources(self):
        res = self.get('data_sources').json()
        return res

    def get_data_source_by_name(self, name: str):
        return next((ds for ds in self.get_data_sources() if ds['name'] == name), None)

    def create_or_update_datasource(self, _type: str, name: str, options: dict = None):
        existing_ds = self.get_data_source_by_name(name)
        if existing_ds is None:
            return self.create_data_source(_type, name, options)
        else:
            ds_id: int = existing_ds['id']
            payload = {
                "id": ds_id,
                "type": _type,
                "name": name,
                "options": options
            }
            return self.post(f"data_sources/{ds_id}", payload)

    def delete_data_source(self, name: str):
        existing_ds = self.get_data_source_by_name(name)
        if existing_ds is not None:
            return self.delete(f"data_sources/{existing_ds['id']}")
        resp = requests.Response()
        resp.status_code = 404
        return resp

    def create_user(self, name: str, email: str):
        payload = {
            "email": email,
            "name": name,
        }
        return self.post(f"users", payload)

    def get_users(self, count: int=250, with_pending: bool=True, sort_order: str='name'):
        params = {'pending': False,'order': sort_order, 'page_size': count}
        res = self.s.get(f"{self.host}/api/users", params=params)

        if res.status_code != 200 and res.status_code != 204:
            raise Exception(f"[GET] /api/users ({res.status_code})")

        all_users = res.json()['results']

        if with_pending:
            params = {'pending': True, 'order': sort_order, 'page_size': count}
            res = self.s.get(f"{self.host}/api/users", params=params)
            if res.status_code != 200 and res.status_code != 204:
                raise Exception(f"[GET] /api/users ({res.status_code})")
            all_users += res.json()['results']
        
        return all_users

    def get_user_by_name(self, name: str):
       #all_users = 
        return next((usr for usr in self.get_users(count=250) if usr['name'] == name), None)

    def delete_user(self, name: str):
        existing_user = self.get_user_by_name(name)
        if existing_user is not None:
            return self.delete(f"users/{existing_user['id']}")
        resp = requests.Response()
        resp.status_code = 404
        return resp

    def create_group(self, name: str):
        if self.get_group_by_name(name) is None:
            payload = {"name": name}

            return self.post('groups', payload)
        raise AlreadyExistsException(f"Group {name} already exists!")

    def get_groups(self):
        return self.get('groups').json()

    def get_group_by_name(self, name: str):
        return next((gr for gr in self.get_groups() if gr['name'] == name), None)

    def get_group_users_by_id(self, id: int):
        return self.get(f'groups/{id}/members').json()

    def get_group_data_sources_by_id(self, id: int):
        return self.get(f'groups/{id}/data_sources').json()

    def delete_group(self, name: str):
        existing_gr = self.get_group_by_name(name)
        if existing_gr is not None:
            return self.delete(f"groups/{existing_gr['id']}")
        resp = requests.Response()
        resp.status_code = 404
        return resp

    def add_user_to_group(self, user_name: str, group_name: str):
        user = self.get_user_by_name(user_name)
        if user is None:
            raise EntityNotFoundException(f"User {user_name} not found!")
        group = self.get_group_by_name(group_name)
        if group is None:
            raise EntityNotFoundException(f"Group {group_name} not found!")
        payload = {"user_id": user["id"]}
        # make POST request if user not present in group, else return 200 - "Not changed"
        if next((member for member in self.get_group_users_by_id(group["id"]) if member['id'] == user['id']), None) is None:
            return self.post(f'groups/{group["id"]}/members', payload)
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b'{"msg": "Not changed"}'
        return resp

    def delete_user_from_group(self, user_name: str, group_name: str):
        user = self.get_user_by_name(user_name)
        if user is None:
            raise EntityNotFoundException(f"User {user_name} not found!")
        group = self.get_group_by_name(group_name)
        if group is None:
            raise EntityNotFoundException(f"Group {group_name} not found!")
        # make DELETE request if user present in group, else return 404
        if next((member for member in self.get_group_users_by_id(group["id"]) if member['id'] == user['id']), None) is not None:
            return self.delete(f'groups/{group["id"]}/members/{user["id"]}')
        resp = requests.Response()
        resp.status_code = 404
        return resp
        
    def add_data_source_to_group(self, data_source_name: str, group_name: str):
        ds = self.get_data_source_by_name(data_source_name)
        if ds is None:
            raise EntityNotFoundException(f"Data source {data_source_name} not found!")
        group = self.get_group_by_name(group_name)
        if group is None:
            raise EntityNotFoundException(f"Group {group_name} not found!")
        payload = {"data_source_id": ds["id"]}
        # make POST request if ds not present in group, else return 404
        if next((member for member in self.get_group_data_sources_by_id(group["id"]) if member['id'] == ds['id']), None) is None:
            return self.post(f'groups/{group["id"]}/data_sources', payload)
        resp = requests.Response()
        resp.status_code = 200
        resp._content = b'{"msg": "Not changed"}'
        return resp

    def delete_data_source_from_group(self, data_source_name: str, group_name: str):
        ds = self.get_data_source_by_name(data_source_name)
        if ds is None:
            raise EntityNotFoundException(f"Data source {data_source_name} not found!")
        group = self.get_group_by_name(group_name)
        if group is None:
            raise EntityNotFoundException(f"Group {group_name} not found!")
        # make DELETE request if ds present in group, else return 404
        if next((member for member in self.get_group_data_sources_by_id(group["id"]) if member['id'] == ds['id']), None) is not None:
            return self.delete(f'groups/{group["id"]}/data_sources/{ds["id"]}')
        resp = requests.Response()
        resp.status_code = 404
        return resp

