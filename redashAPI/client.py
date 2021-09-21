import json
import requests
import time
from datetime import datetime


class AlreadyExistsException(Exception):
    """Raised when the object already exists"""
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
        res = self.get('data_sources')
        return res

    def get_data_source_by_name(self, name: str):
        return next((ds for ds in self.get_data_sources().json() if ds['name'] == name), None)

    def create_or_update_datasource(self, _type: str, name: str, options: dict = None):
        existing_ds = self.get_data_source_by_name(name)
        if existing_ds is None:
            return self.create_data_source(self, _type, name, options)
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
        else:
            resp = requests.Response()
            resp.status_code = 404
            return resp

    def get_users(self, count: int=50, with_pending: bool=False, sort_order: str='name'):
        params = {'pending': with_pending,'order': sort_order, 'page_size': count}
        res = self.s.get(f"{self.host}/api/users", params=params)

        if res.status_code != 200 and res.status_code != 204:
            raise Exception(f"[GET] /api/users ({res.status_code})")

        return res

    def create_group(self, name: str):
        if self.get_group_by_name(name) is None:
            payload = {"name": name}

            return self.post('groups', payload)
        raise AlreadyExistsException(f"Group {name} already exists!")

    def get_groups(self):
        return self.get('groups')

    def get_group_by_name(self, name: str):
        return next((gr for gr in self.get_groups().json() if gr['name'] == name), None)
    
    def delete_group(self, name: str):
        existing_gr = self.get_group_by_name(name)
        if existing_gr is not None:
            return self.delete(f"groups/{existing_gr['id']}")
        else:
            resp = requests.Response()
            resp.status_code = 404
            return resp

    def add_datasource_to_group(self):
        pass  # not implemented yet

    def add_user_to_group(self):
        pass  # not implemented yet

