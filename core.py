from collections import deque
from copy import copy, deepcopy
from functools import partial
from itertools import chain
import shutil
from json.decoder import JSONDecodeError
from sys import getsizeof, stderr
import asyncio
import datetime
import json
import random
import re
import traceback
import logging
import uuid
import pathlib
import time
import copy

try:
    from reprlib import repr
except ImportError:
    pass


from kivy.clock import Clock
from kivy.properties import ObjectProperty
from kivy.logger import Logger
from kivy.event import EventDispatcher

from kivy_tastypie.config import orm_settings
from kivy_tastypie.forms.utils import build_form_registry
from kivy_tastypie.http_requests import (fetch_data_batch, async_get, join_api_path,
    fetch_all_schema, secure_delete, secure_get)
from kivy_tastypie.http_requests.utils import get_request_string
from kivy_tastypie.models.queryset import Queryset
from kivy_tastypie.models.registry import ModelRegistry
from kivy_tastypie.utils import write_to_log, Timer, background_thread, timeit, get_empty_db_index
from kivy_tastypie.store.db.cursor import SimpleCursor
from kivy_tastypie.store.db.utils import db_relative_path
from .db.worker.operation import WriteOperation, ReadOperation, CreateRecordOperation

class TastyPieMemStore(EventDispatcher):

    remote_url = orm_settings.REMOTE_URL
    delay_timer = 1
    updated_model = ObjectProperty({})

    def __init__(self, *args, **kwargs):
        """
        schema - Latest schema detailing structure of all models exposed through API
        """
        super().__init__()
        self.prepare_db()
        self.cursor = SimpleCursor(self)
        self.api_root = kwargs.get('api_root', orm_settings.API_ROOT)
        self.model_registry = ModelRegistry(self)

    @property
    def default_user_dict(self):
        return {'username':'', 'anonymous':True, 'works_for_username':None,
            'api_key':None, 'okay':False, 'remote_url':orm_settings.REMOTE_URL}

    @property
    def api_base_url(self):
        return join_api_path(self.remote_url, self.api_root)

    @property
    def data_dir(self):
        return pathlib.Path(orm_settings.DB_FOLDER_PATH)

    @property
    def logged_in_user(self):
        return self.read(orm_settings.LOGGED_IN_USER_FILENAME) or self.default_user_dict

    def add_many(self, model_name, dict_list):
        data = self.get(model_name)
        data['objects'].extend(dict_list)
        self.put(model_name, data)
        if dict_list:
            self.refresh_related_objects(self.query(model_name,
                resource_uri=dict_list[0]['resource_uri'])[0])
            self.store_sync(force = True)

    def add_object(self, object_class, new_obj, *args, **kwargs):
        data = self.get(object_class)
        new_data = data
        new_data['objects'].append(new_obj)
        self.put(object_class, **new_data)

    def clear(self):
        shutil.rmtree(self.data_dir)
        self.prepare_db()

    def fetch_all_data(self, *args, **kwargs):
        """ Retrieve all data stored on the remote server. """
        api_info = self.model_registry.get_schema('api_info')
        resource_list = self.get_available_resource_list(api_info)
        return self.fetch_resources_from_list(resource_list,
            api_info=api_info)

    def fetch_all_data_get_args(self, user, *args, **kwargs):
        return []

    def fetch_resources_from_list(self, resource_list, *args, user=None,
                                  api_info=None, **kwargs):

        if not api_info:
            api_info = self.model_registry.get_schema('api_info')
        if not user:
            user = self.logged_in_user
        username = user['username']

        #Determine all the list endpoints that will need to be hit
        get_parameters = self.fetch_all_data_get_args(user)
        endpoint_list = []
        get_query = get_request_string(get_parameters)

        for resource_name in resource_list:
            endpoint_options = api_info[resource_name]
            endpoint_query = join_api_path(self.remote_url,
                endpoint_options['list_endpoint'], get_query)
            endpoint_list.append((resource_name, endpoint_query))
        endpoint_list = self.pre_process_endpoints(endpoint_list)
        
        #Synchronously make requests for all of the data
        headers = self.get_api_request_header(user_dict = user)
        return asyncio.run(fetch_data_batch(endpoint_list, headers = headers))

    def pre_process_endpoints(self, endpoint_list, *args, **kwargs):
        return endpoint_list

    def get(self, model_name, resource_uri, searc_remote=True):
        """ Retrieve a specific object from the database. If not found locally attempt
        to fetch it from the remote server."""

        qs = self.query(model_name, resource_uri)
        if not qs.size:
            okay, resp = secure_get(resource_uri, headers=self.get_api_request_header())
            if okay:
                obj = Queryset.object_from_dict(model_name, resp)
                CreateRecordOperation(run_kwargs={'obj': obj})
                return obj
        else:
            return qs[0]

    def get_available_resource_list(self, api_info, *args, **kwargs):
        return list(api_info.keys())

    def get_api_request_header(self, **kwargs):
        user_dict = kwargs.get('user_dict', self.logged_in_user)
        headers = {'Authorization': f"ApiKey {user_dict['username']}:{user_dict['api_key']}",
            'Content-Type':'application/json'}
        return headers

    def get_object(self, model_name, **query_kwargs):
        qs = self.query(model_name, **query_kwargs)
        if qs.objects:
            return qs.objects[0]
        else:
            return None

    def get_related_objects(self, obj, *args):
        """ Find all records in db that have a field that appears to reference the target
        object. """

        related_fields = []
        related_uris = []

        # Get all foreign key field names
        for name, field_schema in obj._schema['fields'].items():
            if field_schema['type'] in ['related', 'm2m']:
                related_fields.append(name)

        # Get the URI for any objects related to this one by foreign keys
        for f in related_fields:
            uri = getattr(obj, f)
            if uri:
                if isinstance(uri, list): # For many-to-many fields the uris are provided as a list
                    for entry in uri:
                        related_uris.append((self._model_name_from_uri(entry), entry))

                # Other relations are one-to-one, so are only a string
                else:
                    related_uris.append((self._model_name_from_uri(uri), uri))

        # Find other models in the schema that have a fk pointing back to the
        # target obj's model class [via the 'related_schema_name']
        related_model_fields = []
        for model_name, model_schema in self.get('schema').items():
            try:
                for field_name, field_schema in model_schema['fields'].items():
                    if field_schema['type'] == 'related':
                        if obj._schema['related_schema_name'] in field_schema['related_schema']:
                            related_model_fields.append([model_name, field_name])
                            break
            except:
                pass
        
        # Find any specific instances of related objects that explicitly reference
        # the target obj and add their uris to the list
        for entry in related_model_fields:
            query_kwargs = {entry[1]:obj.resource_uri}
            qs = self.query(entry[0],  **query_kwargs)
            for instance in qs:
                if (instance.model_name, instance.resource_uri) not in related_uris:
                    related_uris.append((instance.model_name, instance.resource_uri))
        return related_uris

    def prepare_db(self, *args, **kwargs):

        """ Prepare the DB system to begin working. """

        
        # Confirm directory exists
        db_folder_path = pathlib.Path(orm_settings.DB_FOLDER_PATH)
        if not db_folder_path.exists():
            db_folder_path.mkdir(
                parents=True, exist_ok=True)

        # Make sure a private key for validating db integrity has been created
        signing_key_path = orm_settings.PICKLE_SIGNING_KEY_FILEPATH
        if not signing_key_path.exists():
            key = str(uuid.uuid4())
            with signing_key_path.open('w+') as f:
                f.write(key)

        # Make sure the JSON needed by DataSyncWorker.java
        # is created
        user_json_path = pathlib.Path(
            db_relative_path(orm_settings.DATA_SYNC_WORKER_JSON))
        if not user_json_path.exists():
            with user_json_path.open('+w') as fp:
                json.dump(self.default_user_dict, fp, indent=2)

    @property
    def _db_index(self):
        return self.read(orm_settings.INDEX_FILENAME) or get_empty_db_index()

    def save(self, model_name, data):
        pass

    def sync_to_remote_server(self, *args, return_data = False, **kwargs):

        """ Perform the initial setup to sync local DB to the remote server. """
        try:
            logged_in_user = kwargs.pop('logged_in_user', self.logged_in_user)
            self.__fetch_api_schema(logged_in_user=logged_in_user)
            full_remote_data = self.fetch_all_data(user=logged_in_user)
            self.cursor.initial_db_sync(full_remote_data) # Write data to file
            if not return_data:
                return True
            else:
                return full_remote_data

        except:
            write_to_log("Failed to sync with remote server.",
                level = 'error', include_traceback = True)
            return False

    def login(self, user_dict, *args, **kwargs):
        """ Register the currently logged in user and get their data """

        try:
            self.write(orm_settings.LOGGED_IN_USER_FILENAME, user_dict,
                json_duplicate=True).join()
            self.update_user_json(user_dict)
            return self.sync_to_remote_server(logged_in_user = user_dict)
        except:
            write_to_log('Error Logging In User', include_traceback = True)
            return False

    @background_thread
    def update_user_json(self, user_dict, *args, **kwargs):
        user_dict = copy.copy(user_dict)
        user_dict['remote_url'] = orm_settings.REMOTE_URL
        user_json_path = pathlib.Path(
            db_relative_path(orm_settings.DATA_SYNC_WORKER_JSON))
        with user_json_path.open('+w') as fp:
            json.dump(user_dict, fp, indent=2)


    def model_name_from_data(self, data):
        """ Determine the model a particular dictionary represents based on the names of
        the fields present in the dictionary. """

        full_schema = self.get('schema')
        possible_models = list(full_schema.keys())

        # Essentially, search through all the schemas in the db. If any of the fields
        # don't match the keys in the data, remove the model from the list of possible
        # ones that match the data
        for model_name, model_schema in full_schema.items():
            try:
                for field_name, field_schema in model_schema['fields'].items():
                    if field_name not in data:
                        possible_models.remove(model_name)
                        break
            except:
                possible_models.remove(model_name)

        if len(possible_models) == 1:
            return possible_models[0]
        return None

    def query(self, *args, **kwargs):
        """ Legacy method kept for compatibility. New code should use
        TastyPieMemStore.db_filter()
         """
        return self.db_filter(*args, **kwargs) 

    def db_filter(self, *args, **kwargs):
        return self.cursor.filter(*args, **kwargs)

    def read(self, file_path):
        thread = ReadOperation(run_kwargs = {'file_path':file_path})
        return thread.join()

    def refresh_related_objects(self, obj, *args):

        related_uris = self.get_related_objects(obj)
        
        # Retrieve data for the end points
        endpoint_list = [(uri[0], join_api_path(self.remote_url, uri[1], append_slash = True)) for uri in related_uris]
        remote_data = asyncio.run(fetch_data_batch(endpoint_list,
            include_model_name = True))

        for model_name, data in remote_data:
            if self._data_is_valid(model_name, data):
                self.update_object(model_name, data)

        self.store_sync(force = True)

    def register_schema(self, schema = {}):
        """ Register the databse schema currently in use. """
        self.model_registry.set_schema(schema = schema)

    def update_object(self, model_name, data, commit = False):
        db_record = self.get(model_name)
        for index, obj_json in enumerate(db_record['objects']):
            if obj_json['resource_uri'] == data['resource_uri']:
                db_record['objects'][index] = data
                break
        self.put(model_name, db_record)
        self.updated_model = data

    def write(self, file_path, data, *args, json_duplicate=False, **kwargs):
        thread = WriteOperation(
            run_kwargs={'data':data, 'file_path':file_path,
                'json_duplicate':json_duplicate})
        return thread

    @property
    def _last_synch_time(self):
        """ Returns the last time data was synched with remote server. """

        last_known_time = self.get('last_synch_time')
        if last_known_time:
            return datetime.datetime.strptime(last_known_time, '%d/%m/%y')

    def __fetch_api_schema(self, *args, **kwargs):
        """ Fetch latest schema information from remote server. """
        user_dict = kwargs.get('logged_in_user', self.logged_in_user)
        #Retrieve and store the latest api schema's available
        api_info = asyncio.run(async_get(self.api_base_url))
        schema_list = []
        for endpoint_name, endpoint_info in api_info.items():
            schema_list.append((endpoint_name,
                join_api_path(self.remote_url, endpoint_info['schema'])))

        
        headers = self.get_api_request_header(user_dict = user_dict)
        all_model_schema = asyncio.run(fetch_all_schema(schema_list,
            headers = headers))



        #Determine models to keep synched and correctly format tha schema
        processed_schema = {'api_info':api_info}
        for model_schema in all_model_schema:
            #Convert the list of all schema information into a dictionary
            processed_schema[model_schema['model_name']] = model_schema

        #Store this information in the local DB
        self.register_schema(schema = processed_schema)

    def __resolve_query(self, objects, **query_kwargs):
        """ Resolve queries based on key word arguments provided. """
        
        filtered_objects = []
        for obj in objects:
            bad_match = False 

            # Go through each search term provided. If any term is not found,
            # consider this object a bad match
            for query_term, query_value in query_kwargs.items():

                if '__in' in query_term:
                    query_term = query_term.replace('__in', '')
                    if obj[query_term].lower() not in query_value:
                        bad_match = True

                else:
                    if isinstance(query_value, str):
                        try:
                            if not query_value.lower() in obj[query_term].lower():
                                bad_match = True
                        except:
                            bad_match = True
                    else:
                        try:
                            if obj[query_term] != query_value:
                                bad_match = True
                        except:
                            bad_match = True
                        
            # Only select this object is every term in the search query returned
            # a positive match result
            if not bad_match:
                filtered_objects.append(obj)

        return filtered_objects

    def _data_is_valid(self, model_name, data):
        """ Verifies that the data passed is a valid JSON object for the model in question. """
        full_schema = self.get('schema')
        model_schema = full_schema[model_name]
        try:
            for field_name, field_schema in model_schema['fields'].items():
                if field_name not in data:
                    return False
            return True
        except:
            return False

    def _model_name_from_uri(self, uri):
        full_schema = self.get('schema')
        for model_name, model_schema in full_schema.items():
            regex_pattern = re.compile(f"/{model_name}/")
            if re.search(regex_pattern, uri):
                return model_name

    def _pre_create(self, model_name, data, *args, **kwargs):
        """ Preprocessing of the data before POST to remote. """

        #Add some extra information
        data['created'] = datetime.datetime.now()     
        data['user'] = self.data_user['resource_uri']  
        self._sanitize_obj_fields(data)
        return data
            
    def _sanitize_obj_fields(self, data):
        """ Convert all fields into the correct format required by TastyPie """

        for k,v in data.items():
            if isinstance(v, datetime.datetime):
                data[k] = v.strftime(REMOTE_DATETIME_FORMAT)
            elif isinstance(v, datetime.date):
                data[k] = v.strftime(REMOTE_DATE_FORMAT)

    def _set_last_synch_time(self):
        """ Sets last time data was synched with remote server. """

        self.put('last_synch_time',
            datetime.datetime.now().strftime('%d/%m/%y'))