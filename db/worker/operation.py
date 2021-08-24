import threading
import queue
import pickle
import time
import itertools
import traceback
from bisect import bisect_left
import asyncio
import json
import copy

from kivy_tastypie.utils import write_to_log, Timer, timeit, get_empty_db_index
from kivy_tastypie.store.mixins import HMACMixin
from kivy_tastypie.errors import DBOperationError
from kivy_tastypie.config import orm_settings
from kivy_tastypie.models.queryset import Queryset
from kivy_tastypie.store.db.utils import (uri_to_file_path, get_pickle_filename,
    add_object_to_index, remove_object_from_index, initialize_index_with_model,
    uri_to_model_name)
from kivy_tastypie.http_requests.asynchronous import multiple_get_requests
from kivy_tastypie.http_requests.synchronous import join_api_path

from .base import DataAdaptor, Pickler, IndexSearchWorker, RecordRetrievalWorker
from .bulk import BulkDataAdaptor, BulkIndexer
from kivy_tastypie.store.db.utils import db_relative_path


class DBOperation(threading.Thread):
    id_iterator = itertools.count()

    def __init__(self, *args, run_kwargs = {}, **kwargs):
        self.on_complete = kwargs.pop('on_complete', None)
        operation_id = next(self.id_iterator)
        kwargs['name'] = f"{self.__class__.__name__}-{operation_id}"
        self.run_kwargs = run_kwargs
        self._return = None
        kwargs['daemon'] = True
        super().__init__(*args, **kwargs)
        self.start()

    @property
    def store(self):
        return orm_settings.active_store

    def pre_operation(self, *args, **kwargs):
        pass
    
    def do_operation(self, *args, **kwargs):
        raise NotImplemented()

    def post_operation(self, *args, **kwargs):
        pass

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        if self.on_complete:
            self.on_complete(self._return)
        return self._return

    def run(self):
        self.pre_operation(**self.run_kwargs)
        self._return = self.do_operation(**self.run_kwargs)
        self.post_operation(self._return, **self.run_kwargs)

class WriteOperation(DBOperation):

    def do_operation(self, data = {}, file_path = None,
        json_duplicate = False, **kwargs):
        if not file_path:
            write_to_log('No file_path specified, write ignored',
                level = 'warning')
            return

        work_queue = queue.Queue(maxsize = 1)
        obj_to_write = {'write_path': file_path, 'data': data}
        work_queue.put(obj_to_write)
        Pickler.spawn_workers(count = 1, in_queue = work_queue)

        return (True, file_path, data)

class ReadOperation(HMACMixin, DBOperation):

    def do_operation(self, file_path = '', **kwargs):
        return self.read_file(file_path)

    def read_file(self, file_path):
        target_path = db_relative_path(file_path)
        attempts = 0
        while attempts < 3:
            unsafe_content = False
            try:
                # Read content
                with open(target_path, 'rb') as f:
                    digest = f.read(32) 
                    f.seek(33) # One null byte separates the digest from content
                    content = f.read()

                if self.is_safe(digest, content):
                    return pickle.loads(content)
                else:
                    unsafe_content = True

                time.sleep(0.05)
            except:
                pass
            attempts +=1

        if unsafe_content:
            write_to_log(f'Failed to read "{file_path}"'
                    ' digest did not match content.', level = 'warning')
        return None

class IndexSearchOperation(DBOperation):
    """ Search for models agains the in-memory DB index cache. """

    def do_operation(self, binary_search=False, **kwargs):
        if binary_search:
            return self.binary_search(**kwargs)
        else:
            return self.linear_search(**kwargs)

    def binary_search(self, model_name='', search_term='', **kwargs ):
        db_index = self.store._db_index
        search_list = db_index[model_name]['resource_uri']
        i = bisect_left(search_list, search_term)

        if i != len(search_list) and search_list[i] == search_term:
            pickle_filename = get_pickle_filename(model_name)
            existing_data = ReadOperation(
                run_kwargs={'file_path': pickle_filename}
            ).join()
            return Queryset.object_from_dict(model_name, existing_data[i]) 
        else:
            return None


    def linear_search(self, model_name = '', **kwargs ):
        db_index = self.store._db_index
        uri_search_queue = queue.Queue()
        matched_uris = []

        # Populate the search queue
        if db_index.is_valid:
            for data in db_index.iter_model(model_name):
                uri_search_queue.put(data)

        # Perform search
        IndexSearchWorker.spawn_workers(in_queue=uri_search_queue,
            out_list=matched_uris, **kwargs)
        uri_search_queue.join()
        return matched_uris

class InitializeDBOperation(DBOperation):

    def do_operation(self, data = {}, **kwargs):

        # Save everything to disk and prepare for creating db index
        work_queue = queue.Queue(maxsize = 20)
        indexing_queue = queue.Queue()

        # Initialize an empty index dictionary
        db_index = get_empty_db_index()
        for res_data in data:
            initialize_index_with_model(db_index,
                                        res_data['meta']['model_name'])

        BulkDataAdaptor.spawn_workers(out_queue = work_queue, data = data,
            indexing_queue = indexing_queue)
        Pickler.spawn_workers(in_queue = work_queue)
        work_queue.join() # Wait for everything to be written to file

        # Build DB Index
        indexer_threadpool = BulkIndexer.spawn_workers(
            in_queue=indexing_queue, db_index=db_index)
        self.join_pool(indexer_threadpool)
        db_index.save()

    def join_pool(self, thread_pool):
        for t in thread_pool:
            t.join()

class UpdateObjectOperation(DBOperation):

    def do_operation(self, obj=None, obj_json={}, **kwargs):

        pickle_filename = get_pickle_filename(obj.model_name)
        existing_data = ReadOperation(
            run_kwargs={'file_path': pickle_filename}
        ).join()

        # Find and replace the existing json with the new json
        for index, existing_obj in enumerate(existing_data):
            if existing_obj['resource_uri'] == obj.resource_uri:
                existing_data[index] = obj_json
                WriteOperation(run_kwargs={'file_path':pickle_filename,
                    'data':existing_data}).join()
                break

class UpdateURIListOperation(DBOperation):

    def _update_in_db(self, resp):
        if not isinstance(resp, dict):
            return
        UpdateRecordOperation(run_kwargs={'raw_json':resp})

    def do_operation(self, *args, uri_list=[], **kwargs):
        if not uri_list:
            return

        # Expand each URI into a full URL
        expanded_url_list = map(
            lambda x: join_api_path(orm_settings.REMOTE_URL,
                                    x, append_slash=True),
            uri_list)

        resp_list = asyncio.run(
            multiple_get_requests(
                list(expanded_url_list),
                headers=self.store.get_api_request_header())
            )

        for resp in resp_list:
            self._update_in_db(resp)

class UpdateRecordOperation(DBOperation):

    def do_operation(self, *args, raw_json={}, **kwargs):
        if not raw_json:
            return

        model_name = uri_to_model_name(raw_json['resource_uri'])
        pickle_filename = get_pickle_filename(model_name)
        existing_data = ReadOperation(
            run_kwargs={'file_path': pickle_filename}
        ).join()
        if existing_data:
            for index, existing_obj in enumerate(existing_data):
                if existing_obj['resource_uri'] == raw_json['resource_uri']:
                    existing_data[index] = raw_json
                    WriteOperation(run_kwargs={'file_path':pickle_filename,
                        'data':existing_data}).join()
                    break

class CreateRecordOperation(DBOperation):

    def do_operation(self, obj=None, **kwargs):

        # Append new data to the existing data
        pickle_filename = get_pickle_filename(obj.model_name)
        existing_data = ReadOperation(
            run_kwargs={'file_path': pickle_filename}
        ).join()
        if existing_data != None:
            existing_data.append(obj._obj)
            WriteOperation(run_kwargs={'file_path':pickle_filename,
                           'data':existing_data})
            add_object_to_index(obj)

class DeleteRecordOperation(DBOperation):

    def do_operation(self, obj=None, **kwargs):
        
        # Get existing data and locate the obj in question
        pickle_filename = get_pickle_filename(obj.model_name)
        existing_data = ReadOperation(
            run_kwargs={'file_path': pickle_filename}
        ).join()

        # Find and remove the record
        for index, existing_obj in enumerate(existing_data):
            if existing_obj['resource_uri'] == obj.resource_uri:
                existing_data.pop(index)
                WriteOperation(run_kwargs={'file_path':pickle_filename,
                    'data':existing_data}).join()
                remove_object_from_index(obj)
                break



class RetrieveRecordsOperation(DBOperation):

    def do_operation(self, uri_list = [], model_name = '', **kwargs):
        file_path = f"{model_name}.pdb"
        all_records = ReadOperation(
            run_kwargs = {'file_path':file_path}).join()

        if all_records:
            return filter(
                lambda rec: rec['resource_uri'] in (uri_list or []),
                all_records)
        else:
            return []