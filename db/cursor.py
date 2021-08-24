from queue import Queue  # or queue in Python 3
import datetime
import pickle
import functools

from kivy_tastypie.models.queryset import Queryset
from kivy_tastypie.store.db.utils import uri_to_file_path
from kivy_tastypie.utils import background_thread, write_to_log, Timer
from .worker.operation import (InitializeDBOperation, WriteOperation, IndexSearchOperation,
    RetrieveRecordsOperation)


class SimpleCursor():
    """ Class used to read to and write from the database. """

    def __init__(self, store, *args, **kwargs):
        self.store = store

    def get(self, model_name, resource_uri):
        get_kwargs = {"model_name":model_name, "search_term":resource_uri,
            "binary_search":True}
        return IndexSearchOperation(run_kwargs=get_kwargs).join()

    def filter(self, model_name, *search_terms, **kwargs):
        kwargs['model_name'] = model_name
        kwargs = self.set_filter_parameters(search_terms, kwargs)
        uri_list = IndexSearchOperation(run_kwargs=kwargs).join()
        return self.uris_to_queryset(uri_list, **kwargs)

    def set_filter_parameters(self, args_list, kwargs_dict):
        """ Update the dict with the correct parameters to perform th search,
        changing the dictionary in place. The following keys are modified:

        search_term - The string used during search
        search_operator - Operator to apply during search (all, contain, in)
        operator_key - String representing the key in the data dict used to apply
                       the operator
        operator_data - Data used when performing the operation
        """

        # Set default value for search_against key
        if 'search_against' not in kwargs_dict:
            kwargs_dict['search_against'] = 'resource_uri'

        search_term = None
        search_operator = None
        operator_key = None
        operator_data = None

        for k in kwargs_dict:
            if '__' in k:
                operator_key, search_operator = k.split('__')
                operator_data = kwargs_dict[k]
                kwargs_dict.pop(k)
                break
        

        # If there's an explicit search term...
        if len(args_list):
            search_term=args_list[0]

            # If an operator was not explicitly defined, set a suitable one
            if not search_operator:
                is_string = isinstance(search_term, str)
                if is_string:
                    search_operator = 'contain'
                else:
                    search_operator = 'exact'

        # Search operator will default to "all" if none selected so far
        kwargs_dict['search_operator'] = search_operator or 'all'
        kwargs_dict['search_term'] = search_term
        kwargs_dict['operator_key'] = operator_key
        kwargs_dict['operator_data'] = operator_data
        return kwargs_dict


    def uris_to_queryset(self, uri_list, qs_filter=None, model_name='',
        *args, **kwargs):
        """ Convert a list of object URIs into a Queryset."""
        
        file_paths = map(uri_to_file_path, map(lambda x:x[0], uri_list or []))
        db_records = RetrieveRecordsOperation(
            run_kwargs={'model_name':model_name,'uri_list':uri_list}).join()

        # Apply post-processing of the db records, if any
        if qs_filter:
            processed_qs = qs_filter(db_records, model_name = model_name)
        else:
            processed_qs = db_records

        # Return queryset object
        return Queryset(model_name, processed_qs, **kwargs)

    @background_thread
    def initial_db_sync(self, data):
        InitializeDBOperation(run_kwargs = {'data':data, 'store':self.store}).join()
