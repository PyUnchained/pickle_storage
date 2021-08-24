import threading
import pickle
import queue
import itertools
import functools

from kivy_tastypie.utils import write_to_log, Timer
from kivy_tastypie.config import orm_settings
from kivy_tastypie.store.mixins import HMACMixin
from kivy_tastypie.store.db.utils import db_relative_path, initialize_index_with_model

class BaseWorkerThread(threading.Thread):
    id_iterator = itertools.count()

    def __init__(self, **kwargs):
        """ Workers by default always run as a daemon and begin working as soon as they
        are instantiated. """
        worker_id = next(self.id_iterator)
        kwargs['name'] = f"{self.__class__.__name__}-{worker_id}"
        self.lock = kwargs.pop('lock', None)
        super().__init__(daemon = True)
        self.run_kwargs = kwargs
        self.start()

    def run(self, **kwargs):
        """ Standard runtime of a worker simply executes the task described by the 
        'do_work' method of the class. """
        # with Timer(f'{self.__class__.__name__}'):
        self.do_work(**self.run_kwargs)

    def do_work(self, **kwargs):
        """ Perform whatever task the worker's designed to perform"""
        raise NotImplemented()

    @classmethod
    def spawn_workers(cls, count = 1, **kwargs):
        worker_list = []
        kwargs['lock'] = threading.Lock()
        for i in range(count):
            worker = cls(**kwargs)
            worker_list.append(worker)
        return worker_list

class DataAdaptor(BaseWorkerThread):
    """ An adaptor takes some arbitrary raw data and applies some transformation to it. """

    def __init__(self, **kwargs):
        self.work_generator = kwargs.pop('work_generator')
        super().__init__(**kwargs)

    @classmethod
    def chunk_data(cls, data):
        for item in [data]:
            yield {'write_path': 'data.pdb', 'data': item}

    def do_work(self, data = [], out_queue = None,
        indexing_queue = None, db_index = None, **kwargs):
        while True:
            try:
                with self.lock: # Generator instance shared between all threads
                    item = next(self.work_generator)
                out_queue.put(item)
                indexing_queue.put(item)
            except StopIteration:
                break

    @classmethod
    def get_output_generator(cls, data):
        return cls.chunk_data(data)

    @classmethod
    def spawn_workers(cls, count = 4, **kwargs):
        worker_list = []
        work_generator = cls.get_output_generator(kwargs['data'])
        kwargs['work_generator'] = work_generator
        kwargs['lock'] = threading.Lock()
        for i in range(count):
            worker = cls(** kwargs)
            worker_list.append(worker)
        return worker_list

class Pickler(HMACMixin, BaseWorkerThread):
    """ Takes any number of arbitrary python objects and pickles them to a file"""

    def __init__(self, **kwargs):
        self.in_queue = kwargs.pop('in_queue') # Should always have an input queue
        super().__init__(**kwargs)

    def do_work(self, **kwargs):
        if not self.in_queue:
            raise ValueError('Failed to provide input queue.')
        self.empty_queue()
                    
    def empty_queue(self):
        """ Work through all available items in the input queue. """

        while True:
            try:
                work_item = self.in_queue.get(block = False)
                self.pickle_to_file(work_item)
                self.in_queue.task_done()

            # Return once queue empty
            except queue.Empty:
                return True

    def pickle_to_file(self, work_item):
        """ Write content to file. The first 32 bytes of data represent a unique digest 
        to validate data integrity."""

        content = pickle.dumps(work_item['data'])
        digest = self.hmac_digest(content)
        with open(db_relative_path(work_item['write_path']), "wb") as f:
            f.write(digest)
            f.write(bytearray(1))
            f.write(content)

class PickleReader(BaseWorkerThread):

    def __init__(self, **kwargs):
        self.file_path = kwargs.pop('file_path') # Should always have an input queue
        super().__init__(**kwargs)

class IndexSearchWorker(BaseWorkerThread):

    def do_work(self, in_queue = None, out_list = [], name='', **kwargs):

        # Get method called to evaluate each item we'll search
        search_method = self.get_search_method(kwargs['search_operator'])

        while True:
            try:
                item = in_queue.get(block=False)
                if search_method(item, **kwargs):
                    out_list.append(item['resource_uri'])
                in_queue.task_done()
            except queue.Empty:
                return True

    def in_search(self, item, *args, **kwargs):
        """ Assumes that matching items' search_against key has a value
        contained in the given list. """
        return item[kwargs['search_against']] in kwargs['operator_data']

    def all_search(self, item, *args, **kwargs):
        """ Assumes every item is required. """
        return True

    def contain_search(self, item, *args, is_string = True, **kwargs):
        """ Assumes that matching items' search_against key contains
        the search_term string ."""
        if is_string:
            search_term = kwargs['search_term'].lower()
            try:
                item_value = item[kwargs['search_against']].lower()
            except:
                item_value = ''
        else:
            search_term = kwargs.get('search_term', None)
            item_value = item[kwargs['search_against']]

        if search_term:
            return search_term in item_value

    def exact_search(self, item, *args, **kwargs):
        search_term = kwargs.get('search_term', None)
        item_value = item[kwargs['search_against']]
        if search_term:
            return search_term == item_value

    def get_search_method(self, search_operator):
        """ Returns the method to use when executing a search. """

        for attr_name in dir(self):
            if search_operator == attr_name[:len(search_operator)]:
                return getattr(self, attr_name)


class RecordRetrievalWorker(BaseWorkerThread):

    def do_work(self, in_queue = None, **kwargs):
        while True:
            try:
                record_uri = in_queue.get(block = False)
                in_queue.task_done()

            # Return once queue empty
            except queue.Empty:
                return True