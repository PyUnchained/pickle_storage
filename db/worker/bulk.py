import queue
from .base import DataAdaptor, Pickler, BaseWorkerThread
from kivy_tastypie.store.db.utils import (uri_to_file_path, add_model_resource_to_index)
from kivy_tastypie.utils import write_to_log

class BulkDataAdaptor(DataAdaptor):

    @classmethod
    def chunk_data(cls, data):
        """Save the data for each model in a separate file. """

        for model_resp in data:
            # add_model_to_index(model_resp['meta']['model_name'])
            write_path = f"{model_resp['meta']['model_name']}.pdb"
            yield {'write_path': write_path, 'data': model_resp['objects'],
                'model_name' : model_resp['meta']['model_name']}

class BulkPickler(Pickler):
    pass

class BulkIndexer(BaseWorkerThread):
    """ Receives a list of objects from the API in JSON format and creates an index that will
    be used later to search. """

    def do_work(self, **kwargs):
        """ Work through all available items in the input queue. """
        in_queue = kwargs['in_queue']
        while True:
            try:
                work_item = in_queue.get(block = False)
                with self.lock:
                    for resource_data in work_item['data']:
                        add_model_resource_to_index(
                            kwargs['db_index'], resource_data,
                            work_item['model_name'])

                in_queue.task_done()

            # Return once queue empty
            except queue.Empty:
                break



