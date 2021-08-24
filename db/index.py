from collections import defaultdict
from itertools import zip_longest
from natsort import natsorted
from copy import copy



class DBIndex(defaultdict):

    indexed_fields = ['resource_uri', 'verbose_name']

    @property
    def is_valid(self):
        return True

    def iter_model(self, model_name):
        from kivy_tastypie.utils import write_to_log
        from kivy_tastypie.config import orm_settings
        """ Iterate through all the data related to a specific model using a generator.
        Each returned value if a list containing the resource_uri and verbose_ """
        model_exists = self[model_name] != {}
        if model_exists:
            total_length = len(self[model_name]['resource_uri'])
            field_names = list(self[model_name].keys())
            for i in range(total_length):
                model_data = {}
                for field_name in field_names:
                    model_data[field_name] = self[model_name][field_name][i]
                yield model_data
        else:
            if orm_settings.DEBUG:
                write_to_log(f'The "{model_name}" model cannot be found in '
                    "the store's index", level='warning')
            return []

    def save(self):
        from .worker.operation import WriteOperation
        from kivy_tastypie.config import orm_settings
        from kivy_tastypie.utils import write_to_log, Timer

        write_info = WriteOperation(
            run_kwargs={'data':self,
            'file_path': orm_settings.INDEX_FILENAME})

    def sort_index_entries(self):
        """ For each model entry in the index, sort the indexed fields
        according to the resource_uri field. """

        for model_name, db_entry_dict in self.items():
            
            # Get all the fields that make up the index entry for the model
            model_indexed_fields = copy(self.indexed_fields)
            for indexed_field_name in db_entry_dict:
                if indexed_field_name not in model_indexed_fields:
                    model_indexed_fields.append(indexed_field_name)

            # Sort the values using a zip to keep the relative order of all
            # key values is maintained
            zipped_list = natsorted(
                zip(*[db_entry_dict[name] for name in model_indexed_fields]),
                key=lambda x: x[0])

            # Recreate entry dictionary
            ordered_entry_dict = defaultdict(list)
            for sorted_tuple in zipped_list:
                for index, name in enumerate(model_indexed_fields):
                    ordered_entry_dict[name].append(sorted_tuple[index])
            self[model_name] = ordered_entry_dict

