import os
from kivy_tastypie.config import orm_settings
from kivy_tastypie.utils import write_to_log

def db_relative_path(path_str):
    return os.path.join(orm_settings.DB_FOLDER_PATH, path_str)

def add_model_resource_to_index(db_index, resource_json, model_name):
    """ Add each model instance to the database index.
    db_index -  Defaultdict that automatically creates dictionaries when a key doesn't exist.
                The db_index keys are all model names, and each value is another dictionry.
    """
    indexed_model_fields = db_index[model_name].keys()
    for field_name in indexed_model_fields:
        # Not all resources are guaranteed to have all the fields we want to
        # index, resulting in KeyErrors
        try:
            index_value = resource_json[field_name]
            db_index[model_name][field_name].append(index_value)
        except KeyError:
            pass

def get_indexed_model_fields(schema):
    # Lists the fields that should be stored in the index
    indexed_fields = ['verbose_name', 'resource_uri']
    if 'cage_reference' in schema['fields'].keys():
        indexed_fields.append('cage_reference')
    return indexed_fields


def initialize_index_with_model(db_index, model_name):
    """ Sets up the index to work with the given model name"""
    store = orm_settings.active_store
    indexed_model_fields = get_indexed_model_fields(
        store.model_registry.get_schema(model_name))
    for field_name in indexed_model_fields:
        if field_name not in db_index[model_name]:
            db_index[model_name][field_name] = []

def remove_model_resource_from_index(db_index, model_name, resource_uri):
    indexed_uris = db_index[model_name]['resource_uri']
    resource_found = False
    for target_index, item in enumerate(indexed_uris):
        if item == resource_uri:
            resource_found = True
            break
            
    # Remove it from the index
    if resource_found:
        for index_field_name in db_index[model_name].keys():
            db_index[model_name][index_field_name].pop(target_index)


def add_object_to_index(obj):
    db_index = orm_settings.active_store._db_index
    add_model_resource_to_index(db_index, obj._obj, obj.model_name)
    db_index.save()

def remove_object_from_index(obj):
    db_index = orm_settings.active_store._db_index
    remove_model_resource_from_index(
        db_index, obj.model_name, obj.resource_uri)
    db_index.save()

def uri_to_model_name(resource_uri):
    return resource_uri.split('/')[2]

def uri_to_file_path(resource_uri):
    file_path_base = resource_uri[1:-1].replace('/', '.')
    return f"{file_path_base}.pdb"

def get_pickle_filename(model_name):
    return f"{model_name}.pdb"
