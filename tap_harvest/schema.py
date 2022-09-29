import os
import json
from singer import metadata
from tap_harvest.streams import STREAMS


def get_abs_path(path):
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    """
    Load the schema references, prepare metadata for each streams and return
    schema and metadata for the catalog.
    """
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        # Load the schema files from the schema folder
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path, encoding='utf-8') as _file:
            schema = json.load(_file)
        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        #   https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md
        # Reference:
        #   https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=((hasattr(stream_metadata, 'key_properties') or None)
                            and stream_metadata.key_properties),
            valid_replication_keys=((hasattr(stream_metadata, 'replication_keys') or None)
                                    and stream_metadata.replication_keys),
            replication_method=((hasattr(stream_metadata, 'replication_method') or None)
                                and stream_metadata.replication_method))
        mdata_map = metadata.to_map(mdata)

        # Make replication keys of automatic inclusion
        for replication_key in stream_metadata.replication_keys:
            mdata_map[('properties', replication_key)]['inclusion'] = 'automatic'

        mdata = metadata.to_list(mdata_map)

        field_metadata[stream_name] = mdata

    return schemas, field_metadata
