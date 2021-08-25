import os
import json
from singer import metadata
from tap_harvest.streams import STREAMS

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:#pylint: disable=unspecified-encoding
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.key_properties,
            valid_replication_keys=stream_metadata.valid_replication_keys,
            replication_method=stream_metadata.replication_method
        )

        # Add additional metadata
        mdata_map = metadata.to_map(mdata)

        for replication_key in stream_metadata.valid_replication_keys:
            mdata_map = metadata.write(
                    mdata_map,
                    ('properties', replication_key),
                    'inclusion',
                    'automatic')

        mdata = metadata.to_list(mdata_map)

        field_metadata[stream_name] = mdata

    return schemas, field_metadata
