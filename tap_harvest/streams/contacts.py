from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Contacts(IncrementalStream):
    tap_stream_id = "contacts"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "contacts"
    path = "contacts"
    object_to_id = ["client"]
