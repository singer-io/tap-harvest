from singer import  get_logger

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Tasks(IncrementalStream):
    tap_stream_id = "tasks"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "tasks"
    path = "tasks"
