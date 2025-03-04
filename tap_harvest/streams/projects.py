from singer import get_logger

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Projects(IncrementalStream):
    tap_stream_id = "projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "projects"
    path = "projects"
    object_to_id = ["client"]
