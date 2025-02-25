from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Users(IncrementalStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "users"
    path = "users"
    children = ["user_projects"]
