from singer import get_logger
from tap_harvest.streams.abstracts import ParentBaseStream

LOGGER = get_logger()


class Roles(ParentBaseStream):
    tap_stream_id = "roles"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "roles"
    path = "roles"
    children = ["user_roles"]
