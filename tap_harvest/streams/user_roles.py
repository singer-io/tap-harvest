from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class UserRoles(IncrementalStream):
    tap_stream_id = "user_roles"
    key_properties = ["role_id", "user_id"]
    replication_keys = ["updated_at"]
    data_key = "user_roles"
    path = "user_roles"
