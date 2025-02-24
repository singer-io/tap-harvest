from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class User_roles(IncrementalStream):
    tap_stream_id = "user_roles"
    key_properties = ["role_id", "user_id"]
    replication_keys = ["updated_at"]
    data_key = "user_roles"
    path = "user_roles"
