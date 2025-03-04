from typing import Dict
from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Clients(IncrementalStream):
    tap_stream_id = "clients"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "clients"
    path = "clients"
