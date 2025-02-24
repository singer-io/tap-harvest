from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Estimate_line_items(IncrementalStream):
    tap_stream_id = "estimate_line_items"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "estimate_line_items"
    path = "estimate_line_items"
