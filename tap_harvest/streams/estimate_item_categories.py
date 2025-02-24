from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Estimate_item_categories(IncrementalStream):
    tap_stream_id = "estimate_item_categories"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "estimate_item_categories"
    path = "estimate_item_categories"
