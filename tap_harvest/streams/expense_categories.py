from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Expense_categories(IncrementalStream):
    tap_stream_id = "expense_categories"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "expense_categories"
    path = "expense_categories"
