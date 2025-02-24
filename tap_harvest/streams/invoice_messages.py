from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Invoice_messages(IncrementalStream):
    tap_stream_id = "invoice_messages"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "invoice_messages"
    path = "invoice_messages"
