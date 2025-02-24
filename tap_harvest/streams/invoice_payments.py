from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Invoice_payments(IncrementalStream):
    tap_stream_id = "invoice_payments"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "invoice_payments"
    path = "invoice_payments"
