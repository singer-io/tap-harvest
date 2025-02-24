from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Time_entry_external_reference(IncrementalStream):
    tap_stream_id = "time_entry_external_reference"
    key_properties = ["time_entry_id", "external_reference_id"]
    replication_keys = ["updated_at"]
    data_key = "time_entry_external_reference"
    path = "time_entry_external_reference"
