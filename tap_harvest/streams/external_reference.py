from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class External_reference(IncrementalStream):
    tap_stream_id = "external_reference"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "external_reference"
    path = "external_reference"
    parent = "time_entries"


    def sync(
        self,
        state: Dict,
        schema: Dict,
        stream_metadata: Dict,
        transformer: Transformer,
        selected_streams: List,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        pass