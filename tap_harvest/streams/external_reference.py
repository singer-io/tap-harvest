from typing import Dict, Any
from singer import Transformer, get_logger, write_record
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ExternalReference(IncrementalStream):
    tap_stream_id = "external_reference"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = None
    data_key = "external_reference"
    path = "external_reference"
    parent = "time_entries"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        external_reference = parent_obj["external_reference"]
        if external_reference:
            external_reference = transformer.transform(
                external_reference, self.schema, self.metadata
            )
            write_record(self.tap_stream_id, external_reference)

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return state
