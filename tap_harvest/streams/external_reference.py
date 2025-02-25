from typing import Dict
from singer import Transformer, get_logger, write_record
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ExternalReference(IncrementalStream):
    tap_stream_id = "external_reference"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
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
        external_reference = transformer.transform(
            external_reference, self.schema, self.metadata
        )
        write_record(self.tap_stream_id, external_reference)
