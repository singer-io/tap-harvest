from typing import Dict
from singer import Transformer, get_logger, write_record
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class TimeEntryExternalReference(IncrementalStream):
    tap_stream_id = "time_entry_external_reference"
    key_properties = ["time_entry_id", "external_reference_id"]
    replication_keys = ["updated_at"]
    data_key = "time_entry_external_reference"
    path = "time_entry_external_reference"
    parent = "time_entries"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        if parent_obj.get("external_reference"):
            time_entry_external_reference = {
                "time_entry_id": parent_obj["id"],
                "external_reference_id": parent_obj["external_reference"]["id"],
            }
            external_reference = transformer.transform(
                time_entry_external_reference, self.schema, self.metadata
            )
            write_record(self.tap_stream_id, external_reference)
