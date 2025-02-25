from typing import Dict
from singer import Transformer, get_logger, write_record
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EstimateLineItems(IncrementalStream):
    tap_stream_id = "estimate_line_items"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "estimate_line_items"
    path = "estimate_line_items"
    parent = "estimates"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        for line_item in parent_obj["line_items"]:
            line_item["estimate_id"] = parent_obj["id"]
            line_item = transformer.transform(line_item, self.schema, self.metadata)
            write_record(self.tap_stream_id, line_item)
