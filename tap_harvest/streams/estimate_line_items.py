from typing import Dict, Any
from singer import Transformer, write_record
from tap_harvest.streams.abstracts import IncrementalStream


class EstimateLineItems(IncrementalStream):
    tap_stream_id = "estimate_line_items"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = None
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

    def write_bookmark(
        self, state: dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return state
