from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Invoice_line_items(IncrementalStream):
    tap_stream_id = "invoice_line_items"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "invoice_line_items"
    path = "invoice_line_items"
    parent = "invoices"


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
        for line_item in parent_obj['line_items']:
            line_item['invoice_id'] = parent_obj['id']
            if line_item['project'] is not None:
                line_item['project_id'] = line_item['project']['id']
            else:
                line_item['project_id'] = None
            line_item = transformer.transform(line_item, schema, stream_metadata)
            write_record(self.tap_stream_id, line_item)