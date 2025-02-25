from typing import Dict, Any
from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class InvoiceMessages(IncrementalStream):
    tap_stream_id = "invoice_messages"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "invoices/{}/messages"
    data_key = "invoice_messages"
    parent = "invoices"
    support_filter = False
    bookmark_value = None

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        record["invoice_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
