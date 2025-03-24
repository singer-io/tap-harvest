from typing import Dict
from tap_harvest.streams.abstracts import ChildBaseStream


class InvoiceMessages(ChildBaseStream):
    tap_stream_id = "invoice_messages"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "invoice_messages"
    path = "invoices/{}/messages"
    parent = "invoices"
    support_filter = False
    bookmark_value = None

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record["invoice_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
