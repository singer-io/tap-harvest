from typing import Dict
from tap_harvest.streams.abstracts import ChildBaseStream


class InvoicePayments(ChildBaseStream):
    tap_stream_id = "invoice_payments"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "invoice_payments"
    path = "invoices/{}/payments"
    parent = "invoices"
    date_fields = ["send_reminder_on"]
    support_filter = False
    bookmark_value = None

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = super().modify_object(record, parent_record)
        record["invoice_id"] = parent_record["id"]
        record["payment_gateway_id"] = record["payment_gateway"]["id"]
        record["payment_gateway_name"] = record["payment_gateway"]["name"]
        return record
