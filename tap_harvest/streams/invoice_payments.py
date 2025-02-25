from typing import Dict, Any
from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class InvoicePayments(IncrementalStream):
    tap_stream_id = "invoice_payments"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "invoices/{}/payments"
    data_key = "invoice_payments"
    parent = "invoices"
    date_fields = ["send_reminder_on"]
    support_filter = False
    bookmark_value = None

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        record = super().modify_object(record, parent_record)
        record["invoice_id"] = parent_record["id"]
        record["payment_gateway_id"] = record["payment_gateway"]["id"]
        record["payment_gateway_name"] = record["payment_gateway"]["name"]
        return record

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
