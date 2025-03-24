from singer import get_logger
from tap_harvest.streams.abstracts import ParentBaseStream

LOGGER = get_logger()


class Invoices(ParentBaseStream):
    tap_stream_id = "invoices"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "invoices"
    path = "invoices"
    children = ["invoice_payments", "invoice_messages", "invoice_line_items"]
