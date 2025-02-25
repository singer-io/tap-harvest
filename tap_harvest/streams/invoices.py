from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Invoices(IncrementalStream):
    tap_stream_id = "invoices"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "invoices"
    path = "invoices"
    object_to_id = ["client", "estimate", "retainer", "creator"]
    children = ["invoice_payments", "invoice_messages", "invoice_line_items"]
