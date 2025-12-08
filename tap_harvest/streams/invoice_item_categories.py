from tap_harvest.streams.abstracts import IncrementalStream


class InvoiceItemCategories(IncrementalStream):
    tap_stream_id = "invoice_item_categories"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "invoice_item_categories"
    path = "invoice_item_categories"
