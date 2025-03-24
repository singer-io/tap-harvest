from tap_harvest.streams.abstracts import IncrementalStream


class ExpenseCategories(IncrementalStream):
    tap_stream_id = "expense_categories"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "expense_categories"
    path = "expense_categories"
