from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ExpenseCategories(IncrementalStream):
    tap_stream_id = "expense_categories"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "expense_categories"
    path = "expense_categories"
