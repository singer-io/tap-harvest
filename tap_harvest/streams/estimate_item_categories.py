from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EstimateItemCategories(IncrementalStream):
    tap_stream_id = "estimate_item_categories"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "estimate_item_categories"
    path = "estimate_item_categories"
