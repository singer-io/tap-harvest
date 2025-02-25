from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Estimates(IncrementalStream):
    tap_stream_id = "estimates"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "estimates"
    path = "estimates"
    children = ["estimate_messages", "estimate_line_items"]
    object_to_id = ["client", "creator"]
    date_fields = ["issue_date"]
