from singer import get_logger
from tap_harvest.streams.abstracts import ParentBaseStream

LOGGER = get_logger()


class Estimates(ParentBaseStream):
    tap_stream_id = "estimates"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "estimates"
    path = "estimates"
    children = ["estimate_messages", "estimate_line_items"]
