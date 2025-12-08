from singer import get_logger
from tap_harvest.streams.abstracts import ParentBaseStream

LOGGER = get_logger()


class TimeEntries(ParentBaseStream):
    tap_stream_id = "time_entries"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "time_entries"
    path = "time_entries"
    object_to_id = [
        "user",
        "user_assignment",
        "client",
        "project",
        "task",
        "task_assignment",
        "external_reference",
        "invoice",
    ]
    children = ["external_reference", "time_entry_external_reference"]
