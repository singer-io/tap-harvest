from tap_harvest.streams.abstracts import IncrementalStream


class ProjectUsers(IncrementalStream):
    tap_stream_id = "project_users"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "user_assignments"
    path = "user_assignments"
    object_to_id = ["project", "user"]
