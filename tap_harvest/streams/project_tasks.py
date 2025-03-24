from tap_harvest.streams.abstracts import IncrementalStream


class ProjectTasks(IncrementalStream):
    tap_stream_id = "project_tasks"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "task_assignments"
    path = "task_assignments"
    object_to_id = ["project", "task"]
