from typing import Dict
from tap_harvest.streams.abstracts import ChildBaseStream


class UserProjects(ChildBaseStream):
    tap_stream_id = "user_projects"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "project_assignments"
    path = "users/{}/project_assignments"
    object_to_id = ["project", "client", "user"]
    parent = "users"
    children = ["user_project_tasks"]
    support_filter = False
    bookmark_value = None

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record["user"] = parent_record
        record = super().modify_object(record, parent_record)
        return record
