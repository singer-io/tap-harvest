from typing import Dict, Any
from singer import Transformer, write_record
from tap_harvest.streams.abstracts import IncrementalStream


class UserProjectTasks(IncrementalStream):
    tap_stream_id = "user_project_tasks"
    key_properties = ["user_id", "project_task_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["user_projects_updated_at"]
    data_key = "user_project_tasks"
    path = "user_project_tasks"
    parent = "user_projects"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        for project_task in parent_obj["task_assignments"]:
            record = {
                "user_id": parent_obj["user_id"],
                "project_task_id": project_task["id"],
            }
            write_record(self.tap_stream_id, record)

    def write_bookmark(
        self, state: dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return state
