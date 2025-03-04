from typing import Dict, Any
from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Users(IncrementalStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "users"
    path = "users"
    children = ["user_projects"]

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = super().get_bookmark(state, stream) if self.is_selected() else self.client.config["start_date"]

        parent_bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
        user_project_obj = self.child_to_sync[0]
        user_projects_bookmark = super().get_bookmark(state, user_project_obj.tap_stream_id, key=parent_bookmark_key) if user_project_obj.is_selected() else self.client.config["start_date"]

        min_parent_bookmark = min(min_parent_bookmark, user_projects_bookmark)
        return min_parent_bookmark

    def write_bookmark(self, state: Dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if self.is_selected():
            super().write_bookmark(state, stream, value=value)

        for child in self.child_to_sync:
            if child.is_selected():
                bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
                super().write_bookmark(state, child.tap_stream_id, key=bookmark_key, value=value)
