from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class TimeEntries(IncrementalStream):
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

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""

        min_parent_bookmark = super().get_bookmark(state, stream) if self.is_selected() else None
        for child in self.child_to_sync:
            if child.is_selected():
                bookmark_key = f"{self.tap_stream_id}_{self.replication_keys[0]}"
                child_bookmark = super().get_bookmark(state, child.tap_stream_id, key=bookmark_key)
                if min_parent_bookmark:
                    min_parent_bookmark = min(min_parent_bookmark, child_bookmark)
                else:
                    min_parent_bookmark = child_bookmark

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
