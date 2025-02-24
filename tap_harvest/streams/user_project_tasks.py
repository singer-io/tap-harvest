from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record, Catalog
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class User_project_tasks(IncrementalStream):
    tap_stream_id = "user_project_tasks"
    key_properties = ["user_id", "project_task_id"]
    replication_keys = ["updated_at"]
    data_key = "user_project_tasks"
    path = "user_project_tasks"
    parent = "user_projects"

    def sync(
        self,
        state: Dict,
        schema: Dict,
        stream_metadata: Dict,
        transformer: Transformer,
        selected_streams: List,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        record = {
            "user_id": parent_obj["user"]["id"],
            "project_task_id": parent_obj["id"],
        }
        write_record(self.tap_stream_id, record)
