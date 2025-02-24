from typing import Dict, Iterator, List
from tap_harvest.streams.user_project_tasks import User_project_tasks

from singer import Transformer, get_logger, metrics, write_record, Catalog, metadata
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class User_projects(IncrementalStream):
    tap_stream_id = "user_projects"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "project_assignments"
    path = "users/{}/project_assignments"
    object_to_id = ["project", "client", "user"]
    parent = "users"
    children = ["user_project_tasks"]

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

        current_max_bookmark_date = bookmark_date = self.get_bookmark(state)

        self.url_endpoint = (
            f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"
        )
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record["user"] = parent_obj
                record = self.add_object_to_id(record)
                self.remove_empty_date_times(record, schema)

                transformed_record = transformer.transform(
                    record, schema, stream_metadata
                )

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

                if "user_project_tasks" in selected_streams:
                    user_projects_tasks = User_project_tasks(self.client)
                    user_projects_tasks.sync(
                        state=state,
                        schema=schema,
                        stream_metadata=stream_metadata,
                        transformer=transformer,
                        parent_obj=record,
                    )

            state = self.write_bookmark(state, value=current_max_bookmark_date)
            return counter.value
