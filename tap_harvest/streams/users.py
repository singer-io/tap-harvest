from typing import Dict, Iterator, List
from tap_harvest.streams.user_projects import User_projects
from singer import Transformer, get_logger, metrics, write_record, Catalog
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Users(IncrementalStream):
    tap_stream_id = "users"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "users"
    path = "users"
    children = ["user_projects"]

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
        self.update_filter_params(updated_since=bookmark_date)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.map_object(record)
                record = self.add_object_to_id(record)
                self.remove_empty_date_times(record, schema)

                transformed_record = transformer.transform(
                    record, schema, stream_metadata
                )
                self.append_times_to_dates(transformed_record)

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

                if "user_projects" in selected_streams:
                    user_projects = User_projects(self.client)
                    user_projects.sync(
                        state=state,
                        schema=schema,
                        stream_metadata=stream_metadata,
                        transformer=transformer,
                        selected_streams=selected_streams,
                        parent_obj=record,
                    )

            state = self.write_bookmark(state, value=current_max_bookmark_date)
            return counter.value
