from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class ProjectTasks(IncrementalStream):
    tap_stream_id = "project_tasks"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "task_assignments"
    path = "task_assignments"
    object_to_id = ["project", "task"]
