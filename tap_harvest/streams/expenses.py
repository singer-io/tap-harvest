from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Expenses(IncrementalStream):
    tap_stream_id = "expenses"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "expenses"
    path = "expenses"
    object_to_id = [
        "client",
        "project",
        "expense_category",
        "user",
        "user_assignment",
        "invoice",
    ]

    def map_object(self, record: Dict) -> Dict:
        """
        Modify receipt object to be more easily accessible
        """
        if record["receipt"] is None:
            record["receipt_url"] = None
            record["receipt_file_name"] = None
            record["receipt_file_size"] = None
            record["receipt_content_type"] = None
        else:
            record["receipt_url"] = record["receipt"]["url"]
            record["receipt_file_name"] = record["receipt"]["file_name"]
            record["receipt_file_size"] = record["receipt"]["file_size"]
            record["receipt_content_type"] = record["receipt"]["content_type"]
        return record
