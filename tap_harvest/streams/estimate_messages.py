from typing import Dict, Any
from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EstimateMessages(IncrementalStream):
    tap_stream_id = "estimate_messages"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "estimate_messages"
    path = "estimates/{}/messages"
    parent = "estimates"
    date_fields = ["send_reminder_on"]
    support_filter = False
    bookmark_value = None

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        record["estimate_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record

    def get_bookmark(self, state: Dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not self.bookmark_value:        
            self.bookmark_value = super().get_bookmark(state, key)

        return self.bookmark_value
