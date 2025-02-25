from typing import Dict
from singer import get_logger
from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class EstimateMessages(IncrementalStream):
    tap_stream_id = "estimate_messages"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "estimate_messages"
    path = "estimates/{}/messages"
    parent = "estimates"
    date_fields = ["send_reminder_on"]
    support_filter = False

    def get_url_endpoint(self, parent_obj=None):
        return f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """
        Modify the record before writing to the stream
        """
        record = super().modify_object(record, parent_record)
        record["estimate_id"] = parent_record["id"]
        return record
