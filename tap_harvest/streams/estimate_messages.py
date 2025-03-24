from typing import Dict
from tap_harvest.streams.abstracts import ChildBaseStream


class EstimateMessages(ChildBaseStream):
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

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record["estimate_id"] = parent_record["id"]
        record = super().modify_object(record, parent_record)
        return record
