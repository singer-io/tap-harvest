from typing import Dict
from singer import Transformer, write_record
from tap_harvest.streams.abstracts import IncrementalStream


class UserRoles(IncrementalStream):
    tap_stream_id = "user_roles"
    key_properties = ["role_id", "user_id"]
    replication_method = "FULL_TABLE"
    replication_keys = None
    data_key = "user_roles"
    parent = "roles"
    path = "user_roles"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""
        for user_id in parent_obj["user_ids"]:
            user_roles = {"role_id": parent_obj["id"], "user_id": user_id}
            user_roles = transformer.transform(user_roles, self.schema, self.metadata)
            write_record(self.tap_stream_id, user_roles)
