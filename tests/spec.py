import os
import datetime

class TapSpec():
    """ Base class to specify tap-specific configuration. """

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = 100
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    OBEYS_START_DATE = "obey-start-date"
    RECORD_REPLICATION_KEY_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    start_date = ""

    DEFAULT_START_DATE = datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%dT00:00:00Z")

    @staticmethod
    def tap_name():
        """The name of the tap"""
        return "harvest"

    @staticmethod
    def get_type():
        """the expected url route ending"""
        return "platform.harvest"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        properties = {"account_name": os.environ['TAP_HARVEST_ACCOUNT_NAME'],
                      'start_date': self.DEFAULT_START_DATE}

        if original:
            return properties

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > properties["start_date"]

        properties["start_date"] = self.start_date
        return properties

    def get_credentials(self):
        return_val = {"client_id": os.environ["TAP_HARVEST_CLIENT_ID"],
                      "client_secret": os.environ["TAP_HARVEST_CLIENT_SECRET"],
                      "refresh_token":  os.environ["TAP_HARVEST_REFRESH_TOKEN"]}
        return return_val

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""

        default = {
                self.REPLICATION_KEYS: {"updated_at"},
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.OBEYS_START_DATE: True,
                self.API_LIMIT: 100}

        default_no_replication_key = {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL,
                self.API_LIMIT: 100}

        user_role = default_no_replication_key.copy()
        user_role[self.PRIMARY_KEYS] = {"role_id", "user_id"}

        time_entry = default_no_replication_key.copy()
        time_entry[self.PRIMARY_KEYS] = {"time_entry_id", "external_reference_id"}

        user_project = default_no_replication_key.copy()
        user_project[self.PRIMARY_KEYS] = {"user_id", "project_task_id"}

        return {"projects": default,
                "clients": default,
                "contacts": default,
                "estimate_item_categories": default,
                "estimate_line_items": default_no_replication_key,
                "estimate_messages": default,
                "estimates": default,
                "expense_categories": default,
                "expenses": default,
                "external_reference": default_no_replication_key,
                "invoice_item_categories": default,
                "invoice_line_items": default_no_replication_key,
                "invoice_messages": default,
                "invoice_payments": default,
                "invoices": default,
                "project_tasks": default,
                "project_users": default,
                "roles": default,
                "tasks": default,
                "time_entries": default,
                "time_entry_external_reference": time_entry,
                "user_project_tasks": user_project,
                "user_projects": default,
                "user_roles": user_role,
                "users": default}

            # "metafields": meta,
            # "transactions": {
            #     self.REPLICATION_KEYS: {"created_at"},
            #     self.PRIMARY_KEYS: {"id"},
            #     self.FOREIGN_KEYS: {"order_id"},
            #     self.REPLICATION_METHOD: self.INCREMENTAL,
            #     self.API_LIMIT: 250}
