import datetime
import os


class TapSpec:
    """Base class to specify tap-specific configuration."""

    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL = "FULL_TABLE"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    OBEYS_START_DATE = "obey-start-date"
    RECORD_REPLICATION_KEY_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    start_date = ""

    DEFAULT_START_DATE = datetime.datetime.strftime(
        datetime.datetime.today(), "%Y-%m-%dT00:00:00Z"
    )

    # Given streams does not have their own replication keys
    # Uses respective parent's replication key value
    PARENT_REP_VALUE_STREAMS = {
        "invoice_line_items",
        "estimate_line_items",
        "user_project_tasks",
        "user_roles",
        "external_reference",
        "time_entry_external_reference",
    }

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "harvest"

    @staticmethod
    def get_type():
        """the expected url route ending."""
        return "platform.harvest"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""
        properties = {
            "account_name": os.environ["TAP_HARVEST_ACCOUNT_NAME"],
            "start_date": self.DEFAULT_START_DATE,
        }

        if original:
            return properties

        # This test needs the new connections start date to be larger than the default
        assert self.start_date > properties["start_date"]

        properties["start_date"] = self.start_date
        return properties

    def get_credentials(self):
        """Configure cerdentials required for the tap."""
        return_val = {
            "client_id": os.environ["TAP_HARVEST_CLIENT_ID"],
            "client_secret": os.environ["TAP_HARVEST_CLIENT_SECRET"],
            "refresh_token": os.environ["TAP_HARVEST_REFRESH_TOKEN"],
        }
        return return_val

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""

        default = {
            self.REPLICATION_KEYS: {"updated_at"},
            self.PRIMARY_KEYS: {"id"},
            self.REPLICATION_METHOD: self.INCREMENTAL,
            self.OBEYS_START_DATE: True,
            self.API_LIMIT: 100,
        }

        user_role = default.copy()
        user_role[self.PRIMARY_KEYS] = {"role_id", "user_id"}

        time_entry_external_reference = default.copy()
        time_entry_external_reference[self.PRIMARY_KEYS] = {
            "time_entry_id",
            "external_reference_id",
        }

        user_project_tasks = default.copy()
        user_project_tasks[self.PRIMARY_KEYS] = {"user_id", "project_task_id"}

        return {
            "projects": default,
            "clients": default,
            "contacts": default,
            "estimate_item_categories": default,
            "estimate_line_items": default,
            "estimate_messages": default,
            "estimates": default,
            "expense_categories": default,
            "expenses": default,
            "external_reference": default,
            "invoice_item_categories": default,
            "invoice_line_items": default,
            "invoice_messages": default,
            "invoice_payments": default,
            "invoices": default,
            "project_tasks": default,
            "project_users": default,
            "roles": default,
            "tasks": default,
            "time_entries": default,
            "time_entry_external_reference": time_entry_external_reference,
            "user_project_tasks": user_project_tasks,
            "user_projects": default,
            "user_roles": user_role,
            "users": default,
        }
