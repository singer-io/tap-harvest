import os
from tap_tester.base_suite_tests.base_case import BaseCase


class HarvestBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    start_date = "2017-01-01T00:00:00Z"
    PARENT_TAP_STREAM_ID = "parent-tap-stream-id"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-harvest"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.harvest"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "projects": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "clients": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "contacts": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "estimate_item_categories": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "estimate_line_items": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "estimates",
            },
            "estimate_messages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "estimates",
            },
            "estimates": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "expense_categories": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "expenses": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 2,
            },
            "external_reference": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "time_entries",
            },
            "invoice_item_categories": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "invoice_line_items": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "invoices",
            },
            "invoice_messages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "invoices",
            },
            "invoice_payments": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "invoices",
            },
            "invoices": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "project_tasks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 5,
            },
            "project_users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
            },
            "roles": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "tasks": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
            "time_entries": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 5,
            },
            "time_entry_external_reference": {
                cls.PRIMARY_KEYS: {"time_entry_id", "external_reference_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "time_entries",
            },
            "user_projects": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 1,
                cls.PARENT_TAP_STREAM_ID: "users",
            },
            "user_project_tasks": {
                cls.PRIMARY_KEYS: {"user_id", "project_task_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 5,
                cls.PARENT_TAP_STREAM_ID: "user_projects",
            },
            "user_roles": {
                cls.PRIMARY_KEYS: {"role_id", "user_id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: "roles",
            },
            "users": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"updated_at"},
                cls.OBEYS_START_DATE: True,
                cls.API_LIMIT: 100,
            },
        }

    @classmethod
    def get_child_streams(cls):
        """Return all streams that have a parent stream (child streams).
        These streams may be absent from the catalog if their parent is
        inaccessible, so they should be excluded from sync-based tests."""
        return {
            stream_name
            for stream_name, props in cls.expected_metadata().items()
            if props.get(cls.PARENT_TAP_STREAM_ID)
        }

    @staticmethod
    def get_child_streams_with_no_replication_keys():
        return {
            "user_roles",
            "invoice_line_items",
            "estimate_line_items",
            "user_project_tasks",
            "external_reference",
            "time_entry_external_reference",
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "client_id": "TAP_HARVEST_CLIENT_ID",
            "client_secret": "TAP_HARVEST_CLIENT_SECRET",
            "refresh_token": "TAP_HARVEST_REFRESH_TOKEN",
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {"start_date": self.start_date, "account_name": "Stitch"}

        return return_value
