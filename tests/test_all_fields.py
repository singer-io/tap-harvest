from base import HarvestBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class HarvestAllFields(AllFieldsTest, HarvestBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

    MISSING_FIELDS = {
        "users": [
            "is_admin",
            "can_create_invoices",
            "is_project_manager",
            "can_see_rates",
        ]
    }

    start_date = "2025-02-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_harvest_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = {"external_reference", "time_entry_external_reference"}
        return self.expected_stream_names().difference(streams_to_exclude)
