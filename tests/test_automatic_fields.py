"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import HarvestBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class HarvestAutomaticFields(MinimumSelectionTest, HarvestBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    start_date = "2025-02-01T00:00:00Z"

    @staticmethod
    def name():
        return "tap_tester_harvest_automatic_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names().difference(
            self.get_child_streams()
        )
