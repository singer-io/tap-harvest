from base import HarvestBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class HarvestStartDateTest(StartDateTest, HarvestBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_harvest_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "invoice_messages",
            "invoice_payments",
            "estimate_messages",
            "users",
            "time_entries",
            "expenses",
        }.union(HarvestBaseTest.get_child_streams_with_no_replication_keys())
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2022-03-25T00:00:00Z"

    @property
    def start_date_2(self):
        return "2025-10-04T00:00:00Z"
