from tap_tester.base_suite_tests.pagination_test import PaginationTest
from base import HarvestBaseTest


class HarvestPaginationTest(PaginationTest, HarvestBaseTest):
    """Ensure tap can replicate multiple pages of data for streams that use
    pagination."""

    @staticmethod
    def name():
        return "tap_tester_harvest_pagination_test"

    def streams_to_test(self):
        streams_to_exclude = {
            "invoice_messages",
            "invoice_payments",
            "estimate_messages",
            "users",
        }.union(HarvestBaseTest.get_child_streams_with_no_replication_keys())
        return self.expected_stream_names().difference(streams_to_exclude)
