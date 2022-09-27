from base import BaseTapTest
from harvest_api import set_up, tear_down
from tap_tester import menagerie


class HarvestParentChildIndependentTest(BaseTapTest):
    """Test that child steam sync without selecting parent stream"""

    @classmethod
    def setUpClass(cls):
        set_up(cls)

    @classmethod
    def tearDownClass(cls):
        tear_down(cls)

    def name(self):
        return "tap_tester_harvest_parent_child_independent"

    def do_test(self, conn_id):
        """
        Testing that tap is working fine if only child streams are selected
        • Verify that if only child streams are selected then only child streams are replicated.
        """
        child_streams = {
            "user_roles",
            "user_projects",
            "invoice_messages",
            "invoice_payments",
            "estimate_messages",
            "estimate_line_items",
            "external_reference",
            "time_entry_external_reference",
        }

        # Run check mode
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [
            catalog
            for catalog in found_catalogs
            if catalog.get("stream_name") in child_streams
        ]

        self.select_all_streams_and_fields(
            conn_id,
            test_catalogs_automatic_fields,
            select_all_fields=False,
        )

        # Run initial sync
        record_count_by_stream = self.run_sync(conn_id)

        # Verify no unexpected streams were replicated
        synced_stream_names = set(record_count_by_stream.keys())
        self.assertSetEqual(child_streams, synced_stream_names)
