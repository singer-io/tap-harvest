from base import BaseTapTest
from harvest_api import set_up, tear_down
from tap_tester import menagerie, runner

# Can not generate given fields while creating records
KNOWN_MISSING_FIELDS = {
    "external_reference": {
        "task_id",  # Field not available in docs and response
    },
    # Given fields are available in response but not written by tap
    "invoice_messages": {
        "send_reminder_on",
    },
    "invoices": {
        "paid_at",
        "paid_date",
        "period_end",
        "closed_at",
        "period_start",
    },
    "estimates": {
        "accepted_at",
        "declined_at",
    },
    "time_entries": {
        "timer_started_at",
    },
    "projects": {
        "over_budget_notification_date",
    },
    "user_projects": {
        "task_assignments_id"   # NOTE: Remove from map after dict-base implementatation
    },
}


class TestAllFields(BaseTapTest):
    """Test that all fields are received when all the fields are selected."""

    @classmethod
    def setUpClass(cls):
        set_up(cls)

    @classmethod
    def tearDownClass(cls):
        tear_down(cls)

    def name(self):
        return "tap_tester_harvest_all_fields"

    def do_test(self, conn_id):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream.
        • Verify all fields for each stream are replicated
        """

        expected_streams = self.expected_streams()

        # Run check mode
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [
            catalog
            for catalog in found_catalogs
            if catalog.get("stream_name") in expected_streams
        ]
        self.select_all_streams_and_fields(
            conn_id,
            test_catalogs_all_fields,
            select_all_fields=True,
        )

        # Grab metadata after performing table-and-field selection to set expectations
        stream_to_all_catalog_fields = dict() # used for asserting all fields are replicated
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog["stream_id"], catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [
                md_entry["breadcrumb"][1]
                for md_entry in catalog_entry["metadata"]
                if md_entry["breadcrumb"] != []
            ]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        # Run initial sync
        self.run_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_automatic_keys = self.expected_automatic_keys().get(stream)

                # Get all expected keys
                expected_all_keys = stream_to_all_catalog_fields[stream]
                messages = synced_records.get(stream)

                # Collect actual values
                actual_all_keys = set()
                for message in messages["messages"]:
                    if message["action"] == "upsert":
                        actual_all_keys.update(message["data"].keys())

                expected_all_keys = expected_all_keys - KNOWN_MISSING_FIELDS.get(
                    stream, set()
                )

                # Verify all fields for a stream were replicated
                if stream in [
                    "time_entry_external_reference",
                    "user_project_tasks",
                    "user_roles",
                ]:
                    # Given streams have all automatic fields
                    self.assertEqual(
                        len(expected_all_keys), len(expected_automatic_keys)
                    )
                else:
                    self.assertGreater(
                        len(expected_all_keys), len(expected_automatic_keys)
                    )
                self.assertTrue(
                    expected_automatic_keys.issubset(expected_all_keys),
                    msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"',
                )
                self.assertSetEqual(expected_all_keys, actual_all_keys)
