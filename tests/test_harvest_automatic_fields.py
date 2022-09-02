from tap_tester import menagerie, runner
from base import BaseTapTest
from harvest_api import set_up_class, tear_cown_class

class TestAutomaticFields(BaseTapTest):
    """
    Test that automatic fields are received for selected streams.
    """

    @classmethod
    def setUpClass(cls):
        set_up_class(cls)

    @classmethod
    def tearDownClass(cls):
        tear_cown_class(cls)

    def name(self):
        return "tap_tester_harvest_automatic_fields"

    def do_test(self, conn_id):
        """
        • Verify we can deselect all fields except when inclusion=automatic, which is handled by base.py methods
        • Verify that only the automatic fields are sent to the target.
        • Verify that all replicated records have unique primary key values.
        """
        streams_to_test = self.expected_streams()

        # Run check mode
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in streams_to_test]

        self.select_all_streams_and_fields(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )

        # Run initial sync
        record_count_by_stream = self.run_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                
                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_keys = self.expected_automatic_keys().get(stream)

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get('data').keys()) for row in data.get('messages', {})]
                primary_keys_list = [
                    tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                    for message in data.get('messages', [])
                    if message.get('action') == 'upsert']
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit for the {} stream".format(stream))

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify that all replicated records have unique primary key values.
                self.assertEqual(
                    len(primary_keys_list),
                    len(unique_primary_keys_list),
                    msg="Replicated record does not have unique primary key values.")
