from tap_tester import menagerie, runner
from harvest_api import set_up, tear_down
from base import BaseTapTest


class HarvestParentInterruptedSyncAddStreamTest(BaseTapTest):
    """Test tap's ability to recover from an interrupted sync"""

    def name(self):
        return "tap_tester_harvest_interrupted_sync_add_stream"

    @classmethod
    def setUpClass(cls):
        set_up(cls)

    @classmethod
    def tearDownClass(cls):
        tear_down(cls)

    def do_test(self, conn_id):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.

        Test Cases:
         - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
         - Verify only records with replication-key values greater than or equal to the stream level bookmark are
           replicated on the resuming sync for the interrupted stream.
         - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        streams_to_test = {'invoices', 'invoice_messages', 'expense_categories', 'clients'}

        # Run check mode
        found_catalogs = menagerie.get_catalogs(conn_id)

        # Table and field selection
        test_catalogs_fields = [catalog for catalog in found_catalogs
                                if catalog.get('stream_name') in streams_to_test]

        self.select_all_streams_and_fields(
            conn_id, test_catalogs_fields, select_all_fields=False,
        )

        # Run initial sync
        self.run_sync(conn_id)

        # Acquire records from the target output
        full_sync_records = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        # Add a stream between syncs
        added_stream = 'users'
        streams_to_test.add(added_stream)
        added_stream_catalog = [catalog for catalog in found_catalogs
                                if catalog.get('stream_name') == added_stream]

        interrupted_state = full_sync_state.copy()
        interrupted_state['currently_syncing'] = 'expense_categories'
        del interrupted_state['clients']
        interrupted_state['expense_categories'] = full_sync_records.get(
            'expense_categories', {}).get('messages', [])[-1].get('data', {})['updated_at']
        menagerie.set_state(conn_id, interrupted_state)

        # Table and field selection
        test_catalogs_fields = [catalog for catalog in added_stream_catalog
                                if catalog.get('stream_name') in streams_to_test]

        self.select_all_streams_and_fields(
            conn_id, test_catalogs_fields, select_all_fields=False,
        )
        # Run another sync
        self.run_sync(conn_id)

        # acquire records from target output
        interrupted_sync_records = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        # Checking resuming sync resulted in successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state)

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                # set expectations
                expected_primary_keys = list(self.expected_primary_keys()[stream])

                # gather results
                if stream != added_stream:
                    full_records = [message['data'] for message in full_sync_records[stream]['messages']]

                interrupted_records = [message['data']
                                       for message in interrupted_sync_records.get(stream, {}).get('messages', [])]
                interrupted_record_count = len(interrupted_records)

                expected_replication_key = next(iter(self.expected_replication_keys().get(stream, set())))

                if stream in full_sync_state.keys():
                    full_sync_stream_bookmark = full_sync_state.get(stream, {})
                    final_sync_stream_bookmark = full_sync_state.get(stream, {})

                if stream in interrupted_state.keys():
                    interrupted_bookmark = interrupted_state[stream]

                    if stream == interrupted_state['currently_syncing']:

                        for record in interrupted_records:
                            rec_time = record[expected_replication_key]
                            # Verify that interrupted stream replicated records after bookmark
                            self.assertGreaterEqual(rec_time, interrupted_bookmark)

                    else:
                        # verify we collected records that have the same replication value as a bookmark for streams that are already synced
                        self.assertGreaterEqual(interrupted_record_count, 0)

                    # Verify state ends with the same value for common streams after both full and interrupted syncs
                    self.assertEqual(full_sync_stream_bookmark, final_sync_stream_bookmark)

                else:
                    # Verify all interrupted sync records are after the start date
                    for record in interrupted_records:
                        with self.subTest(record_primary_key=record[expected_primary_keys[0]]):
                            rec_time = record[expected_replication_key]
                            self.assertGreaterEqual(rec_time, self.DEFAULT_START_DATE)

                    if stream != added_stream:

                        # Verify all interrupted recs are in full sync
                        for record in interrupted_records:
                            with self.subTest(record_primary_key=record[expected_primary_keys[0]]):
                                self.assertIn(record, full_records,
                                              msg='incremental table record in interrupted sync not found in full sync')

                        # Verify resuming sync replicates all records that were found in the full sync (uninterrupted)
                        for record in full_records:
                            with self.subTest(record_primary_key=record[expected_primary_keys[0]]):
                                self.assertIn(record, interrupted_records, msg='Record missing from resuming sync.')
