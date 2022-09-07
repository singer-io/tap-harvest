from tap_tester import menagerie, runner
from harvest_api import set_up_class, tear_cown_class
from base import BaseTapTest

class HarvestParentInterruptedSyncTest(BaseTapTest):
    """Test tap's ability to recover from an interrupted sync"""

    def name(self):
        return "tap_tester_harvest_interrupted_sync"

    @classmethod
    def setUpClass(cls):
        set_up_class(cls)

    @classmethod
    def tearDownClass(cls):
        tear_cown_class(cls)

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
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                          if catalog.get('stream_name') in streams_to_test]

        self.select_all_streams_and_fields(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False,
        )
        
        # Run initial sync
        self.run_sync(conn_id)

        # Acquire records from the target output
        full_sync_records = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        interrupted_state = full_sync_state.copy()
        interrupted_state['currently_syncing'] = 'expense_categories'
        del interrupted_state['clients']
        interrupted_state['expense_categories'] = full_sync_records.get('expense_categories', {}).get('messages', [])[-1].get('data', {})['updated_at']
        menagerie.set_state(conn_id, interrupted_state)

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

            # Verify final_state is equal to uninterrupted sync's state
            # (This is what the value would have been without an interruption and proves resuming succeeds)
            self.assertDictEqual(final_state, full_sync_state)
            
        for stream in streams_to_test:
            with self.subTest(stream=stream):
                # set expectations
                expected_replication_method = self.expected_replication_method()[stream]
                expected_primary_keys = list(self.expected_primary_keys()[stream])

                # gather results
                full_records = [message['data'] for message in full_sync_records[stream]['messages']]
                full_record_count = len(full_records)
                interrupted_records = [message['data'] for message in interrupted_sync_records.get(stream, {}).get('messages', [])]
                interrupted_record_count = len(interrupted_records)
                
                if expected_replication_method == self.INCREMENTAL:
                    expected_replication_key = next(iter(self.expected_replication_keys().get(stream, set())))

                    if stream in interrupted_state.keys():
                        interrupted_bookmark = interrupted_state[stream]
                        if stream == interrupted_state['currently_syncing']:
                            for record in interrupted_records:
                    
                                rec_time = record[expected_replication_key]
                                self.assertGreaterEqual(rec_time, interrupted_bookmark)

                                # Verify all interrupted recs are in full recs
                                self.assertIn(record, full_records,  msg='incremental table record in interrupted sync not found in full sync')

                                # Record count for all streams of interrupted sync match expectations
                                full_records_after_interrupted_bookmark = 0

                            for record in full_records:
                                rec_time = record[expected_replication_key]

                                if (rec_time > interrupted_bookmark):
                                    full_records_after_interrupted_bookmark += 1

                            self.assertEqual(full_records_after_interrupted_bookmark, len(interrupted_records), \
                                                msg="Expected {} records in each sync".format(full_records_after_interrupted_bookmark))
                        else:
                            # Verify we collected records that have the same replication value as a bookmark for streams that are already synced
                            self.assertGreaterEqual(interrupted_record_count, 0)
                    
                    else:
                        for record in interrupted_records:
                            self.assertGreaterEqual(rec_time, self.DEFAULT_START_DATE)

                        # Verify resuming sync replicates all records that were found in the full sync (uninterrupted)
                        for record in interrupted_records:
                            with self.subTest(record_primary_key=record[expected_primary_keys[0]]):
                                self.assertIn(record, full_records, msg='Unexpected record replicated in resuming sync.')
                        for record in full_records:
                            with self.subTest(record_primary_key=record[expected_primary_keys[0]]):
                                self.assertIn(record, interrupted_records, msg='Record missing from resuming sync.' )

                else:
                     # Verify full table streams do not save bookmarked values at the conclusion of a succesful sync
                    self.assertNotIn(stream, full_sync_state.keys())
                    self.assertNotIn(stream, final_state.keys())

                    # Verify first and second sync have the same records
                    self.assertEqual(full_record_count, interrupted_record_count)
                    for rec in interrupted_records:
                        self.assertIn(rec, full_records, msg='full table record in interrupted sync not found in full sync')

