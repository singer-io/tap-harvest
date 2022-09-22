from time import sleep

from base import BaseTapTest
from harvest_api import insert_one_record, set_up, tear_down, update_streams
from tap_tester import LOGGER, menagerie, runner

PARENT_REP_VALUE_STREAMS = {
    "invoice_line_items",
    "estimate_line_items",
    "user_project_tasks",
    "user_roles",
    "external_reference",
    "time_entry_external_reference",
}


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream."""

    @classmethod
    def setUpClass(cls):
        set_up(cls, rec_count=3)

    @classmethod
    def tearDownClass(cls):
        tear_down(cls)

    def name(self):
        return "{}_bookmark_test".format(super().name())

    def match_records(self, expected_record, actual_record):
        """Method to check if all the fields of expected record is available in
        actual_record.

        Args:
            expected_record (dict): Ecpected record (created/updated)
            actual_record (dict): Actual record (Fetched from tap.)

        Returns:
            Boolean: Return true if expected record is actual record else False.
        """
        dict_items = actual_record.items()
        for items in expected_record.items():
            if items in dict_items:
                return True
        return False

    def do_test(self, conn_id):
        """
        Verify that for each stream you can do a sync which records bookmarks.
        That the bookmark is the maximum value sent to the target for the replication key.
        That a second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                new data added to the stream is done at a rate slow enough that you haven't
                doubled the amount of data from the start date to the first sync between
                the first sync and second sync run in this test)

        Verify that only data for incremental streams is sent to the target

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """
        # Select all streams and no fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = {
            # TODO - BUG (https://github.com/singer-io/tap-harvest/issues/35)
            "projects",  # Limited to 2 projects on free plan
            "users",  # Limited to a single user on the free plan
            "user_projects",  # Limited by projects
            "project_users",  # Limited by users
            "user_project_tasks",  # Limited by user_projects
        }

        streams_to_test = self.expected_streams() - untested_streams

        our_catalogs = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id") in streams_to_test
        ]

        expected = {stream: [] for stream in self.expected_streams()}

        # Field selection
        self.select_all_streams_and_fields(
            conn_id, our_catalogs, select_all_fields=True
        )

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # Verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()), streams_to_test)

        # Get the first state of sync 1
        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Ensure different updated_at times for updates and inserts
        sleep(2)

        # Insert Data before the second sync
        expected = update_streams(self, expected)
        sleep(2)

        for stream, expect in self._master.items():
            if expect.get("delete_me"):
                LOGGER.info(
                    "last synced {}: {}".format(stream, expect["delete_me"][-1])
                )

        try:
            expected = insert_one_record(self, expected)
        finally:
            LOGGER.info("Data Inserted, 2nd Sync Completed")

        # Run a second sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)

        # Get data about rows synced
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)
        second_max_bookmarks = self.max_bookmarks_by_stream(second_sync_records)
        second_sync_state = menagerie.get_state(conn_id)

        # Verify the first sync sets a bookmark of the expected form
        self.assertIsNotNone(first_sync_state)

        # Verify the second sync sets a bookmark of the expected form
        self.assertIsNotNone(second_sync_state)

        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT HAVE BOOKMARKS.
        # ADJUST IF NECESSARY
        for stream in streams_to_test:
            with self.subTest(stream=stream):
                stream_state_name = (
                    stream + "_parent" if stream in PARENT_REP_VALUE_STREAMS else stream
                )

                # Get bookmark values from state and target data
                state_value = first_sync_state.get(stream_state_name)
                final_state_value = second_sync_state.get(stream_state_name)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count, 0, msg="second sync didn't have any records"
                )

                # Verify the 2nd sync has specific inserted and updated records
                actual = second_sync_records.get(stream, {"messages": []}).get(
                    "messages"
                )
                actual = [item["data"] for item in actual]
                for expected_key_values in expected[stream]:
                    matching = False
                    for record in actual:
                        matching = self.match_records(expected_key_values, record)
                        if matching:
                            break
                    self.assertTrue(matching)

                stream_bookmark_keys = self.expected_replication_keys().get(
                    stream, set()
                )
                # There shouldn't be a compound replication key
                assert len(stream_bookmark_keys) == 1

                stream_bookmark_key = next(iter(stream_bookmark_keys))
                first_max_bookmark = first_max_bookmarks.get(stream, {None: None}).get(
                    stream_bookmark_key
                )
                target_min_value = first_min_bookmarks.get(stream, {None: None}).get(
                    stream_bookmark_key
                )
                final_bookmark = second_max_bookmarks.get(stream, {None: None}).get(
                    stream_bookmark_key
                )
                second_min_bookmark = second_min_bookmarks.get(
                    stream, {None: None}
                ).get(stream_bookmark_key)

                try:
                    # Attempt to parse the bookmark as a date
                    state_value = self.parse_bookmark_to_date(state_value)
                    first_max_bookmark = self.parse_bookmark_to_date(first_max_bookmark)
                    target_min_value = self.parse_bookmark_to_date(target_min_value)
                    final_bookmark = self.parse_bookmark_to_date(final_bookmark)
                    final_state_value = self.parse_bookmark_to_date(final_state_value)
                    second_min_bookmark = self.parse_bookmark_to_date(
                        second_min_bookmark
                    )

                except (OverflowError, ValueError, TypeError):
                    LOGGER.warning(
                        "Bookmarks cannot be converted to dates, comparing values directly"
                    )

                # Verify that there is data with different bookmark values - setup necessary
                self.assertGreater(
                    first_max_bookmark,
                    target_min_value,
                    msg="Data isn't set up to be able to test bookmarks",
                )

                # Verify state agrees with target data after 1st sync
                self.assertEqual(
                    state_value,
                    first_max_bookmark,
                    msg="The bookmark value isn't correct based on target data",
                )

                # Verify all data from 2nd sync >= 1st bookmark
                self.assertGreater(final_bookmark, second_min_bookmark)

                # Verify that the minimum bookmark sent to the target for the second sync
                # is greater than or equal to the bookmark from the first sync
                # TODO - BUG this will fail if no streams are updated/created
                self.assertGreaterEqual(second_min_bookmark, state_value)

                # Make sure the final bookmark is the latest updated at
                # Since the API sends data in created_at desc order we made the updated_at, not to test
                # if we send the correct value even when the latest record doesn't have the latest updated_at value
                self.assertEqual(final_bookmark, final_state_value)
