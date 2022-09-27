"""
Test that the start_date configuration is respected
"""
import datetime
from functools import reduce
from time import sleep

from base import BaseTapTest
from dateutil.parser import parse
from harvest_api import set_up, tear_down, update_streams
from tap_tester import LOGGER, menagerie, runner


class StartDateTest(BaseTapTest):
    """
    Test that we get a lot of data back based on the start date configured in the base
    """

    @classmethod
    def setUpClass(cls):
        set_up(cls)

    @classmethod
    def tearDownClass(cls):
        tear_down(cls)

    def name(self):
        return "tap_tester_harvest_start_date"

    def do_test(self, conn_id):
        """
        Test that the start_date configuration is respected

        • Verify that a sync with a later start date has at least one record synced
          and fewer records than the 1st sync with a previous start date
        • Verify that each stream has fewer records than the earlier start date sync
        • Verify all data from later start data has bookmark values >= start_date
        • Verify that the minimum bookmark sent to the target for the later start_date sync
          is greater than or equal to the start date
        """

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        streams_obeys_start_date = {
            key
            for key, value in self.expected_metadata().items()
            if value.get(self.OBEYS_START_DATE)
        }

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = {"estimate_messages", "invoice_messages", "invoice_payments"}

        our_catalogs = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id")
            in streams_obeys_start_date.difference(untested_streams)
        ]

        self.select_all_streams_and_fields(
            conn_id, our_catalogs, select_all_fields=True
        )

        # Run a sync job using orchestrator
        self.run_sync(conn_id)

        # Count actual rows synced
        first_sync_records = runner.get_records_from_target_output()

        # Set the start date for a new connection based on bookmarks' largest value
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        bookmark_list = [
            list(book.values())[0] for stream, book in first_max_bookmarks.items()
        ]
        bookmark_dates = []
        for bookmark in bookmark_list:
            try:
                bookmark_dates.append(parse(bookmark))
            except (ValueError, OverflowError, TypeError):
                pass

        if not bookmark_dates:
            # THERE WERE NO BOOKMARKS THAT ARE DATES.
            # REMOVE CODE TO FIND A START DATE AND ENTER ONE MANUALLY
            raise ValueError

        largest_bookmark = reduce(lambda a, b: a if a > b else b, bookmark_dates)

        # Ensure different updated_at times for updates
        sleep(2)

        # Update Data prior to the 2nd sync
        LOGGER.info("Updating streams prior to 2nd sync job")
        update_streams(self)

        # Get state
        first_sync_state = menagerie.get_state(conn_id)

        # Create a new connection with the new start_date
        self.start_date = datetime.datetime.strftime(
            largest_bookmark, self.START_DATE_FORMAT
        )
        conn_id_2 = self.create_connection(original_properties=False)

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id_2)
        our_catalogs = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id")
            in streams_obeys_start_date.difference(untested_streams)
        ]
        self.select_all_streams_and_fields(
            conn_id_2, our_catalogs, select_all_fields=True
        )

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id_2)
        second_total_records = reduce(
            lambda a, b: a + b, second_sync_record_count.values(), 0
        )
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # Verify that at least one record is synced
        self.assertGreater(second_total_records, 0)

        for stream in streams_obeys_start_date.difference(untested_streams):
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                primary_keys_sync_1 = {
                    tuple(
                        message.get("data", {}).get(expected_pk)
                        for expected_pk in expected_primary_keys
                    )
                    for message in first_sync_records.get(stream, {"messages": []}).get(
                        "messages"
                    )
                    if message.get("action") == "upsert"
                }
                primary_keys_sync_2 = {
                    tuple(
                        message.get("data", {}).get(expected_pk)
                        for expected_pk in expected_primary_keys
                    )
                    for message in second_sync_records.get(
                        stream, {"messages": []}
                    ).get("messages")
                    if message.get("action") == "upsert"
                }

                # Verify that sync 2 has at least one record synced
                self.assertGreater(second_sync_record_count.get(stream, 0), 0)

                if stream not in self.PARENT_REP_VALUE_STREAMS:
                    # Verify by primary key values, that all records in the 1st sync are included in the 2nd sync.
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                # Verify all data from 2nd sync >= start_date
                second_min_bookmark = second_min_bookmarks.get(stream)
                # There should be only one
                target_value = next(iter(second_min_bookmark.values()))

                stream_state_name = (
                    stream + "_parent" if stream in self.PARENT_REP_VALUE_STREAMS else stream
                )

                if target_value:

                    # It's okay if there isn't target data for a stream
                    try:
                        target_value = self.local_to_utc(parse(target_value))

                        # Verify that the minimum bookmark sent to the target for the second sync
                        # is greater than or equal to the start date
                        self.assertGreaterEqual(
                            target_value,
                            self.local_to_utc(
                                parse(first_sync_state[stream_state_name])
                            ),
                        )

                    except (OverflowError, ValueError, TypeError):
                        print(
                            "Bookmarks cannot be converted to dates, "
                            "can't test start_date for {}".format(stream)
                        )
