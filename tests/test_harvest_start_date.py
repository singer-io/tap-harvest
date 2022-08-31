"""
Test that the start_date configuration is respected
"""
import unittest
from functools import reduce
from dateutil.parser import parse

from tap_tester import menagerie, runner

from harvest_api import *
from base import BaseTapTest
from spec import TapSpec


class StartDateTest(BaseTapTest):
    """
    Test that the start_date configuration is respected
    
    • verify that a sync with a later start date has at least one record synced 
      and less records than the 1st sync with a previous start date
    • verify that each stream has less records than the earlier start date sync
    • verify all data from later start data has bookmark values >= start_date
    • verify that the minimum bookmark sent to the target for the later start_date sync
      is greater than or equal to the start date
    """

    @classmethod
    def setUpClass(cls):
        set_up_class(cls)

    @classmethod
    def tearDownClass(cls):
        tear_cown_class(cls)

    def name(self):
        return "tap_tester_harvest_start_date"

    def do_test(self, conn_id):
        """Test we get a lot of data back based on the start date configured in base"""

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        streams_obeys_start_date = {key for key, value in self.expected_metadata().items()
                               if value.get(self.OBEYS_START_DATE)}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union({
            "users",
            "user_projects",
            "estimate_messages",
            "invoice_messages",
            "invoice_payments"
        })

        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_obeys_start_date.difference(
                            untested_streams)]

        self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)
        
        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)
        first_total_records = reduce(lambda a, b: a + b, first_sync_record_count.values())

        # Count actual rows synced
        first_sync_records = runner.get_records_from_target_output()

        # set the start date for a new connection based on bookmarks' largest value
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        bookmark_list = [list(book.values())[0] for stream, book in first_max_bookmarks.items()]
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

        # Update Data prior to the 2nd sync
        logging.info("Updating streams prior to 2nd sync job")

        logging.info("   Users")
        updated_user = update_user(get_random("users"))
        
        logging.info("   Projects")
        project_id = self._teardown_delete['projects'][0]['id']
        updated_project = update_project(project_id)
        updated_project_user = update_project_user(project_id, self._teardown_delete['project_users'][0]['id'])

        logging.info("   Clients")
        updated_client = update_client(self._teardown_delete['clients'][0]['id'])
        
        logging.info("   Tasks")
        updated_task = update_task(self._teardown_delete['tasks'][0]['id'])
        updated_project = update_project_task(project_id, self._teardown_delete['project_tasks'][0]['id'])

        logging.info("   Roles")
        updated_role = update_role(self._teardown_delete['roles'][0]['id'])
        updated_user_role = update_role(self._teardown_delete['roles'][0]['id'], [get_random("users")])
        
        logging.info("   Invoices")
        updated_invoice = update_invoice(self._teardown_delete['invoices'][0]['id'])
        updated_invoice_message = update_invoice_message(self._teardown_delete['invoices'][0]['id'])
        updated_invoice_payment = update_invoice_payment(self._teardown_delete['invoices'][0]['id'])
        updated_category = update_invoice_item_category(self._teardown_delete['invoice_item_categories'][0]['id'])

        logging.info("   Contacts")
        updated_contact = update_contact(self._teardown_delete['contacts'][0]['id'])

        logging.info("   Estimates")
        updated_estimate = update_estimate(self._teardown_delete['estimates'][0]['id'])
        updated_estimate_message = update_estimate_message(self._teardown_delete['estimates'][0]['id'])
        updated_category = update_estimate_item_category(self._teardown_delete['estimate_item_categories'][1]['id'])

        logging.info("   Expenses")
        updated_expense = update_expense(self._teardown_delete['expenses'][0]['id'])
        updated_expense_category = update_expense_category(self._teardown_delete['expense_categories'][0]['id'])

        logging.info("   Time Entries")
        updated_time_entry = update_time_entry(self._teardown_delete['time_entries'][0]['id'])

        # get state
        state = menagerie.get_state(conn_id)

        # create a new connection with the new start_date
        self.start_date = datetime.datetime.strftime(largest_bookmark, self.START_DATE_FORMAT)
        conn_id_2 = self.create_connection(original_properties=False)

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id_2)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in streams_obeys_start_date.difference(
                            untested_streams)]
        self.select_all_streams_and_fields(conn_id_2, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id_2)
        second_total_records = reduce(lambda a, b: a + b, second_sync_record_count.values(), 0)
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # verify that at least one record synced and less records synced than the 1st connection
        self.assertGreater(second_total_records, 0)
        self.assertLess(second_total_records, first_total_records)

        for stream in streams_obeys_start_date.difference(untested_streams):
            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                primary_keys_sync_1 = set([tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                           for message in first_sync_records.get(stream, {'messages': []}).get('messages')
                                           if message.get('action') == 'upsert'])
                primary_keys_sync_2 = set([tuple(message.get('data', {}).get(expected_pk) for expected_pk in expected_primary_keys)
                                           for message in second_sync_records.get(stream, {'messages': []}).get('messages')
                                           if message.get('action') == 'upsert'])

                # verify that sync 2 has at least one record synced
                self.assertGreater(second_sync_record_count.get(stream, 0), 0)

                # Verify by primary key values, that all records in the 1st sync are included in the 2nd sync.
                self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                # verify that each stream has less records than the first connection sync
                self.assertGreaterEqual(
                    first_sync_record_count.get(stream, -1),
                    second_sync_record_count.get(stream, 0),
                    msg="second had more records, start_date usage not verified")

                # verify all data from 2nd sync >= start_date
                target_mark = second_min_bookmarks.get(stream, {"mark": None})
                target_value = next(iter(target_mark.values()))  # there should be only one

                if target_value:

                    # it's okay if there isn't target data for a stream
                    try:
                        target_value = self.local_to_utc(parse(target_value))

                        # verify that the minimum bookmark sent to the target for the second sync
                        # is greater than or equal to the start date
                        self.assertGreaterEqual(target_value,
                                                self.local_to_utc(parse(state[stream])))

                    except (OverflowError, ValueError, TypeError):
                        print("bookmarks cannot be converted to dates, "
                              "can't test start_date for {}".format(stream))
