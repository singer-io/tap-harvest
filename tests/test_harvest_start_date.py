"""
Test that the start_date configuration is respected
"""
import unittest
from functools import reduce
from dateutil.parser import parse

from tap_tester import menagerie, runner
from tap_tester.scenario import SCENARIOS

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
        logging.info("Start Setup")
        # Track what was created to delete in teardown
        cls._teardown_delete = {"contacts": [], "projects": [], "project_tasks": [],
                                "tasks": [], "invoices": [], "invoice_messages": [],
                                "invoice_item_categories": [], "invoice_payments": [],
                                "estimates": [], "expenses": [], "expense_categories": [],
                                "estimate_messages": [], "estimate_item_categories": [],
                                "time_entries": [], "roles": [], "clients": [],
                                "project_users": [], "user_roles": []}

        # Protect against surviving projects from previous failures
        project_1 = ""
        for project in get_all('projects'):
            delete_stream('projects', project['id'])

        # Create dummy data in specifc streams prior to first sync to ensure they are captured        
        for itter in range(2):
            logging.info("Creating {} round(s) of data ...".format(itter + 1))

            # Clients
            client_1 = create_client()
            cls._teardown_delete["clients"].append({"id": client_1["id"]})

            # Contacts
            contact_1 = create_contact(client_1['id'])
            cls._teardown_delete["contacts"].append({"id": contact_1["id"]})

            # Projects
            if itter < 2: # Protect against project max (2)
                project_1 = create_project(client_1['id'])
                cls._teardown_delete["projects"].append({"id": project_1["id"]})
            project_user_1 = create_project_user(project_1['id'], get_random("users"))
            cls._teardown_delete["project_users"].append({"id": project_user_1["id"]})
            
            # Tasks
            task_1 = create_task()
            cls._teardown_delete["tasks"].append({"id": task_1["id"]})
            project_task_1 = create_project_task(project_1['id'], task_1['id'])
            cls._teardown_delete["project_tasks"].append({"id": project_task_1["id"]})

            # Users ( This is necessary to ensure user_projects data exists)
            user_1 = update_user(get_random('users'))

            # Roles
            role_1 = create_role()
            cls._teardown_delete["roles"].append({"id": role_1["id"]})
            user_role_1 = update_role(role_1['id'], [get_random("users")])
            cls._teardown_delete["user_roles"].append({"id": user_role_1["id"]})
            
            # Estimates
            estimate_1 = create_estimate(client_1['id'])
            cls._teardown_delete["estimates"].append({"id": estimate_1['id']})
            estimate_message_1 = create_estimate_message(estimate_1['id'])
            cls._teardown_delete["estimate_messages"].append({"id": estimate_message_1['id']})
            estimate_item_category_1 = create_estimate_item_category()
            cls._teardown_delete["estimate_item_categories"].append({"id": estimate_item_category_1['id']})

            # Invoices
            invoice_1 = create_invoice(client_id=client_1['id'], project_id=project_1['id'])
            cls._teardown_delete["invoices"].append({"id": invoice_1["id"]})
            invoice_message_1 = create_invoice_message(invoice_1['id'])
            cls._teardown_delete["invoice_messages"].append({"id": invoice_message_1["id"]})
            invoice_payment_1 = create_invoice_payment(invoice_1['id'])
            cls._teardown_delete["invoice_payments"].append({"id": invoice_payment_1["id"]})
            invoice_item_category_1 = create_invoice_item_category()
            cls._teardown_delete["invoice_item_categories"].append({"id": invoice_item_category_1["id"]})

            # Time Entries
            time_entry_1 = create_time_entry(project_1['id'], task_1['id'])
            cls._teardown_delete["time_entries"].append({"id": time_entry_1["id"]})

            # Expesnes
            expense_category_1 = create_expense_category()
            cls._teardown_delete["expense_categories"].append({"id": expense_category_1['id']})
            expense_1 = create_expense(project_1['id'])
            cls._teardown_delete["expenses"].append({"id": expense_1['id']})



    @classmethod
    def tearDownClass(cls):
        # Clean up any data created in the setup
        logging.info("Start Teardown")
        # Projects
        for project in cls._teardown_delete['projects']:
            delete_stream('projects', project['id'])
        # Estimates
        for estimate in cls._teardown_delete['estimates']:
            delete_stream('estimates', estimate['id'])
        for category in cls._teardown_delete['estimate_item_categories']:
            delete_stream('estimate_item_categories', category['id'])
        # Time Entries TODO fix the delete methods
        # for time_entry in cls._teardown_delete['time_entries']:
        #     delete_stream('time_entries', time_entry['id'])
        # Tasks
        # for task in cls._teardown_delete['tasks']:
        #     delete_stream('tasks', task['id'])
        # Invoices
        for invoice in cls._teardown_delete['invoices']:
            delete_stream('invoices', invoice['id'])
        for category in cls._teardown_delete['invoice_item_categories']:
            delete_stream('invoice_item_categories', category['id'])
        # Clients
        for client in cls._teardown_delete['clients']:
            delete_stream('clients', client['id'])
        # Expenses
        # for expense in cls._teardown_delete['expenses']:
        #     delete_stream('expenses', expense['id'])
        # for expense_category in cls._teardown_delete['expense_categories']:
        #     delete_stream('expense_categories', expense_category['id'])
        for role in cls._teardown_delete['roles']:
            delete_stream('roles', role['id'])

        ##########  Uncomment for PURGE MODE ###############
        # stream = ""
        # logging.info("Comencing PURGE of stream: {}".format(stream))
        # all_of_stream = get_all(stream)
        # while all_of_stream:
        #     for s in get_all(stream):
        #         try:
        #             delete_stream(stream, s['id'])
        #         except AssertionError:
        #             continue
        #     all_of_stream = get_all(stream)
        ####################################################

    def name(self):
        return "tap_tester_harvest_start_date"


    def do_test(self, conn_id):
        """Test we get a lot of data back based on the start date configured in base"""

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAM
        untested_streams = self.child_streams().union({
            "users",
            "estimate_messages",
            "invoice_messages",
            "invoice_payments"
        })

        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]

        # self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)
        
        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)
        first_total_records = reduce(lambda a, b: a + b, first_sync_record_count.values())

        # Count actual rows synced
        first_sync_records = runner.get_records_from_target_output()

        # set the start date for a new connection based off bookmarks largest value
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
        self.start_date = self.local_to_utc(largest_bookmark).strftime(self.START_DATE_FORMAT)

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
        
        # create a new connection with the new start_date
        conn_id = self.create_connection(original_properties=False)

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]
        # self.select_all_streams_and_fields(conn_id, our_catalogs, select_all_fields=True)

        # Run a sync job using orchestrator
        second_sync_record_count = self.run_sync(conn_id)
        second_total_records = reduce(lambda a, b: a + b, second_sync_record_count.values(), 0)
        second_sync_records = runner.get_records_from_target_output()
        second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)

        # verify that at least one record synced and less records synced than the 1st connection
        self.assertGreater(second_total_records, 0)
        self.assertLess(second_total_records, first_total_records)

        for stream in incremental_streams.difference(untested_streams):
            with self.subTest(stream=stream):

                # verify that each stream has less records than the first connection sync
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
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
                                                self.local_to_utc(parse(self.start_date)))

                    except (OverflowError, ValueError, TypeError):
                        print("bookmarks cannot be converted to dates, "
                              "can't test start_date for {}".format(stream))


SCENARIOS.add(StartDateTest)
