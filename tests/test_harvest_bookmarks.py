"""
Test tap sets a bookmark and respects it for the next sync of a stream
"""
import logging

from time import sleep
from datetime import datetime as dt

from dateutil.parser import parse
import random

from tap_tester import menagerie, runner
from tap_tester.scenario import SCENARIOS

from harvest_api import *
from base import BaseTapTest


class BookmarkTest(BaseTapTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream"""

    @classmethod
    def setUpClass(cls):
        logging.info("Start Setup")
        logging.info("Creating Data..")
        client_1 = create_client()
        try:
            project_1 = create_project(client_1['id'])
        except AssertionError:
            # Protect against surviving projects from previous failures
            for project in get_all('projects'):
                delete_stream('projects', project['id'])
            project_1 = create_project(client_1['id'])
        try:
           project_2 = create_project(client_1['id'])
        except AssertionError:
            for project in get_all('projects'):
                delete_stream('projects', project['id'])
            project_1 = create_project(client_1['id'])
            project_2 = create_project(client_1['id'])
        estimate_1 = create_estimate(client_1['id'])
        estimate_message_1 = create_estimate_message(estimate_1['id'])
        estimate_line_item_1 = estimate_1['line_items'][0]
        estimate_item_category_1 = create_estimate_item_category()
        invoice_1 = create_invoice(client_id=client_1['id'],
                                   estimate_id=estimate_1['id'],
                                   project_id=project_1['id'])
        invoice_line_item_1 = invoice_1['line_items'][0]
        invoice_message_1 = create_invoice_message(invoice_1['id'])
        invoice_payment_1 = create_invoice_payment(invoice_1['id'])
        invoice_item_category_1 = create_invoice_item_category()
        role_1 = create_role()
        update_role(role_1['id'], [get_random('users')])
        project_user_1 = create_project_user(project_1['id'], get_random('users'))
        contact_1 = create_contact(client_1['id'])
        expense_1 = create_expense(project_1['id'])
        expense_category_1 = create_expense_category()
        task_1 = create_task()
        project_task_1 = create_project_task(project_1['id'], task_1['id'])
        # time_entries require task assignment (project_task) prior to creation
        time_entry_1 = create_time_entry(project_1['id'], task_1['id'])
        external_reference_1 = time_entry_1['external_reference']
        sleep(2) # Ensure different created_at times

        client_2 = create_client()
        estimate_2 = create_estimate(client_1['id'])
        estimate_message_2 = create_estimate_message(estimate_2['id'])
        estimate_line_item_2 = estimate_2['line_items'][0]
        estimate_item_category_2 = create_estimate_item_category()
        invoice_2 = create_invoice(client_id=client_1['id'],
                                   estimate_id=estimate_2['id'],
                                   project_id=project_1['id'])
        invoice_line_item_2 = invoice_2['line_items'][0]
        invoice_message_2 = create_invoice_message(invoice_2['id'])
        invoice_payment_2 = create_invoice_payment(invoice_2['id'])
        invoice_item_category_2 = create_invoice_item_category()
        role_2 = create_role()
        update_role(role_2['id'], [get_random('users')])
        project_user_2 = create_project_user(project_2['id'], get_random('users'))
        contact_2 = create_contact(client_2['id'])
        expense_2 = create_expense(project_1['id'])
        expense_category_2 = create_expense_category()
        task_2 = create_task()
        project_task_2 = create_project_task(project_1['id'], task_2['id'])
        time_entry_2 = create_time_entry(project_1['id'], task_2['id'])
        external_reference_2 = time_entry_2['external_reference']
        sleep(2)

        client_3 = create_client()
        estimate_3 = create_estimate(client_1['id'])
        estimate_message_3 = create_estimate_message(estimate_3['id'])
        estimate_line_item_3 = estimate_3['line_items'][0]
        estimate_item_category_3 = create_estimate_item_category()
        invoice_3 = create_invoice(client_id=client_1['id'],
                                   estimate_id=estimate_3['id'],
                                   project_id=project_1['id'])
        invoice_line_item_3 = invoice_3['line_items'][0]
        invoice_message_3 = create_invoice_message(invoice_3['id'])
        invoice_payment_3 = create_invoice_payment(invoice_3['id'])
        invoice_item_category_3 = create_invoice_item_category()
        role_3 = create_role()
        update_role(role_3['id'], [get_random('users')])
        contact_3 = create_contact(client_3['id'])
        expense_3 = create_expense(project_1['id'])
        expense_category_3 = create_expense_category()
        task_3 = create_task()
        project_task_3 = create_project_task(project_1['id'], task_3['id'])
        time_entry_3 = create_time_entry(project_1['id'], task_3['id'])
        external_reference_3 = time_entry_3['external_reference']
        # Remove data after test is complete
        cls._teardown_delete = {"projects": [{"id": project_1["id"]}, {"id": project_2["id"]}],
                                "invoices":[{"id":invoice_1['id']}, {"id":invoice_2['id']}, {"id":invoice_3['id']}],
                                "roles":[{"id":role_1['id']}, {"id":role_2['id']}, {"id":role_3['id']}],
                                "clients": [{"id": client_1["id"]}, {"id": client_2["id"]}, {"id": client_3["id"]}],
                                "contacts": [{"id": contact_1["id"]}, {"id": contact_2["id"]}, {"id": contact_3["id"]}],
                                "estimates": [{"id": estimate_1["id"]}, {"id": estimate_2["id"]}, {"id": estimate_3["id"]}],
                                "expenses": [{"id": expense_1["id"]}, {"id": expense_2["id"]}, {"id": expense_3["id"]}],
                                "tasks": [{"id": task_1["id"]}, {"id": task_2["id"]}, {"id": task_3["id"]}],
                                "time_entries": [{"id": time_entry_1["id"]}, {"id": time_entry_2["id"]}, {"id": time_entry_3["id"]}],
                                "project_tasks": [{"id": project_task_1["id"]},
                                                  {"id": project_task_2["id"]},
                                                  {"id": project_task_3["id"]}],
                                "project_users": [{"id": project_user_1["id"]},
                                                  {"id": project_user_2["id"]}],
                                "estimate_messages": [{"id": estimate_message_1["id"]},
                                                      {"id": estimate_message_2["id"]},
                                                      {"id": estimate_message_3["id"]}],
                                "invoice_messages": [{"id": invoice_message_1["id"]},
                                                     {"id": invoice_message_2["id"]},
                                                     {"id": invoice_message_3["id"]}],
                                "expense_categories": [{"id": expense_category_1["id"]},
                                                       {"id": expense_category_2["id"]},
                                                       {"id": expense_category_3["id"]}],
                                "external_reference": [{"id": external_reference_1["id"]},
                                                        {"id": external_reference_2["id"]},
                                                        {"id": external_reference_3["id"]}],
                                "estimate_item_categories": [{"id": estimate_item_category_1["id"]},
                                                             {"id": estimate_item_category_2["id"]},
                                                             {"id": estimate_item_category_3["id"]}],
                                "invoice_item_categories": [{"id": invoice_item_category_1["id"]},
                                                            {"id": invoice_item_category_2["id"]},
                                                            {"id": invoice_item_category_3["id"]}]}

    @classmethod
    def tearDownClass(cls):
        logging.info("Start Teardown")
        for invoice in cls._teardown_delete['invoices']:
            delete_stream('invoices', invoice['id'])
        for expense in cls._teardown_delete['expenses']:
            delete_stream('expenses', expense['id'])
        # for project_user in cls._teardown_delete['project_users']:
        #     delete_project_user(cls._teardown_delete['projects'][0]['id'], project_user['id'])
        #     delete_project_user(cls._teardown_delete['projects'][0]['id'], project_user['id'])
        for project in cls._teardown_delete['projects']:
            delete_stream('projects', project['id'])
        for role in cls._teardown_delete['roles']:
            delete_stream('roles', role['id'])
        for estimate in cls._teardown_delete['estimates']:
            delete_stream('estimates', estimate['id'])
        for category in cls._teardown_delete['estimate_item_categories']:
            delete_stream('estimate_item_categories', category['id'])
        for category in cls._teardown_delete['invoice_item_categories']:
            delete_stream('invoice_item_categories', category['id'])
        for category in cls._teardown_delete['expense_categories']:
            delete_stream('expense_categories', category['id'])
        # Deleteing Clients also deletes: contacts
        for client in cls._teardown_delete['clients']: 
            delete_stream('clients', client['id'])
        # TODO cannot delete tasks b/c they have 'tracked time'
        # TODO cannot delete time_entry

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
        return "{}_bookmark_test".format(super().name())

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
        incremental_streams = {key for key, value in self.expected_replication_method().items()
                               if value == self.INCREMENTAL}

        # IF THERE ARE STREAMS THAT SHOULD NOT BE TESTED
        # REPLACE THE EMPTY SET BELOW WITH THOSE STREAMS
        untested_streams = self.child_streams().union({
            "estimate_messages", # TODO - BUG (https://github.com/singer-io/tap-harvest/issues/35)
            "invoice_messages", # BUG (see ^ )
            "invoice_payments",# BUG (see ^^ )
            "projects", # Limited to 2 projects on free plan
            "users", # Limited to a single user on the free plan
            "user_projects", # Limited by projects
            "project_users", # Limited by users
            "user_project_tasks"}) # Limited by user_projects
        
        # If a stream does not have a replication key place it in this set
        no_replication_key = ("estimate_line_items",
                              "external_reference",
                              "invoice_line_items",
                              "time_entry_external_reference",
                              "user_roles")
        
        our_catalogs = [catalog for catalog in found_catalogs if
                        catalog.get('tap_stream_id') in incremental_streams.difference(
                            untested_streams)]

        expected = {"clients":[],
                    "contacts":[],
                    "estimate_item_categories":[],
                    "estimate_line_items":[],
                    "estimate_messages":[],
                    "estimates":[],
                    "expense_categories":[],
                    "expenses":[],
                    "external_reference":[], # TODO - BUG (https://github.com/singer-io/tap-harvest/issues/36)
                    "invoice_item_categories":[],
                    "invoice_line_items":[],
                    "invoice_messages":[],
                    "invoice_payments":[],
                    "invoices":[],
                    "project_tasks":[], # task_assignments
                    "project_users":[],
                    "projects":[],
                    "roles":[],
                    "tasks":[],
                    "time_entries":[],
                    "time_entry_external_reference":[],
                    "user_project_tasks": [],
                    "user_projects":[],
                    "user_roles":[],
                    "users":[]}

        # Run a sync job using orchestrator
        first_sync_record_count = self.run_sync(conn_id)

        # verify that the sync only sent records to the target for selected streams (catalogs)
        self.assertEqual(set(first_sync_record_count.keys()).difference(untested_streams),
                         incremental_streams.difference(untested_streams))

        first_sync_state = menagerie.get_state(conn_id)

        # Get data about actual rows synced
        first_sync_records = runner.get_records_from_target_output()
        first_max_bookmarks = self.max_bookmarks_by_stream(first_sync_records)
        first_min_bookmarks = self.min_bookmarks_by_stream(first_sync_records)

        # Insert Data prior to the 2nd sync
        logging.info("Updating clients")
        client_id = self._teardown_delete['clients'][0]['id']
        updated_client = update_client(client_id)
        expected['clients'].append({"id": client_id})

        logging.info(updated_client['updated_at'])
        
        logging.info("Updating contacts")
        contact_id = self._teardown_delete['contacts'][0]['id']
        updated_contact = update_contact(contact_id)
        expected['contacts'].append({"id": contact_id})

        logging.info(updated_contact['updated_at'])
        
        logging.info("Updating estimates")
        estimate_id = self._teardown_delete['estimates'][0]['id']
        updated_estimate = update_estimate(estimate_id)
        expected['estimates'].append({"id": estimate_id})

        logging.info(updated_estimate['updated_at'])
        
        logging.info("'Updating' estimate_messages")
        updated_estimate_message = update_estimate_message(estimate_id)
        expected['estimate_messages'].append({"id": updated_estimate_message['id']})

        logging.info(updated_estimate_message['updated_at'])
        
        logging.info("Updating estimate_line_items")
        expected['estimate_line_items'].append({"id": updated_estimate['line_items'][0]['id']})

        logging.info("Updating estimate_item_categories")
        category_id = self._teardown_delete['estimate_item_categories'][0]['id']        
        updated_category = update_estimate_item_category(category_id)
        expected['estimate_item_categories'].append({"id": category_id})

        logging.info(updated_category['updated_at'])
        
        logging.info("Updating invoices")
        invoice_id = self._teardown_delete['invoices'][0]['id']
        updated_invoice = update_invoice(invoice_id)
        expected['invoices'].append({"id": invoice_id})

        logging.info(updated_invoice['updated_at'])
        
        logging.info("Updating invoice_payments")
        updated_payment = update_invoice_payment(invoice_id)
        expected['invoice_payments'].append({"id": updated_payment['id']})
        
        logging.info(updated_payment['updated_at'])

        logging.info("Updating invoice_messages")
        updated_message = update_invoice_message(invoice_id)
        expected['invoice_messages'].append({"id": updated_message['id']})

        logging.info("Updating invoice_line_items")
        expected['invoice_line_items'].append({"id": updated_invoice['line_items'][0]['id']})

        logging.info(updated_message['updated_at'])
        
        logging.info("Updating invoice_item_categories")
        category_id = self._teardown_delete['invoice_item_categories'][0]['id']        
        updated_category = update_invoice_item_category(category_id)
        expected['invoice_item_categories'].append({"id": category_id})

        logging.info(updated_category['updated_at'])
        
        logging.info("Updating roles")
        role_id = self._teardown_delete['roles'][0]['id']
        updated_role = update_role(role_id)
        expected['roles'].append({"id": role_id})

        logging.info(updated_role['updated_at'])

        logging.info("Updating user_roles")
        user_id = get_random('users')
        update_user_role = update_role(role_id, [])
        updated_user_role = update_role(role_id, [user_id])
        expected['user_roles'].append({"user_id": user_id, "role_id": role_id})

        logging.info(updated_user_role['updated_at'])
        
        # TODO - Why is this the same as user_roles update ^
        # logging.info("Updating user_project_tasks")
        # user_id = get_random('users')
        # update_user_role = update_role(role_id, [])
        # updated_user_role = update_role(role_id, [user_id])
        # expected['user_roles'].append({"roles": user_id, "role_id": role_id})

        # logging.info(updated_user_role['updated_at'])
        
        logging.info("Updating expenses")
        expense_id = self._teardown_delete['expenses'][0]['id']
        updated_expense = update_expense(expense_id)
        expected['expenses'].append({"id": expense_id})

        logging.info(updated_category['updated_at'])
        
        logging.info("Updating expense_categories")
        category_id = self._teardown_delete['expense_categories'][0]['id']
        updated_category = update_expense_category(category_id)
        expected['expense_categories'].append({"id": category_id})

        logging.info(updated_category['updated_at'])

        logging.info("Updating tasks")
        task_id = self._teardown_delete['tasks'][0]['id']
        updated_task = update_task(task_id)
        expected['tasks'].append({"id": task_id})

        logging.info(updated_category['updated_at'])
                
        # This is commented because we are not testing this stream and it affects time_entries and it's child streams
        # logging.info("Updating project_users")
        # project_id = self._teardown_delete["projects"][0]['id']
        # project_user_id = self._teardown_delete["project_users"][0]['id']
        # updated_project_user = update_project_user(project_id, project_user_id)
        # expected['project_users'].append({'id': updated_project_user['id']})
        
        # logging.info(updated_project_user['updated_at'])
        
        logging.info("Updating project_tasks (task_assignments)")
        project_id = self._teardown_delete["projects"][0]['id']
        project_task_id = self._teardown_delete["project_tasks"][0]['id']
        updated_project_task = update_project_task(project_id, project_task_id)
        expected['project_tasks'].append({"id": updated_project_task['id']})
        expected['user_project_tasks'].append({"project_task_id": updated_project_task['id'], "user_id": get_random("users")})

        logging.info(updated_project_task['updated_at'])

        logging.info("Updating time_entries")
        time_entry_id = self._teardown_delete['time_entries'][0]['id']
        updated_time_entry = update_time_entry(time_entry_id)
        expected['time_entries'].append({"id": time_entry_id})

        logging.info(updated_category['updated_at'])
        
        logging.info("Updating external_reference (time_entries)")
        external_reference_id = updated_time_entry['external_reference']['id']
        expected['external_reference'].append({"id": external_reference_id})

        logging.info("Updating time_entry_external_reference (time_entries)")
        expected['time_entry_external_reference'].append({"time_entry_id": time_entry_id,
                                                          "external_reference_id": external_reference_id})

        logging.info("Updated Expectations : \n{}".format(expected))

        # Ensure different updated_at times for updates and inserts
        sleep(2)

        # TODO BUG (https://github.com/singer-io/tap-harvest/issues/34)
        # Expect the tap to start from the last record it left off at in the 1st sync
        # expected['expenses'].append({"id": self._teardown_delete['expenses'][-1]['id']})
        # expected['estimates'].append({"id": self._teardown_delete['estimates'][-1]['id']})
        # expected['contacts'].append({"id": self._teardown_delete['contacts'][-1]['id']})
        # expected['roles'].append({"id": self._teardown_delete['roles'][-1]['id']})
        # expected['clients'].append({"id": self._teardown_delete['clients'][-1]['id']})
        # expected['invoices'].append({"id": self._teardown_delete['invoices'][-1]['id']})

        for stream, expect in self._teardown_delete.items():
            if expect:
                logging.info("last synced {}: {}".format(stream, expect[-1]))

        try:
            logging.info("Inserting roles")
            inserted_role = create_role()
            expected['roles'].append({"id": inserted_role['id']})
            self._teardown_delete['roles'].append({"id":inserted_role['id']})

            logging.info("'Inserting' (update_role) user_roles")
            user_id = get_random('users')
            inserted_user_role = update_role(inserted_role['id'], [user_id])
            expected['user_roles'].append({"user_id": user_id, "role_id": inserted_role['id']})
            
            logging.info("Inserting clients")
            inserted_client = create_client()
            expected['clients'].append({"id": inserted_client['id']})
            self._teardown_delete['clients'].append({"id":inserted_client['id']})

            logging.info("Inserting contacts")
            inserted_contact = create_contact(inserted_client['id'])
            expected['contacts'].append({"id": inserted_contact['id']})
            self._teardown_delete['contacts'].append({"id":inserted_contact['id']})
            
            logging.info("Inserting estimates")
            inserted_estimate = create_estimate(inserted_client['id'])
            expected['estimates'].append({"id": inserted_estimate['id']})
            self._teardown_delete['estimates'].append({"id": inserted_estimate['id']})

            logging.info("Inserting estimate_item_categories")
            inserted_category = create_estimate_item_category()
            expected['estimate_item_categories'].append({"id": inserted_category['id']})
            self._teardown_delete['estimate_item_categories'].append({"id": inserted_category['id']})
            
            logging.info("Inserting estimate_line_items")
            updated_estimate_line_item = inserted_estimate['line_items'][0]
            expected['estimate_line_items'].append({"id": updated_estimate_line_item['id']})

            logging.info("Inserting estimate_messages")
            inserted_estimate_message = create_estimate_message(inserted_estimate['id'])
            expected['estimate_messages'].append({"id": inserted_estimate_message['id']})

            logging.info("Inserting invoices")
            client_id = self._teardown_delete['clients'][0]['id']
            inserted_invoice = create_invoice(client_id=client_id,estimate_id=inserted_estimate['id'])
            expected['invoices'].append({"id": inserted_invoice['id']})
            self._teardown_delete['invoices'].append({"id":inserted_invoice['id']})

            logging.info("Inserting invoice_payments")
            inserted_payment = create_invoice_payment(inserted_invoice['id'])
            expected['invoice_payments'].append({"id": inserted_payment['id']})

            logging.info("Inserting invoice_line_items")
            updated_invoice_line_item = inserted_invoice['line_items'][0]
            expected['invoice_line_items'].append({"id": updated_invoice_line_item['id']})

            logging.info("Inserting invoice_messages")
            inserted_message = create_invoice_message(inserted_invoice['id'])
            expected['invoice_messages'].append({"id": inserted_message['id']})
            self._teardown_delete['invoice_messages'].append({"id":inserted_message['id']})
            
            logging.info("Inserting invoice_item_categories")
            inserted_category = create_invoice_item_category()
            expected['invoice_item_categories'].append({"id": inserted_category['id']})
            self._teardown_delete['invoice_item_categories'].append({"id":inserted_category['id']})

            logging.info("Inserting expenses")
            project_id = self._teardown_delete['projects'][0]['id']
            inserted_expense = create_expense(project_id)
            expected['expenses'].append({"id": inserted_expense['id']})
            self._teardown_delete['expenses'].append({"id": inserted_expense['id']})

            logging.info("Inserting expense_categories")
            inserted_category = create_expense_category()
            expected['expense_categories'].append({"id": inserted_category['id']})
            self._teardown_delete['expense_categories'].append({"id": inserted_category['id']})

            logging.info("Inserting tasks")
            inserted_task = create_task()
            expected['tasks'].append({"id": inserted_task['id']})
            self._teardown_delete['tasks'].append({"id": inserted_task['id']})

            logging.info("Inserting project_users)")
            project_id = self._teardown_delete['projects'][1]['id']
            inserted_project_user = create_project_user(project_id, get_random('users'))
            expected['project_users'].append({"id":inserted_project_user["id"]})

            logging.info("Inserting project_tasks (task_assingments)")
            inserted_project_task = create_project_task(project_id, inserted_task['id'])
            expected['project_tasks'].append({"id":inserted_project_task["id"]})
            expected['user_project_tasks'].append({"project_task_id": inserted_project_task['id'], "user_id": get_random("users")})

            logging.info("Inserting time_entries")
            task_id = inserted_task['id']
            inserted_time_entry = create_time_entry(project_id, task_id)
            expected['time_entries'].append({"id": inserted_time_entry['id']})
            self._teardown_delete['time_entries'].append({"id": inserted_time_entry['id']})
            
            logging.info("Inserting external_reference (time_entries)")
            inserted_external_reference = inserted_time_entry['external_reference']
            expected['external_reference'].append({"id": inserted_external_reference['id']})

            logging.info("Inserting time_entry_external_reference (time_entries)")
            expected['time_entry_external_reference'].append({"time_entry_id": time_entry_id,
                                                              "external_reference_id": external_reference_id})

            # Run a second sync job using orchestrator
            second_sync_record_count = self.run_sync(conn_id)

            # Get data about rows synced
            second_sync_records = runner.get_records_from_target_output()
            second_min_bookmarks = self.min_bookmarks_by_stream(second_sync_records)
            second_max_bookmarks = self.max_bookmarks_by_stream(second_sync_records)
            second_sync_state = menagerie.get_state(conn_id)    

        finally:
            logging.info("Data Inserted, 2nd Sync Completed")
            
        # THIS MAKES AN ASSUMPTION THAT CHILD STREAMS DO NOT HAVE BOOKMARKS.
        # ADJUST IF NECESSARY
        for stream in incremental_streams.difference(untested_streams):
            with self.subTest(stream=stream):
                # get bookmark values from state and target data
                stream_bookmark_key = self.expected_replication_keys().get(stream, set())
                assert len(
                    stream_bookmark_key) == 1  # There shouldn't be a compound replication key
                stream_bookmark_key = stream_bookmark_key.pop()
                state_value = first_sync_state.get(stream)
                final_state_value = second_sync_state.get(stream)
                target_value = first_max_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                target_min_value = first_min_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                final_bookmark = second_max_bookmarks.get(
                    stream, {None: None}).get(stream_bookmark_key)
                try:
                    # attempt to parse the bookmark as a date
                    if state_value:
                        if isinstance(state_value, str):
                            state_value = self.local_to_utc(parse(state_value))
                        if isinstance(state_value, int):
                            state_value = self.local_to_utc(dt.utcfromtimestamp(state_value))

                    if target_value:
                        if isinstance(target_value, str):
                            target_value = self.local_to_utc(parse(target_value))
                        if isinstance(target_value, int):
                            target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                    if target_min_value:
                        if isinstance(target_min_value, str):
                            target_min_value = self.local_to_utc(parse(target_min_value))
                        if isinstance(target_min_value, int):
                            target_min_value = self.local_to_utc(
                                dt.utcfromtimestamp(target_min_value))

                    if final_bookmark:
                        if isinstance(final_bookmark, str):
                            final_bookmark = self.local_to_utc(parse(final_bookmark))
                        if isinstance(final_bookmark, int):
                            final_bookmark = self.local_to_utc(
                                dt.utcfromtimestamp(final_bookmark))

                    if final_state_value:
                        if isinstance(final_state_value, str):
                            final_state_value = self.local_to_utc(parse(final_state_value))
                        if isinstance(final_state_value, int):
                            final_state_value = self.local_to_utc(
                                dt.utcfromtimestamp(final_state_value))

                except (OverflowError, ValueError, TypeError):
                    print("bookmarks cannot be converted to dates, comparing values directly")

                if stream not in no_replication_key:
                    # verify that there is data with different bookmark values - setup necessary
                    self.assertGreater(target_value, target_min_value,
                                       msg="Data isn't set up to be able to test bookmarks")

                    # verify state agrees with target data after 1st sync
                    self.assertEqual(state_value, target_value,
                                     msg="The bookmark value isn't correct based on target data")

                # verify that you get less data the 2nd time around
                self.assertGreater(
                    first_sync_record_count.get(stream, 0),
                    second_sync_record_count.get(stream, 0),
                    msg="second syc didn't have less records, bookmark usage not verified")

                # verify that the 2nd sync gets more than zero records
                self.assertGreater(
                    second_sync_record_count.get(stream, 0), 0,
                    msg="second syc didn't have any records")

                # verify the 2nd sync has specific inserted and updated records
                actual = second_sync_records.get(stream, {'messages': []}).get('messages')
                actual = [item["data"] for item in actual]
                for expected_key_values in expected[stream]:
                    matching = False
                    for record in actual:
                        matching = set(expected_key_values.items()).issubset(set(record.items()))
                        if matching:
                            break
                    self.assertTrue(matching)

                # verify all data from 2nd sync >= 1st bookmark
                if stream not in no_replication_key:
                    target_value = second_min_bookmarks.get(
                        stream, {None: None}).get(stream_bookmark_key)
                    try:
                        if target_value:
                            if isinstance(target_value, str):
                                target_value = self.local_to_utc(parse(target_value))
                            if isinstance(target_value, int):
                                target_value = self.local_to_utc(dt.utcfromtimestamp(target_value))

                    except (OverflowError, ValueError, TypeError):
                        print("bookmarks cannot be converted to dates, comparing values directly")

                    # verify that the minimum bookmark sent to the target for the second sync
                    # is greater than or equal to the bookmark from the first sync
                    # TODO - BUG this will fail if no streams are updated/created

                    self.assertGreater(final_bookmark, target_value)
                    self.assertGreaterEqual(target_value, state_value)

                    # Make sure the final bookmark is the latest updated at
                    # Since the api sends data in created_at desc order we made the updated_at not in order to test
                    # if we send the correct value even when the latest record doesn't have the latest updatye_at value
                    self.assertEqual(final_bookmark, final_state_value)

SCENARIOS.add(BookmarkTest)
