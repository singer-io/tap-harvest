"""
Test that the start_date is respected for some streams
"""
from tap_tester import runner, menagerie

from harvest_api import *
from base import BaseTapTest

class TestStartDateHonoring(BaseTapTest):

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

        client_1 = create_client()
        cls._teardown_delete["clients"].append({"id": client_1["id"]})

        project_1 = create_project(client_1['id'])
        cls._teardown_delete["projects"].append({"id": project_1["id"]})

        # Create dummy data in specifc streams     
        for itter in range(3):
            logging.info("Creating {} round(s) of data ...".format(itter + 1))

            estimate_1 = create_estimate(client_1['id'])
            estimate_message_1 = create_estimate_message(estimate_1['id'])
            cls._teardown_delete["estimates"].append({"id": estimate_1['id']})
            cls._teardown_delete["estimate_messages"].append({"id": estimate_message_1['id']})

            invoice_1 = create_invoice(client_id=client_1['id'],
                                   project_id=project_1['id'])
            invoice_message_1 = create_invoice_message(invoice_1['id'])
            invoice_payment_1 = create_invoice_payment(invoice_1['id'])
            cls._teardown_delete["invoices"].append({"id": invoice_1["id"]})
            cls._teardown_delete["invoice_messages"].append({"id": invoice_message_1["id"]})
            cls._teardown_delete["invoice_payments"].append({"id": invoice_payment_1["id"]})

    @classmethod
    def tearDownClass(cls):
        # Clean up the data created in the setup
        logging.info("Starting Teardown")
        for project in cls._teardown_delete['projects']:
            delete_stream('projects', project['id'])
        for invoice in cls._teardown_delete['invoices']:
            delete_stream('invoices', invoice['id'])
        for estimate in cls._teardown_delete['estimates']:
            delete_stream('estimates', estimate['id'])
        for client in cls._teardown_delete['clients']:
            delete_stream('clients', client['id'])

    def name(self):
        return "tap_tester_harvest_start_date_honoring"

    def get_second_updated_parent_id(self, records, stream):
        # get the invoice id or estimate id that is perviously updated than 1st id
        highest_updated_at = records.get(stream).get("messages")[0].get("data").get("updated_at")
        for data in records.get(stream).get("messages"):
            if data.get("data").get("updated_at") < highest_updated_at:
                return data.get("data").get("id")

    def do_test(self, conn_id):

        # table and field selection
        expected_streams = ["invoices", "invoice_payments", "invoice_messages", "estimates", "estimate_messages"]
        found_catalogs = menagerie.get_catalogs(conn_id)
        test_catalogs = [catalog for catalog in found_catalogs
                                      if catalog.get('stream_name') in expected_streams]
        self.select_all_streams_and_fields(conn_id, test_catalogs, select_all_fields=True)

        sync_record_count = self.run_sync(conn_id)

        # Count actual rows synced
        sync_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            if stream not in ["invoice_payments", "invoice_messages", "estimate_messages"]:
                continue

            if stream in ["invoice_payments", "invoice_messages"]:
                parent_stream = "invoices"
                parent_stream_pk = "invoice_id"
            if stream in ["estimate_messages"]:
                parent_stream = "estimates"
                parent_stream_pk = "estimate_id"

            # get invoice ids or estimate ids created during startup
            parent_ids_created_before_sync = [parent["id"] for parent in self._teardown_delete[parent_stream]]

            # getting invoice ids of all the invoice_payments, invoice_messages
            # or estimate ids of all estimate_messages

            parent_ids_collected_from_sync = [message.get("data").get(parent_stream_pk) for message in sync_records.get(stream).get("messages")]

            record_count_sync = sync_record_count.get(stream, 0)
            self.assertGreater(record_count_sync, 1)

            # check if all the invoice ids or estimate ids were collected in the synced records
            for ids in parent_ids_created_before_sync:
                self.assertTrue(ids in parent_ids_collected_from_sync)

            second_updated_parent_id = self.get_second_updated_parent_id(sync_records, parent_stream)

            # check if the second highest updated invoice ids or estimate ids
            # is present in the ids collected from the synced records
            self.assertTrue(second_updated_parent_id in parent_ids_collected_from_sync)
