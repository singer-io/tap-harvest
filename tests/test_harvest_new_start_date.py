"""
Test that the start_date is respected for some streams
"""
from tap_tester import runner
from tap_tester.scenario import SCENARIOS

from harvest_api import *
from base import BaseTapTest

class TestNewStartDate(BaseTapTest):
    
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

            # estimate_1 = create_estimate(client_1['id'])
            # cls._teardown_delete["estimates"].append({"id": estimate_1['id']})
            # estimate_message_1 = create_estimate_message(estimate_1['id'])
            # cls._teardown_delete["estimate_messages"].append({"id": estimate_message_1['id']})

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
        for client in cls._teardown_delete['clients']:
            delete_stream('clients', client['id'])

    def name(self):
        return "tap_tester_harvest_new_start_date"


    def do_test(self, conn_id):
        sync_record_count = self.run_sync(conn_id)

        # Count actual rows synced
        sync_records = runner.get_records_from_target_output()

        # getting id of 2nd invoice because invoices are returned in order of created date
        second_invoice_id = sync_records.get("invoices").get("messages")[1].get("data").get("id")

        for stream in self.expected_streams():
            if stream not in ["invoice_payments"]:
                continue

            # getting invoice ids of all the invoice_payments
            invoice_ids = [invoice.get("data").get("invoice_id") for invoice in sync_records.get("invoice_payments").get("messages")]

            record_count_sync = sync_record_count.get(stream, 0)
            self.assertGreater(record_count_sync, 1)

            self.assertTrue(second_invoice_id in invoice_ids)

SCENARIOS.add(TestNewStartDate)
