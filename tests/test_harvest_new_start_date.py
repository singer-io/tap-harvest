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


    def get_second_updated_at(self, records):
        # get the invoice id that is perviously updated than 1st invoice
        first_invoice_updated_at = records.get("invoices").get("messages")[0].get("data").get("updated_at")
        for invoice in records.get("invoices").get("messages"):
            if invoice.get("data").get("updated_at") < first_invoice_updated_at:
                return invoice.get("data").get("id")

    def do_test(self, conn_id):
        sync_record_count = self.run_sync(conn_id)

        # Count actual rows synced
        sync_records = runner.get_records_from_target_output()

        all_invoice_ids = [invoice["id"] for invoice in self._teardown_delete["invoices"]]

        for stream in self.expected_streams():
            if stream not in ["invoice_payments"]:
                continue

            # getting invoice ids of all the invoice_payments
            invoice_ids_collected_from_sync = [invoice.get("data").get("invoice_id") for invoice in sync_records.get("invoice_payments").get("messages")]

            record_count_sync = sync_record_count.get(stream, 0)
            self.assertGreater(record_count_sync, 1)

            # check if all the invoices we created
            # are collected in the invoice payment data
            for all_invoice_id in all_invoice_ids:
                self.assertTrue(all_invoice_id in invoice_ids_collected_from_sync)

            second_updated_invoice_id = self.get_second_updated_at(sync_records)

            # check if the second highest updated invoice is present
            # in the invoice ids collected from invoice payments
            self.assertTrue(second_updated_invoice_id in invoice_ids_collected_from_sync)

SCENARIOS.add(TestNewStartDate)
