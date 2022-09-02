import datetime
from tap_tester import menagerie

from harvest_api import *
from base import BaseTapTest

class TestFutureDate(BaseTapTest):
    """
    Test that the start_date is respected for some streams.
    """

    def name(self):
        return "tap_tester_harvest_future_date"

    def do_test(self, conn_id):
        state = {}
        state_streams = self.expected_streams() - {"estimate_line_items", "external_reference", "invoice_line_items", "time_entry_external_reference", "user_project_tasks", "user_roles"}

        # Add the next day as the state for all streams
        for state_stream in state_streams:
            state[state_stream] = datetime.datetime.strftime(datetime.datetime.today() + datetime.timedelta(days=1), "%Y-%m-%dT00:00:00Z")

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_all_streams_and_fields(conn_id, found_catalogs, select_all_fields=True)

        # Set state for running sync mode
        menagerie.set_state(conn_id, state)

        # Run sync mode
        self.run_sync(conn_id)

        # Get the state after running sync mode
        latest_state = menagerie.get_state(conn_id)

        # Verify the child streams have the state in the latest state
        for stream in ["invoice_payments", "invoice_messages", "user_projects", "estimate_messages"]:
            self.assertIsNotNone(latest_state.get(stream))
