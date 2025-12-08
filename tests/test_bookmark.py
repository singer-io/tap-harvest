from base import HarvestBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class HarvestBookMarkTest(BookmarkTest, HarvestBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    start_date = "2025-10-01T00:00:00Z"
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "projects": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "time_entries": {"updated_at": "2025-12-04T00:00:00.000000Z"},
            "clients": {"updated_at": "2025-12-04T00:00:00.000000Z"},
            "contacts": {"updated_at": "2025-12-04T00:00:00.000000Z"},
            "estimate_item_categories": {"updated_at": "2025-12-08T00:00:00.000000Z"},
            "estimate_messages": {"updated_at": "2025-12-01T00:00:00.000000Z"},
            "estimates": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "expense_categories": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "expenses": {"updated_at": "2025-12-01T00:00:00Z"},
            "invoice_item_categories": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "invoice_messages": {"updated_at":"2025-12-01T00:00:00.000000Z"},
            "invoice_payments": {"updated_at": "2025-12-01T00:00:00.000000Z"},
            "invoices": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "project_tasks": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "project_users": {"updated_at": "2025-10-01T00:00:00Z"},
            "roles": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "tasks": {"updated_at": "2025-12-05T00:00:00.000000Z"},
            "user_projects": {"updated_at": "2025-10-01T00:00:00.000000Z"},
        }
    }

    @staticmethod
    def name():
        return "tap_tester_harvest_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = {"users"}.union(
            HarvestBaseTest.get_child_streams_with_no_replication_keys()
        )
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""
        new_bookmarks = {
            "projects": {"updated_at": "2025-12-08T10:37:00.000000Z"},
            "time_entries": {"updated_at": "2025-12-08T05:00:00.000000Z"},
            "clients": {"updated_at": "2025-12-08T05:00:00.000000Z"},
            "contacts": {"updated_at": "2025-12-08T05:00:00.000000Z"},
            "estimate_item_categories": {"updated_at": "2025-12-08T10:28:04.000000Z"},
            "estimate_messages": {"updated_at": "2025-12-08T09:38:00.000000Z"},
            "estimates": {"updated_at": "2025-12-08T05:40:00.000000Z"},
            "expense_categories": {"updated_at": "2025-12-08T10:37:55.000000Z"},
            "expenses": {"updated_at": "2025-12-04T09:06:50.000000Z"},
            "invoice_item_categories": {"updated_at": "2025-12-08T10:26:40.000000Z"},
            "invoice_messages": {"updated_at": "2025-12-08T09:44:00.000000Z"},
            "invoice_payments": {"updated_at": "2025-12-08T09:44:00.000000Z"},
            "invoices": {"updated_at": "2025-12-08T05:40:00.000000Z"},
            "project_tasks": {"updated_at": "2025-12-08T10:37:00.000000Z"},
            "project_users": {"updated_at": "2025-10-08T05:00:00.000000Z"},
            "roles": {"updated_at": "2025-12-08T10:38:02.000000Z"},
            "tasks": {"updated_at": "2025-12-08T10:37:45.000000Z"},
            "user_projects": {"updated_at": "2025-12-04T06:34:50.000000Z"},
        }

        return new_bookmarks
