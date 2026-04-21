import unittest
from unittest.mock import patch, MagicMock
from tap_harvest.streams.abstracts import (
    BaseStream,
    IncrementalStream,
    ParentBaseStream,
    ChildBaseStream,
)


# ---------------------------------------------------------------------------
# Concrete helpers
# ---------------------------------------------------------------------------

def _make_catalog(schema_dict=None, metadata=None):
    catalog = MagicMock()
    catalog.schema.to_dict.return_value = schema_dict or {"properties": {}}
    catalog.metadata = metadata or []
    return catalog


class ConcreteIncremental(IncrementalStream):
    tap_stream_id = "test_stream"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "items"
    path = "items"


class ConcreteChild(ChildBaseStream):
    tap_stream_id = "child_stream"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "children"
    path = "parents/{0}/children"


# ---------------------------------------------------------------------------
# BaseStream helpers
# ---------------------------------------------------------------------------

class TestAddObjectToId(unittest.TestCase):

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={})
    def setUp(self, _):
        self.stream = ConcreteIncremental(catalog=_make_catalog())
        self.stream.object_to_id = ["user", "project"]

    def test_adds_id_from_nested_object(self):
        record = {"user": {"id": 42}, "project": {"id": 7}}
        result = self.stream.add_object_to_id(record)
        self.assertEqual(result["user_id"], 42)
        self.assertEqual(result["project_id"], 7)

    def test_sets_id_to_none_when_object_is_none(self):
        record = {"user": None, "project": {"id": 7}}
        result = self.stream.add_object_to_id(record)
        self.assertIsNone(result["user_id"])
        self.assertEqual(result["project_id"], 7)


class TestAppendTimesToDates(unittest.TestCase):

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={})
    def setUp(self, _):
        self.stream = ConcreteIncremental(catalog=_make_catalog())
        self.stream.date_fields = ["start_date"]

    def test_converts_date_string(self):
        record = {"start_date": "2024-01-15"}
        self.stream.append_times_to_dates(record)
        # Should still be a string and non-empty
        self.assertIsInstance(record["start_date"], str)
        self.assertTrue(len(record["start_date"]) > 0)

    def test_skips_missing_date_field(self):
        record = {}
        # Should not raise even if date_field is absent
        self.stream.append_times_to_dates(record)


class TestUpdateParams(unittest.TestCase):

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={})
    def setUp(self, _):
        self.stream = ConcreteIncremental(catalog=_make_catalog())

    def test_updates_params_when_support_filter(self):
        self.stream.support_filter = True
        self.stream.update_params(updated_since="2024-01-01", page=1)
        self.assertEqual(self.stream.params["updated_since"], "2024-01-01")
        self.assertEqual(self.stream.params["page"], 1)

    def test_does_not_update_params_when_filter_disabled(self):
        self.stream.support_filter = False
        self.stream.update_params(updated_since="2024-01-01")
        self.assertNotIn("updated_since", self.stream.params)


class TestGetRecords(unittest.TestCase):

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={})
    def setUp(self, _):
        self.mock_client = MagicMock()
        self.stream = ConcreteIncremental(
            client=self.mock_client, catalog=_make_catalog()
        )

    def test_paginates_until_no_next_page(self):
        self.mock_client.get.side_effect = [
            {"items": [{"id": 1}, {"id": 2}], "next_page": 2},
            {"items": [{"id": 3}], "next_page": None},
        ]
        records = list(self.stream.get_records())
        self.assertEqual(len(records), 3)
        self.assertEqual(self.mock_client.get.call_count, 2)

    def test_returns_empty_when_no_records(self):
        self.mock_client.get.return_value = {"items": [], "next_page": None}
        records = list(self.stream.get_records())
        self.assertEqual(records, [])


# ---------------------------------------------------------------------------
# IncrementalStream.get_bookmark / write_bookmark
# ---------------------------------------------------------------------------

class TestIncrementalStreamBookmarks(unittest.TestCase):

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={})
    def setUp(self, _):
        self.mock_client = MagicMock()
        self.mock_client.config = {"start_date": "2024-01-01T00:00:00Z"}
        self.stream = ConcreteIncremental(
            client=self.mock_client, catalog=_make_catalog()
        )

    def test_get_bookmark_returns_start_date_when_no_bookmark_in_state(self):
        state = {}
        result = self.stream.get_bookmark(state, "test_stream")
        self.assertEqual(result, "2024-01-01T00:00:00Z")

    def test_get_bookmark_returns_existing_bookmark(self):
        state = {
            "bookmarks": {
                "test_stream": {"updated_at": "2024-06-01T00:00:00Z"}
            }
        }
        result = self.stream.get_bookmark(state, "test_stream")
        self.assertEqual(result, "2024-06-01T00:00:00Z")

    def test_write_bookmark_uses_max(self):
        state = {
            "bookmarks": {
                "test_stream": {"updated_at": "2024-03-01T00:00:00Z"}
            }
        }
        # New value earlier than existing — existing should win
        result = self.stream.write_bookmark(
            state, "test_stream", value="2024-01-01T00:00:00Z"
        )
        self.assertEqual(
            result["bookmarks"]["test_stream"]["updated_at"],
            "2024-03-01T00:00:00Z",
        )

    def test_write_bookmark_advances_bookmark(self):
        state = {
            "bookmarks": {
                "test_stream": {"updated_at": "2024-01-01T00:00:00Z"}
            }
        }
        # New value later than existing — new value should win
        result = self.stream.write_bookmark(
            state, "test_stream", value="2024-06-01T00:00:00Z"
        )
        self.assertEqual(
            result["bookmarks"]["test_stream"]["updated_at"],
            "2024-06-01T00:00:00Z",
        )

    def test_write_bookmark_noop_when_no_replication_keys(self):
        self.stream.replication_keys = None
        state = {}
        result = self.stream.write_bookmark(state, "test_stream", value="2024-01-01")
        self.assertEqual(result, state)


# ---------------------------------------------------------------------------
# ChildBaseStream
# ---------------------------------------------------------------------------

class TestChildBaseStream(unittest.TestCase):

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={})
    def setUp(self, _):
        self.mock_client = MagicMock()
        self.mock_client.base_url = "https://api.harvestapp.com/v2"
        self.mock_client.config = {"start_date": "2024-01-01T00:00:00Z"}
        self.stream = ConcreteChild(
            client=self.mock_client, catalog=_make_catalog()
        )

    def test_get_url_endpoint_formats_parent_id(self):
        parent_obj = {"id": 123}
        url = self.stream.get_url_endpoint(parent_obj)
        self.assertIn("123", url)

    def test_get_bookmark_is_singleton(self):
        """Second call to get_bookmark returns cached value without re-reading state."""
        state = {"bookmarks": {"child_stream": {"updated_at": "2024-05-01T00:00:00Z"}}}
        first = self.stream.get_bookmark(state, "child_stream")
        # Mutate state so a fresh read would differ
        state["bookmarks"]["child_stream"]["updated_at"] = "2025-01-01T00:00:00Z"
        second = self.stream.get_bookmark(state, "child_stream")
        self.assertEqual(first, second)
