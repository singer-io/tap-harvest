import unittest
from unittest.mock import patch
from singer.catalog import Catalog

from tap_harvest.discover import discover


class TestDiscover(unittest.TestCase):
    """Tests for tap_harvest.discover.discover()."""

    @patch("tap_harvest.discover.get_schemas")
    def test_discover_returns_catalog(self, mock_get_schemas):
        """discover() returns a Catalog object."""
        mock_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}},
        }
        mock_mdata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": ["id"],
                    "forced-replication-method": "INCREMENTAL",
                    "valid-replication-keys": ["updated_at"],
                },
            }
        ]
        mock_get_schemas.return_value = (
            {"clients": mock_schema},
            {"clients": mock_mdata},
        )

        catalog = discover()

        self.assertIsInstance(catalog, Catalog)
        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].stream, "clients")
        self.assertEqual(catalog.streams[0].tap_stream_id, "clients")

    @patch("tap_harvest.discover.get_schemas")
    def test_discover_sets_key_properties(self, mock_get_schemas):
        """discover() sets key_properties from metadata table-key-properties."""
        mock_schema = {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
        }
        mock_mdata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": ["id"],
                    "forced-replication-method": "FULL_TABLE",
                    "valid-replication-keys": [],
                },
            }
        ]
        mock_get_schemas.return_value = (
            {"roles": mock_schema},
            {"roles": mock_mdata},
        )

        catalog = discover()
        self.assertEqual(catalog.streams[0].key_properties, ["id"])

    @patch("tap_harvest.discover.get_schemas")
    def test_discover_multiple_streams(self, mock_get_schemas):
        """discover() handles multiple streams without error."""
        make_schema = lambda: {
            "type": "object",
            "properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}},
        }
        make_mdata = lambda: [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": ["id"],
                    "forced-replication-method": "INCREMENTAL",
                    "valid-replication-keys": ["updated_at"],
                },
            }
        ]
        schemas = {name: make_schema() for name in ["clients", "projects", "tasks"]}
        field_metadata = {name: make_mdata() for name in ["clients", "projects", "tasks"]}
        mock_get_schemas.return_value = (schemas, field_metadata)

        catalog = discover()
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, {"clients", "projects", "tasks"})
