import unittest
from unittest.mock import MagicMock, patch
from singer.catalog import Catalog

from tap_harvest.discover import discover, check_stream_access
from tap_harvest.exceptions import HarvestUnauthorizedError, HarvestForbiddenError, HarvestNotFoundError
from tap_harvest.streams import STREAMS


def _make_mock_schemas(stream_names):
    schema = {
        "type": "object",
        "properties": {"id": {"type": "integer"}, "updated_at": {"type": "string"}},
    }
    mdata = [
        {
            "breadcrumb": [],
            "metadata": {
                "table-key-properties": ["id"],
                "forced-replication-method": "INCREMENTAL",
                "valid-replication-keys": ["updated_at"],
            },
        }
    ]
    return (
        {name: schema for name in stream_names},
        {name: mdata for name in stream_names},
    )


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Tests for the check_stream_access helper in tap_harvest.discover."""

    def test_top_level_stream_returns_true_when_accessible(self):
        client = MagicMock()
        result = check_stream_access(client, "clients", STREAMS["clients"])
        self.assertTrue(result)
        client.get.assert_called_once()

    def test_returns_false_on_401(self):
        client = MagicMock()
        client.get.side_effect = HarvestUnauthorizedError("401")
        result = check_stream_access(client, "clients", STREAMS["clients"])
        self.assertFalse(result)

    def test_returns_false_on_403(self):
        client = MagicMock()
        client.get.side_effect = HarvestForbiddenError("403")
        result = check_stream_access(client, "invoices", STREAMS["invoices"])
        self.assertFalse(result)

    def test_returns_false_on_404(self):
        client = MagicMock()
        client.get.side_effect = HarvestNotFoundError("404")
        result = check_stream_access(client, "estimate_line_items", STREAMS["estimate_line_items"])
        self.assertFalse(result)

    def test_reraises_other_errors(self):
        client = MagicMock()
        client.get.side_effect = ConnectionError("timeout")
        with self.assertRaises(ConnectionError):
            check_stream_access(client, "clients", STREAMS["clients"])

    def test_probe_uses_per_page_1(self):
        """Probe uses per_page=1 for minimal data fetch."""
        client = MagicMock()
        check_stream_access(client, "clients", STREAMS["clients"])
        call_kwargs = client.get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        self.assertEqual(params.get("per_page"), 1)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):
    """Tests for tap_harvest.discover.discover()."""

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_discover_returns_catalog(self, mock_get_schemas, mock_check):
        """discover() returns a Catalog object."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients"])
        mock_check.return_value = True

        catalog = discover(MagicMock())

        self.assertIsInstance(catalog, Catalog)
        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].stream, "clients")
        self.assertEqual(catalog.streams[0].tap_stream_id, "clients")

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_discover_sets_key_properties(self, mock_get_schemas, mock_check):
        """discover() sets key_properties from metadata table-key-properties."""
        mock_get_schemas.return_value = _make_mock_schemas(["roles"])
        mock_check.return_value = True

        catalog = discover(MagicMock())
        self.assertEqual(catalog.streams[0].key_properties, ["id"])

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_discover_multiple_streams(self, mock_get_schemas, mock_check):
        """discover() handles multiple streams without error."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients", "projects", "tasks"])
        mock_check.return_value = True

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertEqual(stream_ids, {"clients", "projects", "tasks"})

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_child_stream_excluded_when_parent_inaccessible(self, mock_get_schemas, mock_check):
        """Child streams are excluded from the catalog when their parent stream is inaccessible."""
        # invoices (parent) + invoice_payments (child) + clients (accessible top-level)
        mock_get_schemas.return_value = _make_mock_schemas(["invoices", "invoice_payments", "clients"])
        # invoices blocked, clients accessible — ensures catalog is non-empty so no exception fires
        mock_check.side_effect = lambda client, name, cls: name != "invoices"

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("invoices", stream_ids)
        self.assertNotIn("invoice_payments", stream_ids)
        self.assertIn("clients", stream_ids)

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_child_stream_included_when_parent_accessible(self, mock_get_schemas, mock_check):
        """Child streams are included when their parent stream is accessible."""
        mock_get_schemas.return_value = _make_mock_schemas(["invoices", "invoice_payments"])
        mock_check.return_value = True  # invoices accessible

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertIn("invoices", stream_ids)
        self.assertIn("invoice_payments", stream_ids)

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_inaccessible_stream_excluded(self, mock_get_schemas, mock_check):
        """Streams that fail the access check are excluded from the catalog."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients", "projects", "tasks"])
        mock_check.side_effect = lambda client, name, cls: name != "clients"

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("clients", stream_ids)
        self.assertEqual(stream_ids, {"projects", "tasks"})

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_all_inaccessible_raises_exception(self, mock_get_schemas, mock_check):
        """When all streams are inaccessible, discover() raises an exception."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients", "projects"])
        mock_check.return_value = False

        with self.assertRaises(Exception) as ctx:
            discover(MagicMock())
        self.assertIn("No stream endpoints are accessible", str(ctx.exception))

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_warning_logged_for_excluded_stream(self, mock_get_schemas, mock_check):
        """A warning is logged for each excluded stream."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients", "projects"])
        mock_check.side_effect = lambda client, name, cls: name != "clients"

        with patch("tap_harvest.discover.LOGGER") as mock_logger:
            discover(MagicMock())

        warned_streams = [
            call.args[1] for call in mock_logger.warning.call_args_list
        ]
        self.assertIn("clients", warned_streams)
        self.assertNotIn("projects", warned_streams)


if __name__ == "__main__":
    unittest.main()
