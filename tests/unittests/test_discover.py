import types
import unittest
from unittest.mock import MagicMock, patch
from singer.catalog import Catalog

from tap_harvest.discover import (
    _apply_access_checks,
    _prune_inaccessible_children,
    check_stream_access,
    discover,
)
from tap_harvest.exceptions import HarvestUnauthorizedError, HarvestForbiddenError, HarvestNotFoundError, HarvestError
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
        with self.assertRaises(HarvestUnauthorizedError):
            check_stream_access(client, "clients", STREAMS["clients"])

    def test_returns_false_on_403(self):
        client = MagicMock()
        client.get.side_effect = HarvestForbiddenError("403")
        result = check_stream_access(client, "invoices", STREAMS["invoices"])
        self.assertFalse(result)

    def test_reraises_on_404(self):
        client = MagicMock()
        client.get.side_effect = HarvestNotFoundError("404")
        with self.assertRaises(HarvestNotFoundError):
            check_stream_access(client, "estimate_line_items", STREAMS["estimate_line_items"])

    def test_reraises_other_errors(self):
        client = MagicMock()
        client.get.side_effect = ConnectionError("timeout")
        with self.assertRaises(ConnectionError):
            check_stream_access(client, "clients", STREAMS["clients"])

    def test_reraises_non_auth_harvest_error(self):
        """Non-auth HarvestErrors (e.g. 400) are re-raised by check_stream_access."""
        client = MagicMock()
        client.get.side_effect = HarvestError("bad request")
        with self.assertRaises(HarvestError):
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
    def test_virtual_child_stream_included_when_parent_accessible(self, mock_get_schemas, mock_check):
        """Virtual child streams (flat path, parent set) are included when parent is accessible."""
        # invoice_line_items has parent="invoices" but no '{}' in path
        mock_get_schemas.return_value = _make_mock_schemas(["invoices", "invoice_line_items"])
        mock_check.return_value = True  # invoices accessible

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertIn("invoices", stream_ids)
        self.assertIn("invoice_line_items", stream_ids)
        # check_stream_access must NOT be called for the child (no API probe)
        for call in mock_check.call_args_list:
            self.assertNotEqual(call.args[1], "invoice_line_items")

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_virtual_child_stream_excluded_when_parent_inaccessible(self, mock_get_schemas, mock_check):
        """Virtual child streams are excluded when their parent is inaccessible."""
        mock_get_schemas.return_value = _make_mock_schemas(["invoices", "invoice_line_items", "clients"])
        mock_check.side_effect = lambda client, name, cls: name != "invoices"

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn("invoices", stream_ids)
        self.assertNotIn("invoice_line_items", stream_ids)
        self.assertIn("clients", stream_ids)

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_all_inaccessible_raises_exception(self, mock_get_schemas, mock_check):
        """When all streams are inaccessible, discover() raises an exception."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients", "projects"])
        mock_check.return_value = False

        with self.assertRaises(HarvestForbiddenError) as ctx:
            discover(MagicMock())
        self.assertIn("do not have 'read' access to any", str(ctx.exception))

    @patch("tap_harvest.discover.check_stream_access")
    @patch("tap_harvest.discover.get_schemas")
    def test_warning_logged_for_excluded_stream(self, mock_get_schemas, mock_check):
        """An aggregated warning is logged for excluded top-level streams."""
        mock_get_schemas.return_value = _make_mock_schemas(["clients", "projects"])
        mock_check.side_effect = lambda client, name, cls: name != "clients"

        with patch("tap_harvest.discover.LOGGER") as mock_logger:
            discover(MagicMock())

        warned_streams = [call.args[1] for call in mock_logger.warning.call_args_list if len(call.args) > 1]
        self.assertIn("clients", warned_streams)

    @patch("tap_harvest.discover.check_stream_access", return_value=True)
    @patch("tap_harvest.discover.get_schemas")
    def test_discover_logs_and_raises_on_schema_error(self, mock_get_schemas, _mock_check):
        """discover() logs context and raises when schema parsing fails."""
        mock_get_schemas.return_value = ({"clients": {"bad": "schema"}}, {"clients": []})

        with patch("tap_harvest.discover.Schema.from_dict", side_effect=ValueError("bad schema")):
            with patch("tap_harvest.discover.LOGGER") as mock_logger:
                with self.assertRaises(ValueError):
                    discover(MagicMock())

        self.assertTrue(mock_logger.error.called)


class TestAccessCheckHelpers(unittest.TestCase):
    """Tests helper functions used by discovery."""

    @patch("tap_harvest.discover.LOGGER")
    def test_prune_inaccessible_children_removes_child_streams(self, mock_logger):
        schemas = {"invoice_payments": {}, "clients": {}}
        field_metadata = {"invoice_payments": [], "clients": []}

        pruned_children = _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("invoice_payments", schemas)
        self.assertNotIn("invoice_payments", field_metadata)
        self.assertEqual(pruned_children, ["invoice_payments"])
        mock_logger.warning.assert_called_once()

    @patch("tap_harvest.discover.STREAMS")
    def test_prune_inaccessible_children_handles_multi_level_chain_order_independent(self, mock_streams):
        mock_streams.items.return_value = [
            ("user_project_tasks", types.SimpleNamespace(parent="user_projects")),
            ("user_projects", types.SimpleNamespace(parent="users")),
            ("users", types.SimpleNamespace(parent="")),
        ]
        schemas = {"user_project_tasks": {}, "user_projects": {}}
        field_metadata = {"user_project_tasks": [], "user_projects": []}

        pruned_children = _prune_inaccessible_children(schemas, field_metadata)

        self.assertNotIn("user_projects", schemas)
        self.assertNotIn("user_project_tasks", schemas)
        self.assertCountEqual(pruned_children, ["user_projects", "user_project_tasks"])

    @patch("tap_harvest.discover.check_stream_access")
    def test_apply_access_checks_removes_inaccessible_top_level(self, mock_check):
        mock_check.side_effect = lambda client, name, cls: name != "clients"
        schemas = {"clients": {}, "projects": {}, "invoice_payments": {}}
        field_metadata = {"clients": [], "projects": [], "invoice_payments": []}

        with patch("tap_harvest.discover.LOGGER") as mock_logger:
            _apply_access_checks(MagicMock(), schemas, field_metadata)

        self.assertNotIn("clients", schemas)
        self.assertIn("projects", schemas)
        self.assertNotIn("invoice_payments", schemas)

        warning_messages = [
            call.args[1]
            for call in mock_logger.warning.call_args_list
            if len(call.args) > 1 and call.args[0] == "Unauthorized streams excluded from catalog: %s"
        ]
        self.assertIn("clients, invoice_payments", warning_messages)


if __name__ == "__main__":
    unittest.main()
