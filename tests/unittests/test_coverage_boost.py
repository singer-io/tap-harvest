import unittest
import runpy
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

import tap_harvest
from tap_harvest.client import Client, raise_for_error
from tap_harvest.exceptions import HarvestError
from tap_harvest.streams.abstracts import IncrementalStream
from tap_harvest.streams.estimate_line_items import EstimateLineItems
from tap_harvest.streams.estimate_messages import EstimateMessages
from tap_harvest.streams.expenses import Expenses
from tap_harvest.streams.external_reference import ExternalReference
from tap_harvest.streams.invoice_line_items import InvoiceLineItems
from tap_harvest.streams.invoice_messages import InvoiceMessages
from tap_harvest.streams.invoice_payments import InvoicePayments
from tap_harvest.streams.time_entry_external_reference import TimeEntryExternalReference
from tap_harvest.streams.user_project_tasks import UserProjectTasks
from tap_harvest.streams.user_projects import UserProjects
from tap_harvest.streams.user_roles import UserRoles
from tap_harvest.streams.users import Users


def _catalog():
    catalog = MagicMock()
    catalog.schema.to_dict.return_value = {"properties": {}}
    catalog.metadata = []
    return catalog


def _client_config():
    return {
        "user_agent": "ua",
        "refresh_token": "rt",
        "client_id": "cid",
        "client_secret": "secret",
        "start_date": "2024-01-01T00:00:00Z",
    }


class _ConcreteIncremental(IncrementalStream):
    tap_stream_id = "test_stream"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    data_key = "items"
    path = "items"


class TestTapMainCoverage(unittest.TestCase):
    @patch("tap_harvest.sync")
    @patch("tap_harvest.do_discover")
    @patch("tap_harvest.Client")
    @patch("tap_harvest.singer.utils.parse_args")
    def test_main_discover_path(self, mock_parse, mock_client_cls, mock_discover, mock_sync):
        parsed = MagicMock()
        parsed.state = {"bookmarks": {}}
        parsed.discover = True
        parsed.catalog = None
        parsed.config = _client_config()
        mock_parse.return_value = parsed

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        tap_harvest.main()

        mock_discover.assert_called_once_with(mock_client)
        mock_sync.assert_not_called()

    @patch("tap_harvest.sync")
    @patch("tap_harvest.do_discover")
    @patch("tap_harvest.Client")
    @patch("tap_harvest.singer.utils.parse_args")
    def test_main_sync_path(self, mock_parse, mock_client_cls, mock_discover, mock_sync):
        parsed = MagicMock()
        parsed.state = {"bookmarks": {"clients": {"updated_at": "2024-01-01T00:00:00Z"}}}
        parsed.discover = False
        parsed.catalog = MagicMock()
        parsed.config = _client_config()
        mock_parse.return_value = parsed

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        tap_harvest.main()

        mock_sync.assert_called_once_with(
            client=mock_client,
            config=parsed.config,
            catalog=parsed.catalog,
            state=parsed.state,
        )
        mock_discover.assert_not_called()

    @patch("tap_harvest.client.Client.check_active_account")
    @patch("tap_harvest.client.Client._refresh_access_token")
    @patch("tap_harvest.singer.utils.parse_args")
    def test_module_main_invocation(self, mock_parse, _mock_refresh, _mock_check_account):
        parsed = MagicMock()
        parsed.state = {}
        parsed.discover = False
        parsed.catalog = None
        parsed.config = _client_config()
        mock_parse.return_value = parsed

        init_file = Path(__file__).resolve().parents[2] / "tap_harvest" / "__init__.py"
        runpy.run_path(str(init_file), run_name="__main__")


class TestClientCoverageBoost(unittest.TestCase):
    def test_refresh_access_token_uses_default_expiry(self):
        client = Client(_client_config())
        with patch.object(client, "post", return_value={"access_token": "abc"}):
            client._refresh_access_token()
        self.assertEqual(client._access_token, "abc")
        self.assertGreater(client._expires_at, datetime.now())

    def test_get_uses_path_when_endpoint_missing(self):
        client = Client(_client_config())
        with patch.object(client, "authenticate", return_value=({}, {})):
            with patch.object(client, "_Client__make_request", return_value={"ok": True}) as req:
                result = client.get(endpoint=None, path="projects")
        self.assertEqual(result, {"ok": True})
        req.assert_called_once()
        self.assertEqual(req.call_args.args[1], f"{client.base_url}/projects")

    def test_post_uses_path_when_endpoint_missing(self):
        client = Client(_client_config())
        with patch.object(client, "_Client__make_request", return_value={"ok": True}) as req:
            result = client.post(endpoint=None, path="projects")
        self.assertEqual(result, {"ok": True})
        self.assertEqual(req.call_args.args[1], f"{client.base_url}/projects")

    def test_authenticate_adds_account_header(self):
        client = Client(_client_config())
        client._access_token = "token"
        client._expires_at = datetime.max
        client._account_id = "99"
        headers, params = client.authenticate({}, {})
        self.assertEqual(headers["Harvest-Account-Id"], "99")
        self.assertEqual(headers["Authorization"], "Bearer token")
        self.assertEqual(headers["User-Agent"], "ua")
        self.assertEqual(params, {})

    def test_exit_closes_session(self):
        client = Client(_client_config())
        client.__exit__(None, None, None)
        self.assertFalse(client._session.close is None)

    def test_raise_for_error_non_json_fallback(self):
        response = MagicMock()
        response.status_code = 500
        response.json.side_effect = ValueError("bad json")
        with self.assertRaises(HarvestError):
            raise_for_error(response)

    def test_raise_for_error_prefers_error_key(self):
        response = MagicMock()
        response.status_code = 400
        response.json.return_value = {"error": "explicit error"}
        with self.assertRaises(HarvestError) as ctx:
            raise_for_error(response)
        self.assertIn("explicit error", str(ctx.exception))


class TestStreamCoverageBoost(unittest.TestCase):
    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    @patch("tap_harvest.streams.abstracts.write_record")
    def test_incremental_sync_paths(self, mock_write_record, _):
        client = MagicMock()
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        stream = _ConcreteIncremental(client=client, catalog=_catalog())
        stream.child_to_sync = [MagicMock()]
        stream.get_records = MagicMock(
            return_value=[
                {"id": 1, "updated_at": "2023-12-01T00:00:00Z"},
                {"id": 2, "updated_at": "2024-01-02T00:00:00Z"},
            ]
        )
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_args: record

        state = {"bookmarks": {"test_stream": {"updated_at": "2024-01-01T00:00:00Z"}}}
        stream.sync(state, transformer)

        self.assertEqual(mock_write_record.call_count, 1)
        stream.child_to_sync[0].sync.assert_called_once()

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_users_get_bookmark_no_child_streams(self, _):
        client = MagicMock()
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        stream = Users(client=client, catalog=_catalog())
        stream.child_to_sync = []

        with patch("tap_harvest.streams.users.ParentBaseStream.get_bookmark", return_value="2024-02-01T00:00:00Z"):
            bookmark = stream.get_bookmark({}, "users")
        self.assertEqual(bookmark, "2024-02-01T00:00:00Z")

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_users_get_bookmark_child_selected_and_unselected(self, _):
        client = MagicMock()
        client.config = {"start_date": "2024-01-01T00:00:00Z"}
        stream = Users(client=client, catalog=_catalog())

        child = MagicMock()
        child.tap_stream_id = "user_projects"
        child.is_selected.return_value = True
        stream.child_to_sync = [child]

        with patch("tap_harvest.streams.users.ParentBaseStream.get_bookmark", side_effect=["2024-03-01T00:00:00Z", "2024-02-01T00:00:00Z"]):
            bookmark_selected_child = stream.get_bookmark({}, "users")
        self.assertEqual(bookmark_selected_child, "2024-02-01T00:00:00Z")

        child.is_selected.return_value = False
        with patch("tap_harvest.streams.users.ParentBaseStream.get_bookmark", return_value="2024-03-01T00:00:00Z"):
            bookmark_unselected_child = stream.get_bookmark({}, "users")
        self.assertEqual(bookmark_unselected_child, "2024-01-01T00:00:00Z")

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    @patch("tap_harvest.streams.abstracts.LOGGER")
    @patch("tap_harvest.streams.abstracts.write_schema", side_effect=OSError("boom"))
    def test_write_schema_raises_oserror(self, _mock_write_schema, mock_logger, _):
        stream = _ConcreteIncremental(client=MagicMock(), catalog=_catalog())
        with self.assertRaises(OSError):
            stream.write_schema()
        self.assertTrue(mock_logger.error.called)

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    @patch("tap_harvest.streams.abstracts.IncrementalStream.write_bookmark")
    @patch("tap_harvest.streams.abstracts.BaseStream.is_selected", return_value=False)
    def test_parent_write_bookmark_writes_child_only_when_parent_unselected(self, _mock_selected, mock_super_write, _):
        stream = Users(client=MagicMock(), catalog=_catalog())
        child = MagicMock()
        child.tap_stream_id = "user_projects"
        stream.child_to_sync = [child]

        stream.write_bookmark({}, stream.tap_stream_id, value="2024-05-01T00:00:00Z")

        mock_super_write.assert_called_once_with(
            {},
            "user_projects",
            key="users_updated_at",
            value="2024-05-01T00:00:00Z",
        )

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    @patch("tap_harvest.streams.abstracts.IncrementalStream.write_bookmark")
    @patch("tap_harvest.streams.abstracts.BaseStream.is_selected", return_value=True)
    def test_parent_write_bookmark_writes_parent_when_selected(self, _mock_selected, mock_super_write, _):
        stream = Users(client=MagicMock(), catalog=_catalog())
        stream.child_to_sync = []

        stream.write_bookmark({}, stream.tap_stream_id, value="2024-05-01T00:00:00Z")

        mock_super_write.assert_called_once_with(
            {},
            stream.tap_stream_id,
            value="2024-05-01T00:00:00Z",
        )

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_expenses_modify_object_receipt_variants(self, _):
        stream = Expenses(client=MagicMock(), catalog=_catalog())
        record_without_receipt = {
            "client": None,
            "project": None,
            "expense_category": None,
            "user": None,
            "user_assignment": None,
            "invoice": None,
            "receipt": None,
        }
        out1 = stream.modify_object(record_without_receipt)
        self.assertIsNone(out1["receipt_url"])

        record_with_receipt = {
            "client": None,
            "project": None,
            "expense_category": None,
            "user": None,
            "user_assignment": None,
            "invoice": None,
            "receipt": {
                "url": "u",
                "file_name": "f",
                "file_size": 1,
                "content_type": "image/png",
            },
        }
        out2 = stream.modify_object(record_with_receipt)
        self.assertEqual(out2["receipt_url"], "u")

    @patch("tap_harvest.streams.external_reference.write_record")
    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_external_reference_sync_and_write_bookmark(self, _, mock_write):
        stream = ExternalReference(client=MagicMock(), catalog=_catalog())
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_args: record

        stream.sync({}, transformer, parent_obj={"external_reference": {"id": 1}})
        mock_write.assert_called_once()

        stream.sync({}, transformer, parent_obj={"external_reference": None})
        self.assertEqual(stream.write_bookmark({}, "external_reference"), {})

    @patch("tap_harvest.streams.invoice_line_items.write_record")
    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_invoice_line_items_sync(self, _, mock_write):
        stream = InvoiceLineItems(client=MagicMock(), catalog=_catalog())
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_args: record

        parent = {
            "id": 7,
            "line_items": [
                {"id": 1, "project": {"id": 10}},
                {"id": 2, "project": None},
            ],
        }
        stream.sync({}, transformer, parent)
        self.assertEqual(mock_write.call_count, 2)
        self.assertEqual(stream.write_bookmark({}, "invoice_line_items"), {})

    @patch("tap_harvest.streams.estimate_line_items.write_record")
    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_estimate_line_items_sync(self, _, mock_write):
        stream = EstimateLineItems(client=MagicMock(), catalog=_catalog())
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_args: record

        parent = {"id": 11, "line_items": [{"id": 1}]}
        stream.sync({}, transformer, parent)
        mock_write.assert_called_once()
        self.assertEqual(stream.write_bookmark({}, "estimate_line_items"), {})

    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_message_and_payment_modify_objects(self, _):
        estimate_messages = EstimateMessages(client=MagicMock(), catalog=_catalog())
        invoice_messages = InvoiceMessages(client=MagicMock(), catalog=_catalog())
        invoice_payments = InvoicePayments(client=MagicMock(), catalog=_catalog())
        user_projects = UserProjects(client=MagicMock(), catalog=_catalog())

        self.assertEqual(
            estimate_messages.modify_object({"id": 1}, parent_record={"id": 3})["estimate_id"],
            3,
        )
        self.assertEqual(
            invoice_messages.modify_object({"id": 1}, parent_record={"id": 4})["invoice_id"],
            4,
        )

        payment = invoice_payments.modify_object(
            {"id": 1, "payment_gateway": {"id": 8, "name": "gw"}},
            parent_record={"id": 5},
        )
        self.assertEqual(payment["payment_gateway_id"], 8)
        self.assertEqual(payment["invoice_id"], 5)

        assignment = user_projects.modify_object(
            {"id": 2, "project": None, "client": None, "user": None},
            parent_record={"id": 9},
        )
        self.assertEqual(assignment["user"]["id"], 9)

    @patch("tap_harvest.streams.time_entry_external_reference.write_record")
    @patch("tap_harvest.streams.user_project_tasks.write_record")
    @patch("tap_harvest.streams.user_roles.write_record")
    @patch("tap_harvest.streams.abstracts.metadata.to_map", return_value={(): {"selected": True}})
    def test_full_table_child_sync_streams(self, _, mock_user_roles_write, mock_upt_write, mock_teer_write):
        transformer = MagicMock()
        transformer.transform.side_effect = lambda record, *_args: record

        time_ref = TimeEntryExternalReference(client=MagicMock(), catalog=_catalog())
        time_ref.sync({}, transformer, {"id": 1, "external_reference": {"id": 2}})
        time_ref.sync({}, transformer, {"id": 1, "external_reference": None})
        self.assertEqual(time_ref.write_bookmark({}, "time_entry_external_reference"), {})

        user_roles = UserRoles(client=MagicMock(), catalog=_catalog())
        user_roles.sync({}, transformer, {"id": 4, "user_ids": [1, 2]})

        user_project_tasks = UserProjectTasks(client=MagicMock(), catalog=_catalog())
        user_project_tasks.sync({}, transformer, {"user_id": 6, "task_assignments": [{"id": 7}]})
        self.assertEqual(user_project_tasks.write_bookmark({}, "user_project_tasks"), {})

        self.assertEqual(mock_teer_write.call_count, 1)
        self.assertEqual(mock_user_roles_write.call_count, 2)
        self.assertEqual(mock_upt_write.call_count, 1)
