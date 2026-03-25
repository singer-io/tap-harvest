import os
import json
import unittest
from unittest.mock import patch, mock_open, MagicMock

from tap_harvest.schema import get_abs_path, load_schema_references, get_schemas, write_schema
from tap_harvest.streams import STREAMS


class TestGetAbsPath(unittest.TestCase):
    """Tests for tap_harvest.schema.get_abs_path()."""

    def test_returns_absolute_path(self):

        result = get_abs_path("schemas/clients.json")
        self.assertTrue(os.path.isabs(result))
        self.assertTrue(result.endswith("schemas/clients.json") or
                        result.endswith(os.path.join("schemas", "clients.json")))


class TestLoadSchemaReferences(unittest.TestCase):
    """Tests for tap_harvest.schema.load_schema_references()."""

    def test_returns_empty_dict_when_shared_dir_missing(self):
        with patch("tap_harvest.schema.os.path.exists", return_value=False):
            result = load_schema_references()
        self.assertEqual(result, {})

    def test_loads_shared_files(self):
        fake_content = {"type": "object"}
        fake_files = ["address.json"]

        with patch("tap_harvest.schema.os.path.exists", return_value=True), \
             patch("tap_harvest.schema.os.listdir", return_value=fake_files), \
             patch("tap_harvest.schema.os.path.isfile", return_value=True), \
             patch("builtins.open", mock_open(read_data=json.dumps(fake_content))):
            result = load_schema_references()

        self.assertIn("shared/address.json", result)
        self.assertEqual(result["shared/address.json"], fake_content)


class TestGetSchemas(unittest.TestCase):
    """Tests for tap_harvest.schema.get_schemas()."""

    def test_get_schemas_returns_dicts(self):
        schemas, field_metadata = get_schemas()
        self.assertIsInstance(schemas, dict)
        self.assertIsInstance(field_metadata, dict)
        self.assertGreater(len(schemas), 0)
        self.assertEqual(schemas.keys(), field_metadata.keys())

    def test_get_schemas_contains_all_streams(self):
        schemas, field_metadata = get_schemas()
        for stream_name in STREAMS:
            self.assertIn(stream_name, schemas, f"Missing schema for '{stream_name}'")
            self.assertIn(stream_name, field_metadata, f"Missing metadata for '{stream_name}'")

    def test_each_schema_has_properties(self):
        schemas, _ = get_schemas()
        for stream_name, schema in schemas.items():
            self.assertIn("properties", schema, f"Schema for '{stream_name}' missing 'properties'")


class TestWriteSchema(unittest.TestCase):
    """Tests for tap_harvest.schema.write_schema()."""

    def _make_stream(self, children=None, selected=True):
        stream = MagicMock()
        stream.is_selected.return_value = selected
        stream.children = children or []
        return stream

    @patch("tap_harvest.schema.STREAMS")
    def test_write_schema_selected_stream(self, mock_streams):
        """write_schema() calls stream.write_schema() when stream is selected."""

        stream = self._make_stream(selected=True)
        mock_client = MagicMock()
        catalog = MagicMock()

        write_schema(stream, mock_client, [], catalog)

        stream.write_schema.assert_called_once()

    @patch("tap_harvest.schema.STREAMS")
    def test_write_schema_not_selected(self, mock_streams):
        """write_schema() does NOT call stream.write_schema() when stream is not selected."""

        stream = self._make_stream(selected=False)
        write_schema(stream, MagicMock(), [], MagicMock())

        stream.write_schema.assert_not_called()

    @patch("tap_harvest.schema.STREAMS")
    def test_write_schema_recurses_into_children(self, mock_streams):
        """write_schema() recurses into children and populates child_to_sync."""

        child_instance = MagicMock()
        child_instance.is_selected.return_value = True
        child_instance.children = []

        child_cls = MagicMock(return_value=child_instance)
        mock_streams.__getitem__.return_value = child_cls
        mock_streams.__contains__ = lambda self_, item: True

        parent_stream = self._make_stream(selected=True, children=["child_stream"])
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(parent_stream, MagicMock(), ["child_stream"], catalog)

        # child_to_sync should have one entry
        parent_stream.child_to_sync.append.assert_called_once_with(child_instance)
