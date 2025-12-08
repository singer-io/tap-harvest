import unittest
from unittest.mock import patch, MagicMock
from tap_harvest.streams.abstracts import ParentBaseStream

# Create a concrete subclass of ParentBaseStream for testing
class ConcreteParentBaseStream(ParentBaseStream):
    @property
    def key_properties(self):
        return ["id"]

    @property
    def replication_keys(self):
        return ["updated_at"]

    @property
    def replication_method(self):
        return "INCREMENTAL"

    @property
    def tap_stream_id(self):
        return "parent_stream"

class TestParentBaseStream(unittest.TestCase):
    
    @patch("tap_harvest.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        # Create a mock instance of ParentBaseStream
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        self.stream = ConcreteParentBaseStream(catalog=mock_catalog)
        self.stream.child_to_sync = []

    @patch("tap_harvest.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_harvest.streams.abstracts.IncrementalStream.get_bookmark", return_value = 100)
    def test_get_bookmark_parent_only_with_selected(self, mock_get_bookmark, mock_is_selected):

        # Call the method
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        # Assertions
        mock_get_bookmark.assert_called_once_with(state, "parent_stream")
        self.assertEqual(result, 100)

    @patch("tap_harvest.streams.abstracts.BaseStream.is_selected", return_value=False)
    @patch("tap_harvest.streams.abstracts.IncrementalStream.get_bookmark", return_value = 100)
    def test_get_bookmark_parent_only_but_not_selected(self, mock_get_bookmark, mock_is_selected):

        # Call the method
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        # Assertions
        self.assertEqual(result, None)

    @patch("tap_harvest.streams.abstracts.BaseStream.is_selected", return_value=True)
    @patch("tap_harvest.streams.abstracts.IncrementalStream.get_bookmark", side_effect = [100, 50, 75])
    def test_get_bookmark_with_children(self, mock_get_bookmark, mock_is_selected):

        # Add child streams
        child1 = MagicMock()
        child1.tap_stream_id = "child_stream_1"
        child2 = MagicMock()
        child2.tap_stream_id = "child_stream_2"
        self.stream.child_to_sync = [child1, child2]

        # Call the method
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        # Assertions
        self.assertEqual(mock_get_bookmark.call_count, 3)
        mock_get_bookmark.assert_any_call(state, "parent_stream")
        mock_get_bookmark.assert_any_call(
            state, "child_stream_1", key="parent_stream_updated_at"
        )
        mock_get_bookmark.assert_any_call(
            state, "child_stream_2", key="parent_stream_updated_at"
        )
        self.assertEqual(result, 50)  # Minimum of 100, 50, and 75

    @patch("tap_harvest.streams.abstracts.BaseStream.is_selected", return_value=False)
    @patch("tap_harvest.streams.abstracts.IncrementalStream.get_bookmark", side_effect = [75, 50])
    def test_get_bookmark_only_children_selected(self, mock_get_bookmark, mock_is_selected):

        # Add child streams
        child1 = MagicMock()
        child1.tap_stream_id = "child_stream_1"
        child2 = MagicMock()
        child2.tap_stream_id = "child_stream_2"
        self.stream.child_to_sync = [child1, child2]

        # Call the method
        state = {}
        result = self.stream.get_bookmark(state, "parent_stream")

        # Assertions
        self.assertEqual(mock_get_bookmark.call_count, 2)
        mock_get_bookmark.assert_any_call(
            state, "child_stream_1", key="parent_stream_updated_at"
        )
        mock_get_bookmark.assert_any_call(
            state, "child_stream_2", key="parent_stream_updated_at"
        )
        self.assertEqual(result, 50)  # Minimum of 100, 50, and 75
