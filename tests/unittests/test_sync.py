import unittest
from unittest.mock import patch, MagicMock
from tap_harvest.sync import sync, update_currently_syncing


class TestUpdateCurrentlySyncing(unittest.TestCase):
    """Tests for update_currently_syncing()."""

    @patch("tap_harvest.sync.singer.write_state")
    @patch("tap_harvest.sync.singer.set_currently_syncing")
    @patch("tap_harvest.sync.singer.get_currently_syncing", return_value=None)
    def test_sets_currently_syncing(
        self, mock_get, mock_set, mock_write
    ):
        """Sets currently_syncing in state when stream_name is provided."""
        state = {}
        update_currently_syncing(state, "my_stream")
        mock_set.assert_called_once_with(state, "my_stream")
        mock_write.assert_called_once_with(state)

    @patch("tap_harvest.sync.singer.write_state")
    @patch("tap_harvest.sync.singer.set_currently_syncing")
    @patch("tap_harvest.sync.singer.get_currently_syncing", return_value="old_stream")
    def test_clears_currently_syncing_when_stream_none(
        self, mock_get, mock_set, mock_write
    ):
        """Removes currently_syncing from state when stream_name is None."""
        state = {"currently_syncing": "old_stream"}
        update_currently_syncing(state, None)
        self.assertNotIn("currently_syncing", state)
        mock_write.assert_called_once_with(state)

    @patch("tap_harvest.sync.singer.write_state")
    @patch("tap_harvest.sync.singer.set_currently_syncing")
    @patch("tap_harvest.sync.singer.get_currently_syncing", return_value=None)
    def test_no_key_deletion_when_already_empty(
        self, mock_get, mock_set, mock_write
    ):
        """Does not raise if currently_syncing is not present and stream is None."""
        state = {}
        # when stream_name=None and get_currently_syncing returns None, no deletion
        update_currently_syncing(state, None)
        mock_write.assert_called_once_with(state)


class TestSync(unittest.TestCase):
    """Tests for sync()."""

    def _make_catalog(self, stream_names):
        """Build a minimal mock catalog with the given selected streams."""
        catalog = MagicMock()

        stream_entries = []
        for name in stream_names:
            entry = MagicMock()
            entry.stream = name
            stream_entries.append(entry)

        catalog.get_selected_streams.return_value = stream_entries

        def get_stream(name):
            mock_entry = MagicMock()
            mock_entry.stream = name
            return mock_entry

        catalog.get_stream.side_effect = get_stream
        return catalog

    @patch("tap_harvest.sync.update_currently_syncing")
    @patch("tap_harvest.sync.write_schema")
    @patch("tap_harvest.sync.STREAMS")
    @patch("tap_harvest.sync.singer.get_currently_syncing", return_value=None)
    def test_sync_simple_stream(
        self, mock_gcs, mock_streams, mock_write_schema, mock_update_cs
    ):
        """Syncs a non-parent stream end-to-end."""
        mock_client = MagicMock()
        config = {"start_date": "2024-01-01T00:00:00Z"}
        catalog = self._make_catalog(["clients"])
        state = {}

        mock_stream_instance = MagicMock()
        mock_stream_instance.parent = None
        mock_stream_instance.sync.return_value = 5
        mock_streams.__getitem__.return_value = MagicMock(
            return_value=mock_stream_instance
        )
        mock_streams.__contains__ = lambda self_, item: True

        with patch("tap_harvest.sync.singer.Transformer") as mock_transformer_cls:
            mock_transformer = MagicMock()
            mock_transformer.__enter__ = MagicMock(return_value=mock_transformer)
            mock_transformer.__exit__ = MagicMock(return_value=False)
            mock_transformer_cls.return_value = mock_transformer

            sync(mock_client, config, catalog, state)

        mock_write_schema.assert_called_once()
        mock_stream_instance.sync.assert_called_once_with(
            state=state, transformer=mock_transformer
        )

    @patch("tap_harvest.sync.update_currently_syncing")
    @patch("tap_harvest.sync.write_schema")
    @patch("tap_harvest.sync.STREAMS")
    @patch("tap_harvest.sync.singer.get_currently_syncing", return_value=None)
    def test_sync_skips_child_stream_and_appends_parent(
        self, mock_gcs, mock_streams, mock_write_schema, mock_update_cs
    ):
        """Child streams are deferred; their parent is added to the sync list."""
        mock_client = MagicMock()
        config = {"start_date": "2024-01-01T00:00:00Z"}
        catalog = self._make_catalog(["time_entry_external_reference"])
        state = {}

        mock_stream_instance = MagicMock()
        mock_stream_instance.parent = "time_entries"
        mock_streams.__getitem__.return_value = MagicMock(
            return_value=mock_stream_instance
        )
        mock_streams.__contains__ = lambda self_, item: True

        # Stream for time_entries (the parent)
        mock_parent_instance = MagicMock()
        mock_parent_instance.parent = None
        mock_parent_instance.sync.return_value = 10

        def stream_factory(stream_name):
            if stream_name == "time_entries":
                return mock_parent_instance
            return mock_stream_instance

        mock_streams.__getitem__.side_effect = lambda name: MagicMock(
            return_value=stream_factory(name)
        )

        with patch("tap_harvest.sync.singer.Transformer") as mock_transformer_cls:
            mock_transformer = MagicMock()
            mock_transformer.__enter__ = MagicMock(return_value=mock_transformer)
            mock_transformer.__exit__ = MagicMock(return_value=False)
            mock_transformer_cls.return_value = mock_transformer

            sync(mock_client, config, catalog, state)

        # Parent stream should have been synced
        mock_parent_instance.sync.assert_called_once()
