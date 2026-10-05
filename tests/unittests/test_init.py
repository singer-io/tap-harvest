import io
import json
import unittest
from unittest.mock import patch, MagicMock
import tap_harvest


class TestDoDiscover(unittest.TestCase):
    """Tests for tap_harvest.do_discover()."""

    @patch("tap_harvest.discover")
    def test_do_discover_dumps_catalog(self, mock_discover):
        """do_discover() serialises catalog to stdout."""
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog
        mock_client = MagicMock()

        captured = io.StringIO()
        with patch("sys.stdout", captured):
            tap_harvest.do_discover(mock_client)

        mock_discover.assert_called_once_with(mock_client)
        mock_catalog.to_dict.assert_called_once()
        output = json.loads(captured.getvalue())
        self.assertEqual(output, {"streams": []})

    @patch("tap_harvest.discover")
    def test_do_discover_logs_start_and_finish(self, mock_discover):
        """do_discover() logs start and finish messages."""
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {}
        mock_discover.return_value = mock_catalog
        mock_client = MagicMock()

        with patch("tap_harvest.LOGGER") as mock_logger:
            with patch("sys.stdout", io.StringIO()):
                tap_harvest.do_discover(mock_client)

        self.assertTrue(mock_logger.info.called)
        calls = [str(c) for c in mock_logger.info.call_args_list]
        self.assertTrue(any("discover" in c.lower() for c in calls))


class TestRequiredConfigKeys(unittest.TestCase):
    """Tests for REQUIRED_CONFIG_KEYS."""

    def test_required_config_keys_present(self):
        import tap_harvest
        expected = {
            "refresh_token",
            "client_id",
            "client_secret",
            "start_date",
            "user_agent",
        }
        self.assertEqual(set(tap_harvest.REQUIRED_CONFIG_KEYS), expected)
