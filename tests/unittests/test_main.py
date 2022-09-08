import unittest
from unittest import mock
from singer.catalog import Catalog
from tap_harvest import main
from tap_harvest.discover import discover

TEST_CONFIG = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "refresh_token": "REFRESH_TOKEN",
    "user_agent": "USER_AGENT"
}

class MockArgs:
    """Mock args object class"""
    
    def __init__(self, config = None, catalog = None, state = {}, discover = False) -> None:
        self.config = config 
        self.catalog = catalog
        self.state = state
        self.discover = discover


@mock.patch("tap_harvest.HarvestClient._refresh_access_token")
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_harvest._sync")
class TestSyncMode(unittest.TestCase):
    """
    Test the main function for sync mode.
    """

    mock_catalog = {"streams": [{"stream": "invoices", "schema": {}, "metadata": {}}]}

    @mock.patch("tap_harvest._discover")
    def test_sync_with_catalog(self, mock_discover, mock_sync, mock_args, mock_check_access_token):
        """
        Test sync mode with catalog given in args.
        """

        mock_args.return_value = MockArgs(config=TEST_CONFIG, catalog=Catalog.from_dict(self.mock_catalog))
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(client=mock.ANY,
                                     config=TEST_CONFIG,
                                     catalog=Catalog.from_dict(self.mock_catalog),
                                     state={})

        # verify `_discover` function is not called
        self.assertFalse(mock_discover.called)

    @mock.patch("tap_harvest._discover")
    def test_without_catalog(self, mock_discover, mock_sync, mock_args, mock_check_access_token):
        """
        Test sync mode without catalog given in args.
        """

        discover_catalog = Catalog.from_dict(self.mock_catalog)
        mock_discover.return_value = discover_catalog
        mock_args.return_value = MockArgs(config=TEST_CONFIG)
        main()

        # verify `_discover` and `_sync` function is not called
        self.assertFalse(mock_discover.called)
        self.assertFalse(mock_sync.called)

    def test_sync_with_state(self, mock_sync, mock_args, mock_check_access_token):
        """
        Test sync mode with the state given in args
        """
        mock_state = {"bookmarks": {"projec ts": ""}}
        mock_args.return_value = MockArgs(config=TEST_CONFIG, catalog=Catalog.from_dict(self.mock_catalog), state=mock_state)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(client=mock.ANY,
                                     config=TEST_CONFIG,
                                     catalog=Catalog.from_dict(self.mock_catalog),
                                     state=mock_state)

class TestDiscover(unittest.TestCase):
    """Test `discover` function."""

    def test_discover(self):
        return_catalog = discover()

        # Verify discover function returns `Catalog` type object.
        self.assertIsInstance(return_catalog, Catalog)

    @mock.patch("tap_harvest.discover.Schema")
    @mock.patch("tap_harvest.discover.LOGGER.error")
    def test_discover_error_handling(self, mock_logger, mock_schema):
        """Test discover function if exception arises."""
        mock_schema.from_dict.side_effect = Exception
        with self.assertRaises(Exception):
            discover()

        # Verify logger called 3 times when an exception arises.
        self.assertEqual(mock_logger.call_count, 3)
