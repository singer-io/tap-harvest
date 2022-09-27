import unittest
from unittest import mock
from singer.catalog import Catalog
import pendulum
from tap_harvest import main
from tap_harvest.client import HarvestClient

TEST_CONFIG = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "refresh_token": "REFRESH_TOKEN",
    "user_agent": "USER_AGENT"
}


class MockArgs:
    """Mock args object class"""

    def __init__(self, config=None, catalog=None, state={}, discover=False) -> None:
        self.config = config
        self.catalog = catalog
        self.state = state
        self.discover = discover


@mock.patch("tap_harvest.HarvestClient.get_account_id")
@mock.patch("tap_harvest.HarvestClient._refresh_access_token")
@mock.patch("singer.utils.parse_args")
class TestDiscoverMode(unittest.TestCase):
    """
    Test main function for discover mode
    """

    @mock.patch("tap_harvest._discover")
    def test_discover_with_config(self, mock_discover, mock_args, mock_verify_access, mock_account_id):
        """Test `_discover` function is called for discover mode"""
        mock_discover.return_value = Catalog([])
        mock_args.return_value = MockArgs(discover=True, config=TEST_CONFIG)
        main()

        # Verify that `discover` was called
        self.assertTrue(mock_discover.called)


@mock.patch("tap_harvest.client.pendulum.now", return_value=pendulum.datetime(2022, 2, 5))
@mock.patch("tap_harvest.client.HarvestClient._refresh_access_token")
class TestGetAccessToken(unittest.TestCase):
    """
    Test `get_access_token` token method.
    """
    _client = HarvestClient(TEST_CONFIG)
    _client._access_token = "TEST_TOKEN"

    def test_with_access_token_not_expired(self, mock_refresh_token, mock_now):
        """
        Test if the client token is not expired, then the token will not be refreshed.
        """
        self._client._expires_at = pendulum.datetime(2022, 2, 10)
        self._client.get_access_token()

        # Verify that `_refresh_access_token` is not called.
        self.assertFalse(mock_refresh_token.called)

    def test_with_expired_access_token(self, mock_refresh_token, mock_now):
        """
        Test if the client token is expired, then the token will be refreshed.
        """
        self._client._expires_at = pendulum.datetime(2022, 2, 1)
        self._client.get_access_token()

        # Verify that `_refresh_access_token` is called.
        self.assertTrue(mock_refresh_token.called)

    def test_without_access_token(self, mock_refresh_token, mock_now):
        """
        Test if the client token is expired, then the token will be refreshed.
        """
        _client = HarvestClient(TEST_CONFIG)
        _client.get_access_token()

        # Verify that `_refresh_access_token` is called.
        self.assertTrue(mock_refresh_token.called)
