import unittest
from unittest import mock
import requests
import json
from tap_harvest.client import HarvestClient


def get_mock_http_response(status_code, content={}):
    """
    Returns mock response.
    """
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    return response


TEST_CONFIG = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "refresh_token": "REFRESH_TOKEN",
    "user_agent": "USER_AGENT"
}


class TestAccountAvailability(unittest.TestCase):
    """
    Test `get_account_id` method of the client.
    """

    @mock.patch("tap_harvest.client.HarvestClient._refresh_access_token")
    @mock.patch('requests.Session.send',
                return_value=get_mock_http_response(200, {"accounts": []}))
    def test_get_account_id(self, mock_request, mock_refresh_access_token):
        """
        Test if no account is available in the success response,
        an error with the expected message was raised.
        """

        auth = HarvestClient(TEST_CONFIG)
        auth._access_token = "test"
        with self.assertRaises(Exception) as e:
            auth.get_account_id()
        expected_message = "No Active Harvest Account found"

        # Verify the exception message is expected
        self.assertEqual(str(e.exception), str(expected_message))

    @mock.patch("tap_harvest.client.HarvestClient._refresh_access_token")
    @mock.patch('requests.Session.send',
                return_value=get_mock_http_response(200, {"accounts": [{"id": 12345}]}))
    def test_success_get_account_id(self, mock_request, mock_refresh_access_token):
        """
        Test if an account is present in the response,
        then the client account id will update by account id in the response.
        """
        auth = HarvestClient(TEST_CONFIG)
        auth._access_token = "test"
        auth.get_account_id()

        # Verify that the client account id is updated
        self.assertEqual(auth._account_id, "12345")
