import unittest
from unittest import mock
from parameterized import parameterized
import requests
import json
from tap_harvest.client import HarvestClient, REQUEST_TIMEOUT

def get_mock_http_response(status_code, content={}, headers = {}):
    """
    Returns mock response.
    """
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = headers
    response._content = contents.encode()
    return response


TEST_CONFIG = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "refresh_token": "REFRESH_TOKEN",
    "user_agent": "USER_AGENT"
}


@mock.patch("requests.Session.send")
@mock.patch("tap_harvest.client.time.sleep")
@mock.patch("tap_harvest.client.HarvestClient.get_access_token", return_value="TEST_TOKEN")
class TestRequestRateLimitHandling(unittest.TestCase):
    """
    Test rate-limit exception handling for `request` method of client.
    """

    @parameterized.expand([
        ["10"],
        ["5"],
        ["15"],
    ])
    def test_rate_limit_exceeded(self, mock_acces_token, mock_sleep, mock_request, retry_seconds):
        """
        Test that when the rate limit is exceeded, the function is called again after `Retry-After` seconds.
        """
        mock_request.side_effect = [get_mock_http_response(429, headers={"Retry-After": retry_seconds}), get_mock_http_response(200)]
        _client = HarvestClient(TEST_CONFIG)
        _client._access_token = "TEST_TOKEN"
        _client._account_id = "1234"
        _client.request("https://TEST_URL.com")

        # Verify that `requests` method is called twice.
        self.assertEqual(mock_request.call_count, 2)

        # Verify that `time.sleep` was called for 'Retry-After' seconds from the header.
        mock_sleep.assert_any_call(int(retry_seconds))

    def test_rate_limit_not_exceeded(self, mock_acces_token, mock_sleep, mock_request):
        """
        Test that the function will not retry for the success response.
        """
        mock_request.side_effect = [get_mock_http_response(200)]
        _client = HarvestClient(TEST_CONFIG)
        _client._access_token = "TEST_TOKEN"
        _client._account_id = "1234"
        _client.request("https://TEST_URL.com")

        # Verify that `requests` method is called once.
        self.assertEqual(mock_request.call_count, 1)
        mock_request.assert_called_with(mock.ANY, timeout=REQUEST_TIMEOUT)

@mock.patch("requests.Session.request")
@mock.patch("tap_harvest.client.time.sleep")
class TestGetAccountIdRateLimitHandling(unittest.TestCase):
    """
    Test rate-limit exception handling for `get_account_id` method of the client.
    """

    @parameterized.expand([
        ["10"],
        ["5"],
        ["15"],
    ])
    def test_rate_limit_exceeded(self, mock_sleep, mock_request, retry_seconds):
        """
        Test that when the rate limit is exceeded, the function is called again after `Retry-After` seconds.
        """
        mock_request.side_effect = [get_mock_http_response(429, headers={"Retry-After": retry_seconds}),
                                    get_mock_http_response(200, content={"accounts": [{"id": 1234}]})]
        _client = HarvestClient(TEST_CONFIG)
        _client._access_token = "TEST_TOKEN"
        _client.get_account_id()

        # Verify that `requests` method is called twice.
        self.assertEqual(mock_request.call_count, 2)

        # Verify that `time.sleep` was called for 'Retry-After' seconds from the header.
        mock_sleep.assert_any_call(int(retry_seconds))

    def test_rate_limit_not_exceeded(self, mock_sleep, mock_request):
        """
        Test that the function will not retry for the success response.
        """
        mock_request.side_effect = [get_mock_http_response(200, content={"accounts": [{"id": 1234}]})]
        _client = HarvestClient(TEST_CONFIG)
        _client._access_token = "TEST_TOKEN"
        _client.get_account_id()

        # Verify that `requests` method is called once.
        self.assertEqual(mock_request.call_count, 1)
