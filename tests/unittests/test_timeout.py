import unittest
from parameterized import parameterized
import tap_harvest.client as client_

TIMEOUT_INT = 100
TIMEOUT_STR_INT = "100"
TIMEOUT_STR_FLOAT = "100.0"
TIMEOUT_FLOAT = 100.0
TIMEOUT_ZERO = 0
TIMEOUT_STR_ZERO = "0"
TIMEOUT_INVALID_STRING = "abc"
NULL_STRING = ""

CONFIG = {
    'client_id': 'CLIENT_ID',
    'client_secret': 'CLIENT_SECRET',
    'refresh_token': 'REFRESH_TOKEN',
    'user_agent': 'USER_AGENT',
}

class TestrequestTimeoutValue(unittest.TestCase):

    @parameterized.expand([
        # ["request_timeout_value", "expected_value"]
        [TIMEOUT_INT, TIMEOUT_FLOAT],
        [TIMEOUT_STR_INT, TIMEOUT_FLOAT],
        [TIMEOUT_STR_FLOAT, TIMEOUT_FLOAT],
        [TIMEOUT_FLOAT, TIMEOUT_FLOAT],
    ])
    def test_request_timeout_for_valid_values(self, request_timeout_value, expected_value):
        """
        Test the various values of request_timeout:
            - For integer, float, string(integer), string(float) value converts to float
        """
        config = {**CONFIG, "request_timeout": request_timeout_value}
        client = client_.HarvestClient(config)

        # Verify the request_timeout is the same as the expected value
        self.assertEqual(client.request_timeout, expected_value)

    @parameterized.expand([
        # ["request_timeout_value"]
        [TIMEOUT_INVALID_STRING],
        [TIMEOUT_STR_ZERO],
        [TIMEOUT_ZERO],
        [NULL_STRING],
    ])
    def test_request_timeout_for_invalid_values(self, request_timeout_value):
        """
        Test the various values of request_timeout:
            - For null string, zero(string), zero(integer) raises error
        """

        config = {**CONFIG, "request_timeout": request_timeout_value}
        # Verify the tap raises Exception
        with self.assertRaises(Exception) as e:
            client_.HarvestClient(config)

        # Verify the tap raises an error with expected error message
        self.assertEqual(str(e.exception), "The entered timeout is invalid, it should be a valid none-zero integer.")

    def test_without_request_timeout(self):
        """
        Test if no request timeout is given in config, default request_timeout will be considered.
        """
        config = {**CONFIG}
        client = client_.HarvestClient(config)

        # Verify the request_timeout is the same as the default value
        self.assertEqual(client.request_timeout, client_.REQUEST_TIMEOUT)
