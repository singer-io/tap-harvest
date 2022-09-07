import unittest
from unittest import mock
from parameterized import parameterized
from tap_harvest.client import HarvestClient, REQUEST_TIMEOUT


TEST_CONFIG = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "refresh_token": "REFRESH_TOKEN",
    "user_agent": "USER_AGENT"
}

class TestTimeOut(unittest.TestCase):
    """
    Test `get_request_timeout` method of the client.
    """

    @parameterized.expand([
        ["integer_value", 10, 10.0],
        ["float_value", 100.5, 100.5],
        ["string_integer", "10", 10.0],
        ["string_float", "100.5", 100.5],
    ])
    def test_timeout_values(self, name, timeout_value, expected_value):
        """
        Test that for the valid value of timeout,
        No exception is raised and the expected value is set.
        """
        config = {**TEST_CONFIG, "request_timeout": timeout_value}
        _client = HarvestClient(config)

        # Verify timeout value is expected
        self.assertEqual(_client.request_timeout, expected_value)

    @parameterized.expand([
        ["integer_zero", 0],
        ["float_zero", 0.0],
        ["string_zero", "0"],
        ["string_float_zero", "0.0"],
        ["string_alphabate", "abc"],
    ])
    def test_invalid_value(self, name, timeout_value):
        """
        Test that for invalid value exception is raised.
        """
        config = {**TEST_CONFIG, "request_timeout": timeout_value}
        with self.assertRaises(Exception) as e:
            _client = HarvestClient(config)

        # Verify that the exception message is expected.
        self.assertEqual(str(e.exception), "The entered timeout is invalid, it should be a valid none-zero integer.")

    def test_none_value(self):
        """
        Test if no timeout is not passed in the config, then set it to the default value.
        """
        config = {**TEST_CONFIG}
        _client = HarvestClient(config)

        # Verify that the default timeout value is set.
        self.assertEqual(_client.request_timeout, REQUEST_TIMEOUT)
