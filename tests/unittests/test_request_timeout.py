import tap_harvest
import unittest
import requests
from unittest import mock

# Mock response object
def get_mock_http_response(*args, **kwargs):
    contents = '{"access_token": "test", "expires_in":100, "accounts":[{"id": 12}]}'
    response = requests.Response()
    response.status_code = 200
    response._content = contents.encode()
    return response

@mock.patch('requests.request', side_effect = get_mock_http_response)
@mock.patch('requests.Session.send')
@mock.patch("requests.Request.prepare")
class TestRequestTimeoutValue(unittest.TestCase):

    def test_no_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is not provided in config then default value is used
        """
        tap_harvest.CONFIG = {} # No request_timeout in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_integer_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is provided in config(integer value) then it should be use
        """
        tap_harvest.CONFIG = {"request_timeout": 100} # integer timeout in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 100.0) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100.0) # Verify timeout argument

    def test_float_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is provided in config(float value) then it should be use
        """
        tap_harvest.CONFIG = {"request_timeout": 100.5} # float timeout in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 100.5) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100.5) # Verify timeout argument

    def test_string_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is provided in config(string value) then it should be use
        """
        tap_harvest.CONFIG = {"request_timeout": "100"} # string format timeout in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 100) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 100) # Verify timeout argument

    def test_empty_string_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is provided in config with empty string then default value is used
        """
        tap_harvest.CONFIG = {"request_timeout": ""} # empty string in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is provided in config with zero value then default value is used
        """
        tap_harvest.CONFIG = {"request_timeout": 0.0} # zero value in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument

    def test_zero_string_request_timeout_in_config(self, mocked_prepare, mocked_send, mocked_request):
        """
            Verify that if request_timeout is provided in config with zero in string format then default value is used
        """
        tap_harvest.CONFIG = {"request_timeout": '0.0'} # zero value in config
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        # Call request method which call requests.request and Session.send with timeout
        tap_harvest.request("http://test")

        # Verify requests.request and Session.send is called with expected timeout
        args, kwargs = mocked_send.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument
        args, kwargs = mocked_request.call_args
        self.assertEqual(kwargs.get('timeout'), 300) # Verify timeout argument


@mock.patch("time.sleep")
class TestRequestTimeoutBackoff(unittest.TestCase):

    @mock.patch('requests.request', side_effect = get_mock_http_response)
    @mock.patch('requests.Session.send', side_effect = requests.exceptions.Timeout)
    @mock.patch("requests.Request.prepare",)
    def test_request_timeout_backoff(self, mocked_prepare, mocked_send, mocked_request, mocked_sleep):
        """
            Verify request function is backoff for 5 times on Timeout exceeption
        """
        tap_harvest.AUTH = tap_harvest.Auth("test", "test", "test")

        try:
            tap_harvest.request("http://test")
        except requests.exceptions.Timeout:
            pass

        # Verify that Session.send is called 5 times
        self.assertEqual(mocked_send.call_count, 5)

    @mock.patch('requests.request', side_effect = requests.exceptions.Timeout)
    @mock.patch("tap_harvest.Auth._refresh_access_token")
    def test_timeout_backoff_for_make_refresh_token_request(self, mocked_token, mocked_request, mocked_sleep):
        """
            Verify _make_refresh_token_request function is backoff for 5 times on Timeout exceeption
        """
        auth = tap_harvest.Auth("test", "test", "test")

        try:
            auth._make_refresh_token_request()
        except Exception:
            pass

        # Verify that requests.request is called 5 times
        self.assertEqual(mocked_request.call_count, 5)
