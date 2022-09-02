import unittest
from unittest import mock
from parameterized import parameterized
import requests
import json
import tap_harvest.client as client
from tap_harvest.client import raise_for_error, ERROR_CODE_EXCEPTION_MAPPING

TEST_CONFIG = {
    "client_id": "CLIENT_ID",
    "client_secret": "CLIENT_SECRET",
    "refresh_token": "REFRESH_TOKEN",
    "user_agent": "USER_AGENT"
}

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

class TestExceptionHanfling(unittest.TestCase):
    """
    Test Error is thrown with the expected error message.
    """

    @parameterized.expand([
        [400, client.HarvestBadRequestError],
        [401, client.HarvestUnauthorizedError],
        [403, client.HarvestForbiddenError],
        [404, client.HarvestNotFoundError],
        [422, client.HarvestUnprocessableEntityError],
        [429, client.HarvestRateLimitExceeededError],
        [500, client.HarvestInternalServiceError],
        [503, client.Server5xxError],  # Unknown 5xx error
    ])
    def test_custom_error_message(self, error_code, error):
        """
        Test that error is thrown with the custom error message
        if no description is provided in response.
        """
        expected_message = "HTTP-error-code: {}, Error: {}".format(
            error_code,ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "An Unknown Error occurred."))
        with self.assertRaises(error) as e:
            raise_for_error(get_mock_http_response(error_code))

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)\

    @parameterized.expand([
        [400, "Request can not be fulfilled due to bad syntax.", client.HarvestBadRequestError],
        [401, "Authentication Failure", client.HarvestUnauthorizedError],
        [403, "The object you requested was found but you don't have authorization to perform your request.", client.HarvestForbiddenError],
        [404, "The object you requested can't be found.", client.HarvestNotFoundError],
        [422, "There were errors while processing your request.", client.HarvestUnprocessableEntityError],
        [429, "Your request has been throttled.", client.HarvestRateLimitExceeededError],
        [500, "There was a server error.", client.HarvestInternalServiceError],
        [503, "Service Unavailable.", client.Server5xxError],  # Unknown 5xx error
    ])
    def test_error_response_message(self, error_code, message, error):
        """
        Test that error is thrown with description in the response.
        """
        expected_message = "HTTP-error-code: {}, Error: {}".format(error_code, message)
        with self.assertRaises(error) as e:
            raise_for_error(get_mock_http_response(error_code, {"error_description": message}))

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)

    def test_json_decoder_error(self):
        """Test for invalid json response, tap does not throw JSON decoder error."""
        mock_response = get_mock_http_response(400)
        mock_response._content = "ABC".encode()
        expected_message = "HTTP-error-code: {}, Error: {}".format(400, "The request is missing or has a bad parameter.")

        with self.assertRaises(client.HarvestBadRequestError) as e:
            raise_for_error(mock_response)

        # Verify that an error message is expected
        self.assertEqual(str(e.exception), expected_message)


class TestBackOffHandling(unittest.TestCase):
    """
    Test backoff handling for all 5xx, timeout and connection errors.
    """

    @parameterized.expand([
        ["For error 500", lambda *x,**y: get_mock_http_response(500), client.HarvestInternalServiceError],
        ["For 503 (unknown 5xx error)", lambda *x,**y:get_mock_http_response(503), client.Server5xxError],
        ["For Connection Error", requests.ConnectionError, requests.ConnectionError],
        ["For timeour Error", requests.Timeout, requests.Timeout],
    ])
    @mock.patch("requests.Session.request")
    @mock.patch("time.sleep")
    def test_refresh_access_token_backoff(self, name, mock_response, error, mocked_sleep, mock_request):
        """
        Test that an exception is thrown with the proper message,
        when calling `_refresh_access_token` method of the client, returns the response with the given status code.
        """
        mock_request.side_effect = mock_response
        harvest_client = client.HarvestClient(TEST_CONFIG)

        with self.assertRaises(error) as e:
            harvest_client._refresh_access_token()

        # Verify `_refresh_access_token` method back off 5 times.
        self.assertEqual(mock_request.call_count, 5)

    @parameterized.expand([
        ["For error 500", lambda *x,**y: get_mock_http_response(500), client.HarvestInternalServiceError],
        ["For 503 (unknown 5xx error)", lambda *x,**y:get_mock_http_response(503), client.Server5xxError],
        ["For Connection Error", requests.ConnectionError, requests.ConnectionError],
        ["For timeout Error", requests.Timeout, requests.Timeout],
    ])
    @mock.patch("requests.Session.send")
    @mock.patch("tap_harvest.client.HarvestClient.get_access_token", return_value = "ACCESS_TOKEN")
    @mock.patch("time.sleep")
    def test_request_backoff(self, name, mock_response, error, mocked_sleep, mock_access_token, mock_request):
        """
        Test that an exception is thrown with the proper message,
        when calling `request` method of the client, returns the response with the given status code.
        """
        mock_request.side_effect = mock_response
        harvest_client = client.HarvestClient(TEST_CONFIG)
        harvest_client._account_id = "123456"

        with self.assertRaises(error) as e:
            harvest_client.request("https://sample-url.com")

        # Verify request method back off 5 times.
        self.assertEqual(mock_request.call_count, 5)

    @parameterized.expand([
        ["For error 500", lambda *x,**y: get_mock_http_response(500), client.HarvestInternalServiceError],
        ["For 503 (unknown 5xx error)", lambda *x,**y:get_mock_http_response(503), client.Server5xxError],
        ["For Connection Error", requests.ConnectionError, requests.ConnectionError],
        ["For timeout Error", requests.Timeout, requests.Timeout],
    ])
    @mock.patch("requests.Session.request")
    @mock.patch("time.sleep")
    def test_get_account_id_backoff(self, name, mock_response, error, mocked_sleep, mock_request):
        """
        Test that an exception is thrown with the proper message,
        when calling `get_account_id` method of the client, returns the response with the given status code.
        """
        mock_request.side_effect = mock_response
        harvest_client = client.HarvestClient(TEST_CONFIG)
        harvest_client._access_token = "123456"
        with self.assertRaises(error) as e:
            harvest_client.get_account_id()

        # Verify `get_account_id` method back off 5 times.
        self.assertEqual(mock_request.call_count, 5)


@mock.patch("tap_harvest.client.LOGGER.info")
@mock.patch("tap_harvest.client.LOGGER.critical")
@mock.patch("requests.Session.request")
@mock.patch("time.sleep")
class TestRefreshAccessToken(unittest.TestCase):
    """
    Test `_refresh_access_token` method of the client.
    """

    def test_success_refresh_token(self, mocked_sleep, mock_request, logger_critical, logger_info):
        """
        Test if an access token is given in the response, and the client access token is updated.
        """
        mock_request.return_value = get_mock_http_response(201, {"access_token": "TEST_TOKEN"})
        harvest_client = client.HarvestClient(TEST_CONFIG)
        harvest_client._refresh_access_token()

        # Verify client access token is from response
        self.assertEqual(harvest_client._access_token, "TEST_TOKEN")
        self.assertFalse(logger_critical.called)

        # Verify expected info logger was called
        logger_info.assert_any_call("Got refreshed access token")

    def test_refresh_token_error(self, mocked_sleep, mock_request, logger_critical, logger_info):
        """
        Test that if no access token was given in the response,
        then expected critical level loggers were printed and an expected error is raised.
        """
        resp_json = {
            "error": "invalid_client",
            "error_description": "The client identifier provided is invalid."
        }
        mock_request.return_value = get_mock_http_response(400, resp_json)
        harvest_client = client.HarvestClient(TEST_CONFIG)
        
        with self.assertRaises(client.HarvestBadRequestError) as e:
            harvest_client._refresh_access_token()

        # Verify that error message is expected
        self.assertEqual(str(e.exception), "HTTP-error-code: 400, Error: {}".format(resp_json.get("error_description")))

        # Verify that client access token is not updated
        self.assertEqual(harvest_client._access_token, None)

        # Verify that expected critical level loggers were called
        logger_critical.assert_any_call(resp_json.get("error"))
        logger_critical.assert_any_call(resp_json.get("error_description"))
