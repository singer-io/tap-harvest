from unittest import mock
import tap_harvest.client as client
from requests.models import HTTPError
import tap_harvest
import unittest
import requests
import json

def get_mock_http_response(status_code, content={}):
    contents = json.dumps(content)
    response = requests.Response()
    response.status_code = status_code
    response.headers = {}
    response._content = contents.encode()
    return response

@mock.patch("requests.Session.send")
@mock.patch("requests.Request")
@mock.patch("tap_harvest.client.HarvestClient._refresh_access_token")
@mock.patch("tap_harvest.client.HarvestClient.get_access_token")
@mock.patch("tap_harvest.client.HarvestClient.get_account_id")
@mock.patch("tap_harvest.write_catalog")
class TestAccessTokeninDiscover(unittest.TestCase):
    '''
        Test cases to verify API calls in discover mode
    '''

    def test_200_response(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify no exception is thrown when the do_discover call returns the response with status 200. 
        valid_res = {'expense_feature': True, 'invoice_feature': True, 'estimate_feature': True}
        mocked_send_request.return_value = get_mock_http_response(200, valid_res)
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        harvest_client._access_token = "test"
        tap_harvest.do_discover(harvest_client)
        self.assertEqual(mocked_write_catalog.call_count, 1)

    def test_400_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 400.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(400, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestBadRequestError as e:
            self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_401_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 401.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(401, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestUnauthorizedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_403_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 403.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(403, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestForbiddenError as e:
            self.assertEqual(str(e), "HTTP-error-code: 403, Error: User does not have permission to access the resource or related feature is disabled.")
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_404_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 404.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(404, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestNotFoundError as e:
            self.assertEqual(str(e), "HTTP-error-code: 404, Error: The resource you have specified cannot be found.")
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_422_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 422.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(422, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestUnprocessableEntityError as e:
            self.assertEqual(str(e), "HTTP-error-code: 422, Error: The request was not able to process right now.")
            self.assertEqual(mocked_write_catalog.call_count, 0)

    @mock.patch("time.sleep")
    def test_429_error(self, mocked_sleep, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 429.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(429, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestRateLimitExceeededError as e:
            self.assertEqual(str(e), "HTTP-error-code: 429, Error: API rate limit exceeded.")
            self.assertEqual(mocked_write_catalog.call_count, 0)

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        # Verify the exception is thrown with the proper message when the do_discover call returns the response with status 500.
        # Verify write catalog method won't call. 
        mocked_send_request.return_value = get_mock_http_response(500, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(harvest_client)
        except client.HarvestInternalServiceError as e:
            self.assertEqual(str(e), "HTTP-error-code: 500, Error: An error has occurred at Harvest's end.")
            self.assertEqual(mocked_write_catalog.call_count, 0)
