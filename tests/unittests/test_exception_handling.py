from unittest import mock
import tap_harvest.client as client
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
@mock.patch("tap_harvest.client.HarvestClient.get_access_token")
@mock.patch("tap_harvest.client.HarvestClient._refresh_access_token")
@mock.patch("tap_harvest.client.HarvestClient.get_account_id")
class TestRequest(unittest.TestCase):

    def test_200_error(self, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        valid_res = {'expense_feature': True, 'invoice_feature': True, 'estimate_feature': True}
        mocked_send_request.return_value = get_mock_http_response(200, valid_res)
        harvest_client = client.HarvestClient("test", "test", "test", "test")

        resp = harvest_client.request('http://test')

        self.assertEqual(resp, valid_res)
        self.assertEqual(mocked_get_account.call_count, 1)
        self.assertEqual(mocked_get_token.call_count, 1)
        self.assertEqual(mocked_send_request.call_count, 1)

    def test_400_error(self, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(400, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestBadRequestError as e:
            self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")

    def test_401_error(self, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(401, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestUnauthorizedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_403_error(self, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(403, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestForbiddenError as e:
            self.assertEqual(str(e), "HTTP-error-code: 403, Error: User does not have permission to access the resource or related feature is disabled.")

    def test_404_error(self, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(404, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestNotFoundError as e:
            self.assertEqual(str(e), "HTTP-error-code: 404, Error: The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account.")

    def test_422_error(self, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(422, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestUnprocessableEntityError as e:
            self.assertEqual(str(e), "HTTP-error-code: 422, Error: The request was not able to process right now.")

    @mock.patch("time.sleep")
    def test_429_error(self, mocked_sleep, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(429, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestRateLimitExceeededError as e:
            self.assertEqual(str(e), "HTTP-error-code: 429, Error: API rate limit exceeded.")
            self.assertEqual(mocked_get_account.call_count, 5)
            self.assertEqual(mocked_get_token.call_count, 5)
            self.assertEqual(mocked_send_request.call_count, 5)

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(500, {})
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        try:
            harvest_client.request('http://test')
        except client.HarvestInternalServiceError as e:
            self.assertEqual(str(e), "HTTP-error-code: 500, Error: An error has occurred at Harvest's end.")
            self.assertEqual(mocked_get_account.call_count, 5)
            self.assertEqual(mocked_get_token.call_count, 5)
            self.assertEqual(mocked_send_request.call_count, 5)


@mock.patch("requests.request")
class TestMakeRequestToken(unittest.TestCase):
    '''
        Test cases to verify _make_refresh_token_request() function which is called by __init__() method.
    '''

    def test_201_error(self, mocked_request):
        valid_res = {'access_token': 'xyz'}
        mocked_request.return_value = get_mock_http_response(201, valid_res)
        harvest_client = client.HarvestClient("test", "test", "test", "test")
        self.assertEqual(mocked_request.call_count, 1)

    def test_400_error(self, mocked_request):
        mocked_request.return_value = get_mock_http_response(400, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestBadRequestError as e:
            self.assertEqual(str(e), "HTTP-error-code: 400, Error: The request is missing or has a bad parameter.")

    def test_401_error(self, mocked_request):
        mocked_request.return_value = get_mock_http_response(401, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestUnauthorizedError as e:
            self.assertEqual(str(e), "HTTP-error-code: 401, Error: Invalid authorization credentials.")

    def test_403_error(self, mocked_request):
        mocked_request.return_value = get_mock_http_response(403, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestForbiddenError as e:
            self.assertEqual(str(e), "HTTP-error-code: 403, Error: User does not have permission to access the resource or related feature is disabled.")

    def test_404_error(self, mocked_request):
        mocked_request.return_value = get_mock_http_response(404, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestNotFoundError as e:
            self.assertEqual(str(e), "HTTP-error-code: 404, Error: The resource you have specified cannot be found. Either the accounts provided are invalid or you do not have access to the Ad Account.")

    def test_422_error(self, mocked_request):
        mocked_request.return_value = get_mock_http_response(422, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestUnprocessableEntityError as e:
            self.assertEqual(str(e), "HTTP-error-code: 422, Error: The request was not able to process right now.")

    @mock.patch("time.sleep")
    def test_429_error(self, mocked_sleep, mocked_request):
        mocked_request.return_value = get_mock_http_response(429, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestRateLimitExceeededError as e:
            self.assertEqual(str(e), "HTTP-error-code: 429, Error: API rate limit exceeded.")
            self.assertEqual(mocked_request.call_count, 5)

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_request):
        mocked_request.return_value = get_mock_http_response(500, {})
        try:
            harvest_client = client.HarvestClient("test", "test", "test", "test")
        except client.HarvestInternalServiceError as e:
            self.assertEqual(str(e), "HTTP-error-code: 500, Error: An error has occurred at Harvest's end.")
            self.assertEqual(mocked_request.call_count, 5)
