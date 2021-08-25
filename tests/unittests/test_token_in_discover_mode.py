from unittest import mock
from tap_harvest.client import HarvestClient
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
class TestAccessToken(unittest.TestCase):

    def test_200_response(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        valid_res = {'expense_feature': True, 'invoice_feature': True, 'estimate_feature': True}
        mocked_send_request.return_value = get_mock_http_response(200, valid_res)
        client = HarvestClient("test", "test", "test", "test")
        client._access_token = "test"
        tap_harvest.do_discover(client)
        self.assertEqual(mocked_write_catalog.call_count, 1)

    def test_401_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(401, {})
        client = HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(client)
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 401)
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_403_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(403, {})
        client = HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(client)
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 403)
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_404_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(404, {})
        client = HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(client)
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 404)
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_422_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(422, {})
        client = HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(client)
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 422)
            self.assertEqual(mocked_write_catalog.call_count, 0)

    def test_429_error(self, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(429, {})
        client = HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(client)
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 429)
            self.assertEqual(mocked_write_catalog.call_count, 0)

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_write_catalog, mocked_get_account, mocked_refresh_token, mocked_get_token, mocked_request, mocked_send_request):
        mocked_send_request.return_value = get_mock_http_response(500, {})
        client = HarvestClient("test", "test", "test", "test")
        try:
            tap_harvest.do_discover(client)
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 500)
            self.assertEqual(mocked_write_catalog.call_count, 0)
