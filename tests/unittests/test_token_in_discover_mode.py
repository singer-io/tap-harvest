from unittest import mock

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

@mock.patch("tap_harvest.AUTH")
@mock.patch("requests.Session.send")
@mock.patch("requests.Request")
@mock.patch("builtins.print")
class TestAccessToken(unittest.TestCase):

    def test_200_response(self, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(200, {})
        tap_harvest.do_discover()
        self.assertEqual(mocked_print.call_count, 1)
        mocked_print.assert_called_with('{"streams":[]}')

    def test_401_error(self, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(401, {})
        try:
            tap_harvest.do_discover()
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 401)
            self.assertEqual(mocked_print.call_count, 0)


    def test_403_error(self, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(403, {})
        try:
            tap_harvest.do_discover()
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 403)
            self.assertEqual(mocked_print.call_count, 0)

    def test_404_error(self, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(404, {})
        try:
            tap_harvest.do_discover()
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 404)
            self.assertEqual(mocked_print.call_count, 0)

    def test_422_error(self, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(422, {})
        try:
            tap_harvest.do_discover()
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 422)
            self.assertEqual(mocked_print.call_count, 0)

    def test_429_error(self, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(429, {})
        try:
            tap_harvest.do_discover()
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 429)
            self.assertEqual(mocked_print.call_count, 0)

    @mock.patch("time.sleep")
    def test_500_error(self, mocked_sleep, mocked_print, mocked_request, mocked_send_request, mocked_auth):
        mocked_send_request.return_value = get_mock_http_response(500, {})
        try:
            tap_harvest.do_discover()
        except HTTPError as e:
            self.assertEqual(e.response.status_code, 500)
            self.assertEqual(mocked_print.call_count, 0)
