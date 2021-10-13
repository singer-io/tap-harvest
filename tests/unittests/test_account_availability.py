from tap_harvest import Auth
import unittest
import requests
import json
from unittest import mock

def get_mock_http_response(status_code, contents):
        response = requests.Response()
        response.status_code = status_code
        response._content = contents.encode()
        return response

class TestAccountAvailability(unittest.TestCase):

    @mock.patch("tap_harvest.Auth._refresh_access_token")
    @mock.patch('requests.request')
    def test_get_account_id(self,mock_request,mock_refresh_access_token):
        client_id="test"
        client_secret="test"
        refresh_token="test"
        auth = Auth(client_id, client_secret, refresh_token)
        auth._access_token = "test"
        mock_request.return_value=get_mock_http_response(200,json.dumps({"accounts":[]}))
        try: 
            auth.get_account_id()
        except Exception as err:
            expected_message = "No Active Harvest Account found"
            self.assertEquals(str(err), str(expected_message))