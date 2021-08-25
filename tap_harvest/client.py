
import backoff
import requests
import pendulum
from singer import utils
import singer

LOGGER = singer.get_logger()

BASE_ID_URL = "https://id.getharvest.com/api/v2/"
BASE_API_URL = "https://api.harvestapp.com/v2/"

class HarvestClient:#pylint: disable=too-many-instance-attributes
    def __init__(self, client_id, client_secret, refresh_token, user_agent):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._user_agent = user_agent
        self._account_id = None
        self.session = requests.Session()
        self._refresh_access_token()

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2)
    def _make_refresh_token_request(self):
        return requests.request('POST',
                                url=BASE_ID_URL + 'oauth2/token',
                                data={
                                    'client_id': self._client_id,
                                    'client_secret': self._client_secret,
                                    'refresh_token': self._refresh_token,
                                    'grant_type': 'refresh_token',
                                },
                                headers={"User-Agent": self._user_agent})

    def _refresh_access_token(self):
        LOGGER.info("Refreshing access token")
        resp = self._make_refresh_token_request()
        expires_in_seconds = resp.json().get('expires_in', 17 * 60 * 60)
        self._expires_at = pendulum.now().add(seconds=expires_in_seconds)
        resp_json = {}
        try:
            resp_json = resp.json()
            self._access_token = resp_json['access_token']
        except KeyError as key_err:
            if resp_json.get('error'):
                LOGGER.critical(resp_json.get('error'))
            if resp_json.get('error_description'):
                LOGGER.critical(resp_json.get('error_description'))
            raise key_err
        LOGGER.info("Got refreshed access token")

    def get_access_token(self):
        if self._access_token is not None and self._expires_at > pendulum.now():
            return self._access_token

        self._refresh_access_token()
        return self._access_token

    def get_account_id(self):
        if self._account_id is not None:
            return self._account_id

        response = requests.request('GET',
                                    url=BASE_ID_URL + 'accounts',
                                    headers={'Authorization': 'Bearer ' + self._access_token,
                                             'User-Agent': self._user_agent})

        if response.json().get('accounts'):
            self._account_id = str(response.json()['accounts'][0]['id'])
            return self._account_id

        raise Exception("No Active Harvest Account found") from None

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2)
    @utils.ratelimit(100, 15)
    def request(self, url, params=None):
        params = params or {}
        access_token = self.get_access_token()
        headers = {"Accept": "application/json",
                "Harvest-Account-Id": self.get_account_id(),
                "Authorization": "Bearer " + access_token,
                "User-Agent": self._user_agent}
        req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
        LOGGER.info("GET {}".format(req.url))
        resp = self.session.send(req)
        resp.raise_for_status()
        return resp.json()
