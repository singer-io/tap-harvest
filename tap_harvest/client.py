
import backoff
import requests
import pendulum
from singer import utils
import singer

LOGGER = singer.get_logger()

BASE_ID_URL = "https://id.getharvest.com/api/v2/"
BASE_API_URL = "https://api.harvestapp.com/v2/"

class HarvestError(Exception):
    pass

class Server5xxError(Exception):
    pass

class HarvestBadRequestError(HarvestError):
    pass

class HarvestUnauthorizedError(HarvestError):
    pass

class HarvestNotFoundError(HarvestError):
    pass

class HarvestForbiddenError(HarvestError):
    pass

class HarvestUnprocessableEntityError(HarvestError):
    pass

class HarvestRateLimitExceeededError(HarvestError):
    pass

class HarvestInternalServiceError(Server5xxError):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": HarvestBadRequestError,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": HarvestUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": HarvestForbiddenError,
        "message": "User does not have permission to access the resource or "\
                   "related feature is disabled."
    },
    404: {
        "raise_exception": HarvestNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    422: {
        "raise_exception": HarvestUnprocessableEntityError,
        "message": "The request was not able to process right now."
    },
    429: {
        "raise_exception": HarvestRateLimitExceeededError,
        "message": "API rate limit exceeded."
    },
    500: {
        "raise_exception": HarvestInternalServiceError,
        "message": "An error has occurred at Harvest's end."
    }
}

def raise_for_error(response):
    # Forming a custom response message for raising exception

    try:
        response_json = response.json()
    except Exception:
        response_json = {}

    error_code = response.status_code
    error_message = response_json.get(
        "message", response_json.get(
            "error_description", ERROR_CODE_EXCEPTION_MAPPING.get(
                error_code, {}).get(
                    "message", "An Unknown Error occurred, please try after some time.")))
    message = f"HTTP-error-code: {error_code}, Error: {error_message}"

    ex = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", HarvestError)
    raise ex(message) from None

class HarvestClient: #pylint: disable=too-many-instance-attributes

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
        (HarvestRateLimitExceeededError, Server5xxError),
        max_tries=5,
        factor=2)
    def _make_refresh_token_request(self):
        resp = requests.request('POST',
                                url=BASE_ID_URL + 'oauth2/token',
                                data={
                                    'client_id': self._client_id,
                                    'client_secret': self._client_secret,
                                    'refresh_token': self._refresh_token,
                                    'grant_type': 'refresh_token',
                                },
                                headers={"User-Agent": self._user_agent})
        if resp.status_code not in [200, 201]:
            raise_for_error(resp)
        return resp

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
        # Get the account Id of the Active Harvest account.
        # It will throw an exception if no active harvest account is found.
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
        (HarvestRateLimitExceeededError, Server5xxError),
        max_tries=5,
        factor=2)
    @utils.ratelimit(100, 15)
    def request(self, url, params=None):
        # Retrive API data in JSON form.
        params = params or {}
        access_token = self.get_access_token()
        headers = {"Accept": "application/json",
                "Harvest-Account-Id": self.get_account_id(),
                "Authorization": "Bearer " + access_token,
                "User-Agent": self._user_agent}
        req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
        LOGGER.info("GET %s", req.url)
        resp = self.session.send(req)

        if resp.status_code != 200:
            raise_for_error(resp)

        return resp.json()
