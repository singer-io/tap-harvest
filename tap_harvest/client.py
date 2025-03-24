from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
import pendulum
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger, metrics

from tap_harvest.exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING,
    HarvestError,
    HarvestBackoffError,
)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300
REFRESH_URL = "https://id.getharvest.com/api/v2"


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            error_message = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}
            ).get("message", "Unknown Error")
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', error_message)}"
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
            "raise_exception", HarvestError
        )
        raise exc(message, response) from None


class Client:
    """A Wrapper class. ~~~

    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api.harvestapp.com/v2"
        self._access_token = None
        self._expires_at = None
        self._account_id = None

        # Set the request timeout
        config_request_timeout = config.get("request_timeout")
        if config_request_timeout and float(config_request_timeout):
            self.request_timeout = float(config_request_timeout)
        else:
            self.request_timeout = REQUEST_TIMEOUT

    def __enter__(self):
        self._refresh_access_token()
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def _refresh_access_token(self) -> None:
        """Refreshes the access token."""
        LOGGER.info("Refreshing Access Token")
        resp_json = self.post(
            endpoint=REFRESH_URL + "/oauth2/token",
            headers={"User-Agent": self.config["user_agent"]},
            body={
                "refresh_token": self.config["refresh_token"],
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "grant_type": "refresh_token",
            },
        )
        self._access_token = resp_json["access_token"]
        expires_in_seconds = resp_json.get("expires_in", 17 * 60 * 60)
        self._expires_at = pendulum.now().add(seconds=expires_in_seconds)
        LOGGER.info("Got refreshed access token")

    def get_access_token(self) -> str:
        """Return access token if available or generate one."""
        if self._access_token and self._expires_at > pendulum.now():
            return self._access_token

        self._refresh_access_token()
        return self._access_token

    def check_api_credentials(self) -> None:
        """Check if the API credentials are valid."""
        resp_json = self.get(endpoint=REFRESH_URL + "/accounts")

        # Set account-id if any account is available in response
        if resp_json.get("accounts"):
            self._account_id = str(resp_json["accounts"][0]["id"])
            return self._account_id

        raise Exception("No Active Harvest Account found") from None

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request with the token."""
        headers["Authorization"] = f"Bearer {self.get_access_token()}"
        headers["User-Agent"] = self.config["user_agent"]
        if self._account_id:
            headers["Harvest-Account-Id"] = self._account_id

        return headers, params

    def get(
        self, endpoint: str, params: Dict = {}, headers: Dict = {}, path: str = None
    ) -> Any:
        """Calls the make_request method with a prefixed method type `GET`"""
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request(
            "GET",
            endpoint,
            headers=headers,
            params=params,
            timeout=self.request_timeout,
        )

    def post(
        self,
        endpoint: str,
        params: Dict = {},
        headers: Dict = {},
        body: Dict = {},
        path: str = None,
    ) -> Any:
        """Calls the make_request method with a prefixed method type `POST`"""
        endpoint = endpoint or f"{self.base_url}/{path}"
        return self.__make_request(
            "POST",
            endpoint,
            headers=headers,
            params=params,
            data=body,
            timeout=self.request_timeout,
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            HarvestBackoffError,
        ),
        max_tries=5,
        factor=2,
    )
    def __make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[Mapping[Any, Any]]:
        """
        Performs HTTP Operations
        Args:
            method (str): represents the state file for the tap.
            endpoint (str): url of the resource that needs to be fetched
            params (dict): A mapping for url params eg: ?name=Avery&age=3
            headers (dict): A mapping for the headers that need to be sent
            body (dict): only applicable to post request, body of the request

        Returns:
            Dict,List,None: Returns a `Json Parsed` HTTP Response or None if exception
        """
        with metrics.http_request_timer(endpoint):
            response = self._session.request(method, endpoint, **kwargs)
            raise_for_error(response)

        return response.json()
