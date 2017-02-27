# tap-harvest

A data stream generator for the Harvest REST API, written in python 3.
Author: Jordan Ryan (jordan@facetinteractive.com)

## Quick start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python 3 venv
    > source venv/bin/activate
    > python setup.py install
    ```

2. Create an OAuth2 API Client

    Login to your Harvest account, go to the Authorized OAuth2 API Clients settings:

    https://YOURACCOUNT.harvestapp.com/oauth2_clients

    Generate a new app with the redirect URI: http://localhost:8080/oauth_redirect.

3. Browse to your Authorization URL

    https://YOURACCOUNT.harvestapp.com/oauth2/authorize?client_id=YOURCLIENTID&redirect_uri=https%3A%2F%2Flocalhost:8080%2Foauth_redirect&state=optional-csrf-token&response_type=code

    Authorize Your App. You will be redirected to a URL. You need to copy and paste the auth_code in the URL into the next section for `YOURAUTHCODE`

4. Get your Refresh Token

    Run `python` to get your refresh token.

    ``` python
    >>> import requests
    >>> headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
    >>> url = 'https://api.harvestapp.com/oauth2/token'
    >>> data = {'code': 'YOURAUTHCODE', 'client_id': 'YOURCLIENTID', 'client_secret': 'YOURCLIENTSECRET', 'redirect_uri': 'https://localhost:8080/oauth_redirect', 'grant_type': 'authorization_code'}
    >>> requests.post(url,data,headers).json()
    ```

    Your response should look like this:

    ```json
    {
        'access_token': 'YOURACCESSTOKEN',
        'refresh_token': 'YOURREFRESHTOKEN',
        'token_type': 'bearer',
        'expires_in': 64799
    }
    ```



4. Create the config file

    Create a JSON file containing the refresh token you just created and
    along with your other connection credentials.

    ```json
    {"harvest_client_id": "YOURID",
     "harvest_client_secret": "YOURSECRET",
    "harvest_refresh_token": "YOURREFRESHTOKEN"
     "harvest_host": "https://YOURACCOUNT.harvestapp.com"}
    ```

5. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the "commit" and
    "issues" endpoints to force the application to only fetch commits and
    issues newer than those dates. If you omit the file it will fetch all
    commits and issues.

    ```json
    {"commits": "2017-01-17T20:32:05Z",
     "issues":  "2017-01-17T20:32:05Z"}
    ```

6. Run the application

    `tap-harvest` can be run with:

    ```bash
    tap-harvest --config config.json [--state state.json]
    ```

---

Copyright &copy; 2017 Facet Interactive
