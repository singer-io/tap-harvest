# tap-harvest

A singer.io tap for extracting data from the Harvest REST API, written in python 3.

API V1 Author: Jordan Ryan (jordan@facetinteractive.com)
API V2 Author: Steven Hernandez (steven.hernandez@fostermade.co)

## Quick start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    ```

2. Create your tap's config file which should look like the following:

    ```json
    {
        "client_id": "OAUTH_CLIENT_ID",
        "client_secret": "OAUTH_CLIENT_SECRET",
        "refresh_token": "YOUR_OAUTH_REFRESH_TOKEN",
        "start_date": "2017-04-19T13:37:30Z",
        "user_agent": "MyApp (your.email@example.com)"
    }
    ```

3. [Optional] Create the initial state file

    ```json
    {
        "clients": "2000-01-01T00:00:00Z",
        "contacts": "2000-01-01T00:00:00Z",
        "estimate_item_categories": "2000-01-01T00:00:00Z",
        "estimate_messages": "2000-01-01T00:00:00Z",
        "estimates": "2000-01-01T00:00:00Z",
        "expense_categories": "2000-01-01T00:00:00Z",
        "expenses": "2000-01-01T00:00:00Z",
        "invoice_item_categories": "2000-01-01T00:00:00Z",
        "invoice_messages": "2000-01-01T00:00:00Z",
        "invoice_payments": "2000-01-01T00:00:00Z",
        "invoices": "2000-01-01T00:00:00Z",
        "project_tasks": "2000-01-01T00:00:00Z",
        "project_users": "2000-01-01T00:00:00Z",
        "projects": "2000-01-01T00:00:00Z",
        "roles": "2000-01-01T00:00:00Z",
        "tasks": "2000-01-01T00:00:00Z",
        "time_entries": "2000-01-01T00:00:00Z",
        "user_projects": "2000-01-01T00:00:00Z",
        "users": "2000-01-01T00:00:00Z"
    }
    ```

4. Run the application

    `tap-harvest` can be run with:

    ```bash
    tap-harvest --config config.json [--state state.json]
    ```

---

Copyright &copy; 2017 Stitch
