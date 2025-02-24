# tap-harvest

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Harvest API].
- Extracts the following resources:
    - Projects(https://github.com/singer-io)

    - Clients(https://github.com/singer-io)

    - Contacts(https://github.com/singer-io)

    - Estimate_item_categories(https://github.com/singer-io)

    - Estimate_line_items(https://github.com/singer-io)

    - Estimate_messages(https://github.com/singer-io)

    - Estimates(https://github.com/singer-io)

    - Expense_categories(https://github.com/singer-io)

    - Expenses(https://github.com/singer-io)

    - External_reference(https://github.com/singer-io)

    - Invoice_item_categories(https://github.com/singer-io)

    - Invoice_line_items(https://github.com/singer-io)

    - Invoice_messages(https://github.com/singer-io)

    - Invoice_payments(https://github.com/singer-io)

    - Invoices(https://github.com/singer-io)

    - Project_tasks(https://github.com/singer-io)

    - Project_users(https://github.com/singer-io)

    - Roles(https://github.com/singer-io)

    - Tasks(https://github.com/singer-io)

    - Time_entries(https://github.com/singer-io)

    - Time_entry_external_reference(https://github.com/singer-io)

    - User_project_tasks(https://github.com/singer-io)

    - User_projects(https://github.com/singer-io)

    - User_roles(https://github.com/singer-io)

    - (https://github.com/singer-io)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


** [projects](https://github.com/singer-io)**
- Data Key = projects
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [clients](https://github.com/singer-io)**
- Data Key = clients
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [contacts](https://github.com/singer-io)**
- Data Key = contacts
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [estimate_item_categories](https://github.com/singer-io)**
- Data Key = estimate_item_categories
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [estimate_line_items](https://github.com/singer-io)**
- Data Key = estimate_line_items
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [estimate_messages](https://github.com/singer-io)**
- Data Key = estimate_messages
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [estimates](https://github.com/singer-io)**
- Data Key = estimates
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [expense_categories](https://github.com/singer-io)**
- Data Key = expense_categories
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [expenses](https://github.com/singer-io)**
- Data Key = expenses
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [external_reference](https://github.com/singer-io)**
- Data Key = external_reference
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [invoice_item_categories](https://github.com/singer-io)**
- Data Key = invoice_item_categories
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [invoice_line_items](https://github.com/singer-io)**
- Data Key = invoice_line_items
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [invoice_messages](https://github.com/singer-io)**
- Data Key = invoice_messages
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [invoice_payments](https://github.com/singer-io)**
- Data Key = invoice_payments
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [invoices](https://github.com/singer-io)**
- Data Key = invoices
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [project_tasks](https://github.com/singer-io)**
- Data Key = project_tasks
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [project_users](https://github.com/singer-io)**
- Data Key = project_users
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [roles](https://github.com/singer-io)**
- Data Key = roles
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [tasks](https://github.com/singer-io)**
- Data Key = tasks
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [time_entries](https://github.com/singer-io)**
- Data Key = time_entries
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [time_entry_external_reference](https://github.com/singer-io)**
- Data Key = time_entry_external_reference
- Primary keys: ['time_entry_id', 'external_reference_id']
- Replication strategy: INCREMENTAL

** [user_project_tasks](https://github.com/singer-io)**
- Data Key = user_project_tasks
- Primary keys: ['user_id', 'project_task_id']
- Replication strategy: INCREMENTAL

** [user_projects](https://github.com/singer-io)**
- Data Key = user_projects
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

** [user_roles](https://github.com/singer-io)**
- Data Key = user_roles
- Primary keys: ['role_id', 'user_id']
- Replication strategy: INCREMENTAL

** [](https://github.com/singer-io)**
- Data Key = user_projects
- Primary keys: ['id']
- Replication strategy: INCREMENTAL



## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-harvest
    > pip install -e .
    ```
2. Dependent libraries. The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file.  The tap config file for this tap should include these entries:
   - `start_date` - the default value to use if no bookmark exists for an endpoint (rfc3339 date string)
   - `user_agent` (string, optional): Process and email for API logging purposes. Example: `tap-harvest <api_user_email@your_company.com>`
   - `request_timeout` (integer, `300`): Max time for which request should wait to get a response. Default request_timeout is 300 seconds.
   
    ```json
    {
        "client_id": "OAUTH_CLIENT_ID",
        "client_secret": "OAUTH_CLIENT_SECRET",
        "refresh_token": "YOUR_OAUTH_REFRESH_TOKEN",
        "start_date": "2017-04-19T13:37:30Z",
        "user_agent": "MyApp (your.email@example.com)",
        "request_timeout": 300
    }
    ```

3. [Optional] Create the initial state file

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
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
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-harvest --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md

    For Sync mode:
    ```bash
    > tap-harvest --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-harvest --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-harvest --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the harvest tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md
    ```bash
    > pylint tap_harvest -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools
    ```bash
    > tap-mixpanel --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

    #### Unit Tests

    Unit tests may be run with the following.

    ```
    python -m pytest --verbose
    ```

    Note, you may need to install test dependencies.

    ```
    pip install -e .'[dev]'
    ```
---

Copyright &copy; 2017 Stitch
