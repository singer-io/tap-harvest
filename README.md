# tap-harvest

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [Harvest API].
- Extracts the following resources:
    - [Projects](https://help.getharvest.com/api-v2/projects-api/projects/projects/)

    - [Clients](https://help.getharvest.com/api-v2/clients-api/clients/clients/)

    - [Contacts](https://help.getharvest.com/api-v2/clients-api/clients/contacts/)

    - [EstimateItemCategories](https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-item-categories/)

    - [Estimates](https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/)

        - [EstimateLineItems](https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/#the-estimate-line-item-object)

        - [EstimateMessages](https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/)

    - [ExpenseCategories](https://help.getharvest.com/api-v2/expenses-api/expenses/expense-categories/)

    - [Expenses](https://help.getharvest.com/api-v2/expenses-api/expenses/expenses/)

    - [InvoiceItemCategories](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-item-categories/)

    - [Invoices](https://help.getharvest.com/api-v2/invoices-api/invoices/invoices/)

        - [InvoiceLineItems](https://help.getharvest.com/api-v2/invoices-api/invoices/invoices#the-invoice-line-item-object)

        - [InvoiceMessages](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-messages/)

        - [InvoicePayments](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-payments/)

    - [ProjectTasks](https://help.getharvest.com/api-v2/projects-api/projects/task-assignments/)

    - [ProjectUsers](https://help.getharvest.com/api-v2/projects-api/projects/user-assignments/)

    - [Roles](https://help.getharvest.com/api-v2/roles-api/roles/roles/)
        - [UserRoles](https://help.getharvest.com/api-v2/roles-api/roles/roles/)

    - [Tasks](https://help.getharvest.com/api-v2/tasks-api/tasks/tasks/)

    - [TimeEntries](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)

        - [ExternalReference](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)

        - [TimeEntryExternalReference](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)

    - [Users](https://help.getharvest.com/api-v2/users-api/users/users/)
        - [UserProjects](https://help.getharvest.com/api-v2/users-api/users/project-assignments/)
            - [UserProjectTasks](https://help.getharvest.com/api-v2/users-api/users/project-assignments/)

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams


[projects](https://help.getharvest.com/api-v2/projects-api/projects/projects/)
- Data Key = projects
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[clients](https://help.getharvest.com/api-v2/clients-api/clients/clients/)
- Data Key = clients
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[contacts](https://help.getharvest.com/api-v2/clients-api/clients/contacts/)
- Data Key = contacts
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[estimate_item_categories](https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-item-categories/)
- Data Key = estimate_item_categories
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[estimate_line_items](https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/#the-estimate-line-item-object)
- Data Key = estimate_line_items
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[estimate_messages](https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/)
- Data Key = estimate_messages
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[estimates](https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/)
- Data Key = estimates
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[expense_categories](https://help.getharvest.com/api-v2/expenses-api/expenses/expense-categories/)
- Data Key = expense_categories
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[expenses](https://help.getharvest.com/api-v2/expenses-api/expenses/expenses/)
- Data Key = expenses
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[external_reference](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)
- Data Key = external_reference
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[invoice_item_categories](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-item-categories/)
- Data Key = invoice_item_categories
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[invoice_line_items](https://help.getharvest.com/api-v2/invoices-api/invoices/invoices#the-invoice-line-item-object)
- Data Key = invoice_line_items
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[invoice_messages](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-messages/)
- Data Key = invoice_messages
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[invoice_payments](https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-payments/)
- Data Key = invoice_payments
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[invoices](https://help.getharvest.com/api-v2/invoices-api/invoices/invoices/)
- Data Key = invoices
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[project_tasks](https://help.getharvest.com/api-v2/projects-api/projects/task-assignments/)
- Data Key = task_assignments
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[project_users](https://help.getharvest.com/api-v2/projects-api/projects/user-assignments/)
- Data Key = project_users
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[roles](https://help.getharvest.com/api-v2/roles-api/roles/roles/)
- Data Key = roles
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[tasks](https://help.getharvest.com/api-v2/tasks-api/tasks/tasks/)
- Data Key = tasks
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[time_entries](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)
- Data Key = time_entries
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[time_entry_external_reference](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)
- Data Key = time_entry_external_reference
- Primary keys: ['time_entry_id', 'external_reference_id']
- Replication strategy: INCREMENTAL

[user_project_tasks](https://help.getharvest.com/api-v2/)
- Data Key = user_project_tasks
- Primary keys: ['user_id', 'project_task_id']
- Replication strategy: INCREMENTAL

[user_projects](https://help.getharvest.com/api-v2/clients-api/clients/contacts/)
- Data Key = project_assignments
- Primary keys: ['id']
- Replication strategy: INCREMENTAL

[user_roles](https://help.getharvest.com/api-v2/)
- Data Key = user_roles
- Primary keys: ['role_id', 'user_id']
- Replication strategy: INCREMENTAL

[users](https://help.getharvest.com/api-v2/users-api/users/users/)
- Data Key = users
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
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-harvest <api_user_email@your_company.com>",
        "request_timeout": 300,
        ...
    }
    ```

    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "engage",
        "bookmarks": {
            "export": "2019-09-27T22:34:39.000000Z",
            "funnels": "2019-09-28T15:30:26.000000Z",
            "revenue": "2019-09-28T18:23:53Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-harvest --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md)

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md)

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
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md)
    ```bash
    > pylint tap_harvest -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.67/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools)
    ```bash
    > tap_harvest --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
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

Copyright &copy; 2019 Stitch
