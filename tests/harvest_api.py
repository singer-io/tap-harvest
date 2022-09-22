"""Harvest API Response, Access Token and Refresh Token."""

import datetime
import os
import random
from datetime import date

import requests
from base import BaseTapTest
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from spec import TapSpec
from tap_tester import LOGGER

_ACCESS_TOKEN = None
_ACCOUNT_ID = None
UPDATED_SINCE = datetime.datetime.strptime(
    TapSpec.DEFAULT_START_DATE, "%Y-%m-%dT%H:%M:%SZ"
).isoformat()

##############################
#  Access Token Methods      #
##############################


def _make_refresh_token_request():
    return requests.request(
        "POST",
        url="https://id.getharvest.com/api/v2/oauth2/token",
        json={
            "client_id": BaseTapTest.get_credentials(None)["client_id"],
            "client_secret": BaseTapTest.get_credentials(None)["client_secret"],
            "refresh_token": BaseTapTest.get_credentials(None)["refresh_token"],
            "grant_type": "refresh_token",
        },
        headers={"User-Agent": "dev@talend.com", "Content-Type": "application/json"},
    )


def _refresh_access_token():
    print("Refreshing access token")
    resp = _make_refresh_token_request()
    resp_json = {}
    try:
        resp_json = resp.json()
        global _ACCESS_TOKEN
        _ACCESS_TOKEN = resp_json["access_token"]
    except KeyError as key_err:
        if resp_json.get("error"):
            LOGGER.critical(resp_json.get("error"))
        if resp_json.get("error_description"):
            LOGGER.critical(resp_json.get("error_description"))
        raise key_err
    print("Got refreshed access token")


def get_access_token():
    global _ACCESS_TOKEN
    if _ACCESS_TOKEN is not None:
        return _ACCESS_TOKEN
    _refresh_access_token()
    return _ACCESS_TOKEN


def get_account_id():
    global _ACCOUNT_ID
    global _ACCESS_TOKEN
    if _ACCOUNT_ID is not None:
        return _ACCOUNT_ID
    response = requests.request(
        "GET",
        url="https://id.getharvest.com/api/v2/accounts",
        headers={
            "Authorization": "Bearer " + _ACCESS_TOKEN,
            "User-Agent": "dev@talend.com",
        },
    )
    _ACCOUNT_ID = str(response.json()["accounts"][0]["id"])
    return _ACCOUNT_ID


HEADERS = {
    "Authorization": f"Bearer {get_access_token()}",
    "Harvest-Account-Id": get_account_id(),
    "User-Agent": "dev@talend.com",
    "Content-Type": "application/json",
}


##############################
#  Request Methods           #
##############################


####################
# Create Methods   #
####################
# return value: key value pair for the create stream's ID

# def create_user(): free plan allows for only 1 user
#     assert None
#     email ="bob{}@someemail.com".format(random.randint(0,1000))
#     data = {"email":email,"first_name":"FirstName","last_name":"LastName","is_project_manager":True}
#     response = requests.post(url="https://api.harvestapp.com/v2/users", headers=HEADERS, json=data)
#     if response.status_code >= 400:
#         LOGGER.warning("A user with the email {} already exists. Removing user...")
#         response = requests.get(url="https://api.harvestapp.com/v2/users", headers=HEADERS)
#         all_users = response.json()['users']
#         for user in all_users:
#             if user['email'] == email:
#                 response = delete_user(user['id'])
#         response = requests.post(url="https://api.harvestapp.com/v2/users", headers=HEADERS, json=data)
#         if response.status_code >= 400:
#             LOGGER.warning("Unable to delete this user")
#             LOGGER.warning("create_user: {} {}".format(response.status_code, response.text))
#             assert None
#     return response.json()


def create_client():
    """required | name."""
    data = {
        "name": "New {} Client {}".format(
            random.randint(0, 1000000), random.randint(0, 1000000)
        ),
        "currency": "EUR",
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/clients", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        if data["name"] in [client["name"] for client in get_all("clients")]:
            data["name"] = "Retry Client"
            response = requests.post(
                url="https://api.harvestapp.com/v2/clients", headers=HEADERS, json=data
            )
            if response.status_code >= 400:
                LOGGER.warning(f"create_client: {response.status_code} {response.text}")
                assert None
            return response.json()
        LOGGER.warning(f"create_client: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_contact(client_id):
    """required | first_name, client_id."""
    data = {
        "client_id": client_id,
        "first_name": "George",
        "last_name": "Frank",
        "email": "george{}{}@example.com".format(
            random.randint(0, 1000000), random.randint(0, 1000000)
        ),
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/contacts", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_contact: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_estimate(client_id):
    """used for estimate_line_items as well."""
    line_items = [
        {
            "kind": "Service",
            "description": f"estimate description {random.randint(0, 1000000)}",
            "unit_price": random.randint(1, 1000000),
        }
    ]
    data = {
        "client_id": client_id,
        "subject": f"ABC{random.randint(0, 100)} Project Quote",
        "line_items": line_items,
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/estimates", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_estimate: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_estimate_item_category():
    data = {
        "name": "Random {} Category {}".format(
            random.randint(0, 1000000), random.randint(0, 1000000)
        )
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/estimate_item_categories",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "create_estimate_item_category: {} {}".format(
                response.status_code, response.text
            )
        )
        assert None
    return response.json()


def create_estimate_message(estimate_id):
    """required | recipients."""
    rand = random.randint(0, 1000000)
    data = {
        "subject": f"Estimate #{estimate_id}_{random.randint(0, 1000000)}",
        "body": "Here is our estimate.",
        "send_me_a_copy": False,
        "recipients": [
            {
                "name": f"Rando {rand}",
                "email": f"rando{rand}@example.com",
            }
        ],
    }
    response = requests.post(
        url=f"https://api.harvestapp.com/v2/estimates/{estimate_id}/messages",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"create_estimate_message: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def create_expense(project_id):
    """required | project_id, expense_category_id, spent_date."""
    rand_month = random.randint(1, 5)
    spent_date = date.today() - relativedelta(months=rand_month)
    data = {
        "project_id": project_id,
        "expense_category_id": get_random("expense_categories"),
        "spent_date": str(spent_date),
        "total_cost": random.randint(1, 10000000),
    }
    response = None

    response = requests.post(
        url="https://api.harvestapp.com/v2/expenses", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_expense: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_expense_category():
    """required | name."""
    data = {
        "name": "Expense {} category {}".format(
            random.randint(0, 1000000), random.randint(0, 1000000)
        )
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/expense_categories",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"create_expense_category: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def create_invoice(client_id, estimate_id: str = "", project_id: str = ""):
    if estimate_id == "":
        estimate_id = get_random("estimates")
    if project_id == "":
        project_id = get_random("projects")
    rand_month = random.randint(1, 12)
    due_date = date.today() + relativedelta(months=rand_month)
    line_items = [
        {
            "kind": "Service",
            "description": f"ABC{random.randint(0, 1000000)} Project",
            "unit_price": random.randint(1, 1000000),
            "project_id": project_id,
        }
    ]
    data = {
        "client_id": client_id,
        "subject": "ABC Project Quote",
        "due_date": f"{due_date}",
        "line_items": line_items,
        "estimate_id": estimate_id,
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/invoices", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_invoice: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_invoice_message(invoice_id):
    data = {
        "subject": f"Invoice #{invoice_id}",
        "body": "The invoice is attached below.",
        "attach_pdf": True,
        "send_me_a_copy": False,
        "recipients": [{"name": "Richard Roe", "email": "richardroe@example.com"}],
    }
    response = requests.post(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}/messages",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"create_invoice_message: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def create_invoice_payment(invoice_id):
    """amount | required."""
    invoice = requests.get(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}",
        headers=HEADERS,
    )
    amount = invoice.json()["amount"]
    rand_month = random.randint(1, 12)
    paid_date = date.today() - relativedelta(months=rand_month)
    data = {
        "amount": random.randint(1, amount - 1),
        "paid_at": str(paid_date),
        "notes": "Paid by phone",
    }
    response = requests.post(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}/payments",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"create_invoice_payment: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def create_invoice_item_category():
    """amount | requireed."""
    rand_1 = random.randint(0, 1000000)
    rand_2 = random.randint(0, 1000000)
    data = {"name": f"Category {rand_1} {rand_2}"}
    response = requests.post(
        url="https://api.harvestapp.com/v2/invoice_item_categories",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "create_invoice_item_category: {} {}".format(
                response.status_code, response.text
            )
        )
        assert None
    return response.json()


def create_project(client_id):
    data = {
        "client_id": client_id,
        "name": "Created {} Project {}".format(
            random.randint(0, 1000000), random.randint(0, 1000000)
        ),
        "is_billable": True,
        "bill_by": "Project",
        "hourly_rate": 100.0,
        "budget_by": "project",
        "budget": 10000,
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/projects", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_project: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_project_task(project_id, task_id):
    """a.k.a 'task_assignment."""
    data = {"task_id": task_id, "hourly_rate": random.randint(0, 100)}
    response = requests.post(
        url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(
            project_id
        ),
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"create_task_assignment: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def create_project_user(project_id, user_id):
    data = {
        "user_id": user_id,
        "use_default_rates": False,
        "hourly_rate": 42.0,
        "budget": random.randint(100, 1000000),
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/projects/{}/user_assignments".format(
            project_id
        ),
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_project_user: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_role():
    """required | name."""
    data = {
        "name": "Manager #{}-{}".format(
            random.randint(0, 1000000), random.randint(0, 1000000)
        ),
        "user_ids": [get_random("users")],
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/roles", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_roles: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_task():
    rand_1 = random.randint(0, 1000000)
    rand_2 = random.randint(0, 1000000)
    rand_3 = random.randint(0, 1000000)
    data = {
        "name": f"{rand_1} Task {rand_2}  Name {rand_3}",
        "hourly_rate": 42.0,
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/tasks", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_task: {response.status_code} {response.text}")
        assert None
    return response.json()


def create_time_entry(project_id, task_id):
    """required | project_id, task_id, spend_date."""
    rand_month = random.randint(1, 12)
    spent_date = date.today() - relativedelta(months=rand_month)
    data = {
        "project_id": project_id,
        "task_id": task_id,
        "spent_date": str(spent_date),
        "hours": 1.0,
        "external_reference": {
            "id": random.randint(5, 1000000),
            "group_id": random.randint(1, 1000000),
            "permalink": "https://help.getharvest.com/api-v2/",
        },
    }
    response = requests.post(
        url="https://api.harvestapp.com/v2/time_entries", headers=HEADERS, json=data
    )
    if response.status_code >= 400:
        LOGGER.warning(f"create_time_entry: {response.status_code} {response.text}")
        assert None
    return response.json()


####################
# Update Methods   #
####################
# return value: response from a patch


def update_client(client_id):
    data = {"name": f"client test name #{random.randint(0, 10000000)}"}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/clients/{client_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_client: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_contact(contact_id):
    data = {"title": f"Title{random.randint(0, 1000000)}"}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/contacts/{contact_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_contact: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_estimate(estimate_id):
    """used for estimate_line_items as well."""
    data = {"subject": f"Subject {random.randint(0, 1000000)}"}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/estimates/{estimate_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_estimate: {response.status_code} {response.text}")
        assert None
    return response.json()


# def update_estimate_line_items(estimate_id):
#     line_items = [{"kind":"Service","description":"updated estimate description {}".format(random.randint(0,1000000)),
#                    "unit_price":42.0}]
#     data = {"line_items":line_items}
#     response = requests.patch(url="https://api.harvestapp.com/v2/estimates/{}".format(estimate_id), headers=HEADERS, json=data)
#     if response.status_code >= 400:
#         LOGGER.warning('update_estimate_line_items: {} {}'.format(response.status_code, response.text))
#         assert None
#     return response.json()


def update_estimate_message(estimate_id):
    # mark = ["accept", "decline", "sent", "re-open", "view", "invoice"]
    data = {"event_type": "accept"}
    response = requests.post(
        url=f"https://api.harvestapp.com/v2/estimates/{estimate_id}/messages",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"update_estimate_message: {response.status_code} {response.text}"
        )
        data = {"event_type": "view"}
        response = requests.post(
            url="https://api.harvestapp.com/v2/estimates/{}/messages".format(
                estimate_id
            ),
            headers=HEADERS,
            json=data,
        )
        if response.status_code >= 400:
            LOGGER.warning(
                "update_estimate_message: {} {}".format(
                    response.status_code, response.text
                )
            )
            assert None
    return response.json()


def update_estimate_item_category(category_id):
    data = {"name": f"Updated Category {random.randint(0, 1000000)}"}
    response = requests.patch(
        url="https://api.harvestapp.com/v2/estimate_item_categories/{}".format(
            category_id
        ),
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "update_estimate_item_category: {} {}".format(
                response.status_code, response.text
            )
        )
        if response.status_code == 422:  # Name is in use
            data = {"name": f"Different Updated Category {0}"}
            response = requests.patch(
                url="https://api.harvestapp.com/v2/estimate_item_categories/{}".format(
                    category_id
                ),
                headers=HEADERS,
                json=data,
            )
            return response.json()
        assert None
    return response.json()


def update_expense(expense_id):
    spent_date = date.today() - relativedelta(months=random.randint(7, 12))
    data = {"spent_date": str(spent_date)}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/expenses/{expense_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_expense: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_expense_category(category_id):
    data = {"name": f"Updated Category {random.randint(0, 1000000)}"}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/expense_categories/{category_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"update_expense_category: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def update_invoice(invoice_id):
    data = {"purchase_order": f"{random.randint(1000, 10000000)}"}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_invoice: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_invoice_message(invoice_id):
    mark = ["close", "send", "re-open", "view", "draft"]
    for event_type in mark:
        try:
            data = {"event_type": event_type}
            response = requests.post(
                url="https://api.harvestapp.com/v2/invoices/{}/messages".format(
                    invoice_id
                ),
                headers=HEADERS,
                json=data,
            )
            if response.status_code >= 400:
                LOGGER.warning(
                    "update_invoice_message: {} {}".format(
                        response.status_code, response.text
                    )
                )
                assert None
            return response.json()
        except:
            pass
    assert None


def update_invoice_payment(invoice_id):
    rand_month = random.randint(1, 12)
    paid_date = date.today() - relativedelta(months=rand_month)
    data = {
        "amount": random.randint(15000, 75000) / 100,
        "paid_at": str(paid_date),
        "notes": "Paid by phone",
    }
    response = requests.post(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}/payments",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"create_invoice_payment: {response.status_code} {response.text}"
        )
        assert None
    return response.json()


def update_invoice_item_category(category_id):
    data = {"name": f"Updated Category {random.randint(0, 1000000)}"}
    response = requests.patch(
        url="https://api.harvestapp.com/v2/invoice_item_categories/{}".format(
            category_id
        ),
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "update_invoice_item_category: {} {}".format(
                response.status_code, response.text
            )
        )
        assert None
    return response.json()


def update_project(project_id):
    data = {"hourly_rate": str(random.randint(20, 90))}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/projects/{project_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_project: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_project_task(project_id, task_assignment_id):
    """a.k.a. task_assignment"""
    data = {"budget": random.randint(1000, 1000000000)}
    response = requests.patch(
        url="https://api.harvestapp.com/v2/projects/{}/task_assignments/{}".format(
            project_id, task_assignment_id
        ),
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"ERROR in Updating task {task_assignment_id}")
        assert None
    return response.json()


def update_project_user(project_id, project_user_id):
    data = {"budget": random.randint(15, 80)}
    response = requests.patch(
        url="https://api.harvestapp.com/v2/projects/{}/user_assignments/{}".format(
            project_id, project_user_id
        ),
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_project_user: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_role(role_id, user_ids=None):
    data = {"name": f"Update Manager #{random.randint(0, 1000000)}"}
    if user_ids is not None:
        data["user_ids"] = user_ids
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/roles/{role_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_role: {response.status_code} {response.text}")
        assert None
    return response.json()


def update_task(task_id):
    data = {"name": f"Updated Task Name {random.randint(0, 1000000)}"}
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/tasks/{task_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        data = {
            "name": "Updated Task {} Name {}".format(
                random.randint(0, 1000000), random.randint(0, 1000000)
            )
        }
        response = requests.patch(
            url=f"https://api.harvestapp.com/v2/tasks/{task_id}",
            headers=HEADERS,
            json=data,
        )
        if response.status_code >= 400:
            LOGGER.warning(f"update_task: {response.status_code} {response.text}")
            assert None
    return response.json()


def update_time_entry(time_entry_id):
    data = {
        "notes": f"Updated Time Entry Note {random.randint(0, 1000000)}",
        "external_reference": {
            "id": random.randint(5, 1000000),
            "group_id": random.randint(100, 500),
            "permalink": "https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/",
        },
    }
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/time_entries/{time_entry_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_time_entry: {response.status_code} {response.text}")
        data = {
            "notes": f"Updated Time Entry Note {random.randint(0, 1000000)}",
            "external_reference": {
                "id": random.randint(5, 1000000),
                "group_id": random.randint(100, 500),
                "permalink": "https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/",
            },
        }
        response = requests.patch(
            url=f"https://api.harvestapp.com/v2/time_entries/{time_entry_id}",
            headers=HEADERS,
            json=data,
        )
        if response.status_code >= 400:
            LOGGER.warning(f"update_time_entry: {response.status_code} {response.text}")
            assert None
    return response.json()


def update_user(user_id, role_names=None):
    rand_2 = random.randint(1000000000, 9999999999)
    data = {"telephone": f"{rand_2}"}
    if role_names is not None:
        data["roles"] = role_names
    response = requests.patch(
        url=f"https://api.harvestapp.com/v2/users/{user_id}",
        headers=HEADERS,
        json=data,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"update_user: {response.status_code} {response.text}")
        assert None
    return response.json()


####################
# Get All          #
####################
# return value: list of current streams
def get_all(stream):
    response = requests.get(
        url=f"https://api.harvestapp.com/v2/{stream}", headers=HEADERS
    )
    if response.status_code >= 400:
        LOGGER.warning(f"get_all_{stream}: {response.status_code} {response.text}")
        assert None
    return response.json()[stream]


# def get_all(stream):
#     return_response =[]
#     response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS)
#     pages = response.json()["total_pages"]
#     for _ in pages:
#         if response.status_code >= 400:
#             LOGGER.warning("get_all_{}: {} {}".format(stream, response.status_code, response.text))
#             assert None
#         return_response.append(response.json()[stream])
#         response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS)

# Complex streams require their own functions
def get_user_projects(user_id):
    response = requests.get(
        url="https://api.harvestapp.com/v2/users/{}/project_assignments".format(
            user_id
        ),
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "get_all_project_users: {} {}".format(
                stream, response.status_code, response.text
            )
        )
        assert None
    return response.json()["project_assignments"]


def get_user_projects_all():
    response = requests.get(
        url="https://api.harvestapp.com/v2/users/me/project_assignments",
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "get_all_project_users: {} {}".format(
                stream, response.status_code, response.text
            )
        )
        assert None
    return response.json()["project_assignments"]


def get_all_estimate_messages(estimate_id):
    response = requests.get(
        url=f"https://api.harvestapp.com/v2/estimates/{estimate_id}/messages",
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "get_all_estimate_messages: {} {}".format(
                response.status_code, response.text
            )
        )
        assert None
    return response.json()[stream]


def get_all_invoice_messages(invoice_id):
    response = requests.get(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}/messages",
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "get_all_invoice_messages: {} {}".format(
                response.status_code, response.text
            )
        )
        assert None
    return response.json()["invoice_messages"]


def get_all_invoice_payments(invoice_id):
    response = requests.get(
        url=f"https://api.harvestapp.com/v2/invoices/{invoice_id}/payments",
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            "get_all_invoice_payments: {} {}".format(
                response.status_code, response.text
            )
        )
        assert None
    return response.json()["invoice_payments"]


def get_all_project_tasks(project_id):
    response = requests.get(
        url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(
            project_id
        ),
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"get_all_project_tasks: {response.status_code} {response.text}")
        assert None
    num_project_tasks = len(response.json()["task_assignments"]) - 1
    return response.json()["task_assignments"][random.randint(0, num_project_tasks)][
        "id"
    ]


####################
# Get Counts       #
####################
# return value: tuple (number of pages, number of records)
def get_stream_counts(stream, parameters=[("updated_since", UPDATED_SINCE)]):
    if stream == "estimate_line_items":
        stream = (
            "estimates"  # All estimates that are created by us have a line_items field
        )
    if stream == "invoice_line_items":
        stream = (
            "invoices"  # All invoices that are created by us have a line_items field
        )
    if stream == "project_tasks":
        stream = "task_assignments"
    if stream == "time_entry_external_reference":
        stream = "time_entries"  # All invoices that are created by us have a line_items field
    if stream == "external_reference":
        stream = "time_entries"  # All invoices that are created by us have a line_items field
    response = requests.get(
        url=f"https://api.harvestapp.com/v2/{stream}",
        headers=HEADERS,
        params=parameters,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"get_{stream}_count: {response.status_code} {response.text}")
        assert None
    return (
        response.json()["total_pages"],
        response.json()["total_entries"],
        list(map(lambda x: {"id": x["id"]}, response.json()[stream])),
    )


####################
# Get Random       #
####################
# Works for simple streams (simple = has one pk and does not rely on other streams for access)
# return value: ID value from the stream
def get_random(stream):
    """get random instance of stream | role, contact, estimate, invoice, client
    ..."""
    response = requests.get(
        url=f"https://api.harvestapp.com/v2/{stream}", headers=HEADERS
    )
    if response.status_code >= 400:
        LOGGER.warning(f"get_random_{stream}: {response.status_code} {response.text}")
        assert None
    num_streams = len(response.json()[stream]) - 1
    return response.json()[stream][random.randint(0, num_streams)]["id"]


# Complex streams require their own functions
def get_random_project_user(project_id):
    response = requests.get(
        url="https://api.harvestapp.com/v2/projects/{}/user_assignments".format(
            project_id
        ),
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"get_random_project_user: {response.status_code} {response.text}"
        )
        assert None
    num_users = len(response.json()["user_assignments"]) - 1
    return response.json()["user_assignments"][random.randint(0, num_users)]["id"]


def get_random_project_task(project_id):
    response = requests.get(
        url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(
            project_id
        ),
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(
            f"get_random_project_taks: {response.status_code} {response.text}"
        )
        assert None
    num_project_tasks = len(response.json()["task_assignments"]) - 1
    return response.json()["task_assignments"][random.randint(0, num_project_tasks)][
        "id"
    ]


def get_random_task(project_id: str = None):
    if project_id:
        response = requests.get(
            url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(
                project_id
            ),
            headers=HEADERS,
        )
        if response.status_code >= 400:
            LOGGER.warning(f"get_random_task: {response.status_code} {response.text}")
            assert None
        num_project_tasks = len(response.json()["task_assignments"]) - 1
        return response.json()["task_assignments"][
            random.randint(0, num_project_tasks)
        ]["task"]["id"]
    response = requests.get(url="https://api.harvestapp.com/v2/tasks", headers=HEADERS)
    if response.status_code >= 400:
        LOGGER.warning(f"get_random_task: {response.status_code} {response.text}")
        assert None
    num_tasks = len(response.json()["tasks"]) - 1
    return response.json()["tasks"][random.randint(0, num_tasks)]["id"]


####################
# Get Random       #
####################
# Use to alter our field expectations to mimic tap field naming based
# on things like client: {id, name} != client_id
def get_fields(stream):
    """Checks a stream's keys (using the json response from an api call) for
    values that are dictionaries.

    This indicates it has subfields. returns the keys with necessary id-
    adjusted key names
    """
    keys = set(stream.keys())

    reformed = set()
    removed = set()

    # Find the fields which have subfields
    has_sub_fields = [key for key in keys if type(stream[key]) is dict]

    # Some fields are actually child streams
    is_stream = ["line_items"]  # , 'external_reference']
    for field in keys:
        if field in is_stream:
            removed.add(field)

    # Reform the fields which have subfields (which have values of type dict)
    for field in has_sub_fields:
        # Queue main field for removal
        removed.add(field)

        # If field has is stream remove it
        if field in is_stream:
            removed.add(field)
            continue

        # If field has a subfield of 'id' keep only 'field_id'
        elif "id" in stream[field].keys():
            reformed.add(field + "_id")
            continue

        # Otherwise keep all subfields
        for key in stream[field].keys():
            reformed.add(field + "_" + key)

    # Some fields cannot be generated via api b/c of Harvest limitations, so
    # ensure fields which have subfields but have values are null are still captured
    has_sub_fields = ["retainer"]

    # Get all null (value: None) fields to check for subfields
    null_fields = [key for key in keys if stream[key] is None]
    for field in null_fields:

        if field in has_sub_fields:
            removed.add(field)
            reformed.add(field + "_id")
            continue

        removed.add(field)

    # Return reformed keys and any key that did not need reforming
    return reformed.union(keys - removed)


####################
# Delete Method    #
####################
# return value: response from delete
def delete_stream(stream, stream_id):
    """delete specific stream | role, contact, estimate, invoice, client ..."""
    response = requests.delete(
        url=f"https://api.harvestapp.com/v2/{stream}/{stream_id}",
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"delete_{stream}: {response.status_code} {response.text}")
    return response.json()


# Complex streams require their own functions
def delete_project_user(project_id, project_user_id):
    response = requests.delete(
        url="https://api.harvestapp.com/v2/projects/{}/user_assignments{}".format(
            project_id, project_user_id
        ),
        headers=HEADERS,
    )
    if response.status_code >= 400:
        LOGGER.warning(f"delete_project_user: {response.status_code} {response.text}")
    return response.json()


def set_up(cls, rec_count=2):
    """
    Function to create records for all the streams before starting test.

    Args:
        rec_count (int, optional): Number of records to generate. Defaults to 2.
    """

    LOGGER.info("Start Setup")

    # ###  BREAKDOWN for cls._master  #############################################################################
    # Each stream has an information map which dictates how we create data and how we test the stream
    # cls._master = {'stream' : {"test": True,              | whether or not we are testing this stream
    #                            "child": False,            | if this stream is a child of another stream
    #                            "delete_ me": [],          | array of ids for records that we cleanup post test
    #                            "expected_fields": set(),  | set of expected fields the target should receive
    #                            "total": 0}}               | total record count for the stream
    ##############################################################################################################

    cls._master = {
        "clients": {"test": True, "child": False},
        "contacts": {"test": True, "child": False},
        "estimate_item_categories": {"test": True, "child": False},
        "estimate_line_items": {
            "test": True,
            "child": True,
        },
        "estimate_messages": {
            "test": False,
            "child": False,
        },  # BUG see (https://github.com/singer-io/tap-harvest/issues/35)
        "estimates": {"test": True, "child": False},
        "expense_categories": {"test": True, "child": False},
        "expenses": {"test": True, "child": False},
        "external_reference": {
            "test": True,
            "child": True,
        },
        "invoice_item_categories": {"test": True, "child": False},
        "invoice_line_items": {
            "test": True,
            "child": True,
        },
        "invoice_messages": {
            "test": False,
            "child": False,
        },  # BUG (see issue/35 ^ )
        "invoice_payments": {
            "test": False,
            "child": False,
        },  # BUG (see issue/35 ^ )
        "invoices": {"test": True, "child": False},
        "project_tasks": {"test": True, "child": False},
        "project_users": {
            "test": False,
            "child": False,
        },  # Unable to test - limited by projects
        "projects": {"test": False, "child": False},  # Unable to test - limit 2
        "roles": {"test": True, "child": False},
        "tasks": {"test": True, "child": False},
        "time_entries": {"test": True, "child": False},
        "time_entry_external_reference": {"test": True, "child": True},
        "user_project_tasks": {
            "test": False,
            "child": False,
        },  # Unable to test - limited by users
        "user_projects": {
            "test": False,
            "child": False,
        },  # Unable to test - limited by projects
        "user_roles": {"test": False, "child": False},  # TODO TEST THIS STREAM
        "users": {"test": False, "child": False},  # Unable to test - limit 1
    }

    # Assign attributes to each stream that is under test
    for stream in cls._master:
        cls._master[stream]["delete_me"] = []
        cls._master[stream]["expected_fields"] = set()
        if cls._master[stream]["test"] or stream == "projects":
            _, record_count, rec_ids = get_stream_counts(stream)
            LOGGER.info(f"{stream} has {record_count} records")
            cls._master[stream]["delete_me"] += rec_ids
            cls._master[stream]["total"] = record_count
        if cls._master[stream]["test"] == False:
            cls._master[stream]["total"] = 0

    # Protect against surviving projects corrupting the test
    project = ""
    if cls._master["projects"]["total"] > 1:
        delete_stream("projects", get_random("projects"))  # This also deletes expenses
        cls._master["projects"]["total"] -= 1

    # Create dummy data in specific streams prior to first sync to ensure they are captured
    for itter in range(rec_count):
        LOGGER.info(f"Creating {itter + 1} round(s) of data ...")

        # Clients
        if cls._master["clients"]["total"] < rec_count:
            LOGGER.info("  Creating Client")
            client = create_client()
            cls._master["clients"]["total"] += 1
            # BUG (https://github.com/singer-io/tap-harvest/issues/37)
            remove_expected = {"statement_key"}  # field removed so tests pass
            expectations = get_fields(client) - remove_expected
            cls._master["clients"]["expected_fields"].update(expectations)
            cls._master["clients"]["delete_me"].append({"id": client["id"]})

        # Contacts
        if cls._master["contacts"]["total"] < rec_count:
            LOGGER.info("  Creating Contact")
            contact = create_contact(get_random("clients"))
            cls._master["contacts"]["total"] += 1
            remove_expected = {"client"}  # this is a correct expectation
            expectations = get_fields(contact) - remove_expected
            cls._master["contacts"]["expected_fields"].update(expectations)
            cls._master["contacts"]["delete_me"].append({"id": contact["id"]})

        # Roles
        if cls._master["roles"]["total"] < rec_count:
            LOGGER.info("  Creating Role")
            role = create_role()
            cls._master["roles"]["total"] += 1
            # BUG (see clients bug above)
            remove_expected = {"user_ids"}  # field removed
            expectations = get_fields(role) - remove_expected
            cls._master["roles"]["expected_fields"].update(expectations)
            cls._master["roles"]["delete_me"].append({"id": role["id"]})

        # Projects
        if cls._master["projects"]["total"] < 1:
            LOGGER.info("  Creating Project")
            project = create_project(get_random("clients"))
            cls._master["projects"]["total"] += 1
            cls._master["projects"]["expected_fields"].update(project.keys())
            cls._master["projects"]["delete_me"].append({"id": project["id"]})

        # Project users
        if cls._master["project_users"]["total"] < rec_count:
            LOGGER.info("  Creating Project Users")
            project_id = cls._master["projects"]["delete_me"][-1]["id"]
            project_user = create_project_user(project_id, get_random("users"))
            cls._master["project_users"]["total"] += 1
            cls._master["project_users"]["expected_fields"].update(project_user.keys())
            cls._master["project_users"]["delete_me"].append({"id": project_user["id"]})

        # Tasks
        if (
            cls._master["tasks"]["total"] < rec_count
            or cls._master["project_tasks"]["total"] < rec_count
            or cls._master["time_entries"]["total"] < rec_count
            or cls._master["external_reference"]["total"] < rec_count
        ):
            LOGGER.info("  Creating Task")
            task = create_task()
            cls._master["tasks"]["total"] += 1
            cls._master["tasks"]["expected_fields"].update(get_fields(task))
            cls._master["tasks"]["delete_me"].append({"id": task["id"]})
            project_id = cls._master["projects"]["delete_me"][-1]["id"]
            task_id = task["id"]

            # Project_Tasks
            LOGGER.info("  Creating Project_Task")
            project_task = create_project_task(project_id, task_id)
            cls._master["project_tasks"]["total"] += 1
            cls._master["project_tasks"]["expected_fields"].update(
                get_fields(project_task)
            )
            cls._master["project_tasks"]["delete_me"].append({"id": project_task["id"]})

            # Time Entries;
            LOGGER.info("  Creating Time Entry")
            time_entry = create_time_entry(project_id, task_id)
            cls._master["time_entries"]["total"] += 1
            # NOTE: time_entries has fields which are set to null in a create and so do not get picked up
            # automatically when checking the keys, so we set partial expectations manually.
            add_expected = {"invoice_id"}
            remove_expected = {
                "invoice",
                "timer_started_at",
                "rounded_hours",
                "hours_without_timer",
            }  # BUG (for timer_started_at see clients bug above)
            expectations = add_expected.union(get_fields(time_entry) - remove_expected)
            cls._master["time_entries"]["expected_fields"].update(expectations)
            cls._master["time_entries"]["delete_me"].append({"id": time_entry["id"]})

            # External Reference
            reference = time_entry["external_reference"]
            cls._master["external_reference"]["total"] += 1
            cls._master["external_reference"]["expected_fields"].update(
                get_fields(reference)
            )

            # Time Entry External Reference
            cls._master["time_entry_external_reference"]["total"] += 1
            # NOTE: time_entry_external_reference is a connection b/t time_entry and external_reference
            # and is created implicitly by the creation of a time entry, so expecations must be set manually.
            cls._master["time_entry_external_reference"]["expected_fields"].update(
                {"time_entry_id", "external_reference_id"}
            )

        # Estimates
        if (
            cls._master["estimates"]["total"] < rec_count
            or cls._master["estimate_messages"]["total"] < rec_count
        ):
            LOGGER.info("  Creating Estimate")
            client_id = cls._master["clients"]["delete_me"][0]["id"]
            estimate = create_estimate(client_id)
            cls._master["estimates"]["total"] += 1
            # BUG (see clients bug above)
            remove_expected = {
                "declined_at",
                "accepted_at",
            }  # field removed so tests pass
            expectations = get_fields(estimate) - remove_expected
            cls._master["estimates"]["expected_fields"].update(expectations)
            cls._master["estimates"]["delete_me"].append({"id": estimate["id"]})

            # Estimate Line Items
            cls._master["estimate_line_items"]["expected_fields"].update(
                estimate["line_items"][0].keys()
            )
            cls._master["estimate_line_items"]["expected_fields"].update(
                {"estimate_id"}
            )
            cls._master["estimate_line_items"]["total"] += 1

            # Estimate Messages (BUG See cls._master)
            LOGGER.info("  Creating Estimate_Message")
            estimate_message = create_estimate_message(estimate["id"])
            cls._master["estimate_messages"]["total"] += 1
            cls._master["estimate_messages"]["expected_fields"].update(
                estimate_message.keys()
            )
            cls._master["estimate_messages"]["delete_me"].append(
                {"id": estimate_message["id"]}
            )

        # Invoices
        if (
            cls._master["invoices"]["total"] < rec_count
            or cls._master["invoice_line_items"]["total"] < rec_count
            or cls._master["invoice_messages"]["total"] < rec_count
            or cls._master["invoice_payments"]["total"] < rec_count
        ):
            LOGGER.info("  Creating Invoice")
            invoice = create_invoice(
                client_id=get_all("projects")[0]["client"]["id"],
                project_id=get_all("projects")[0]["id"],
            )
            cls._master["invoices"]["total"] += 1
            # BUG see bug in clients above, removing so tests pass
            remove_expected = {
                "closed_at",
                "paid_at",
                "recurring_invoice_id",
                "paid_date",
                "period_start",
                "period_end",
            }  # 'sent_at',
            expectations = get_fields(invoice) - remove_expected
            cls._master["invoices"]["expected_fields"].update(expectations)
            cls._master["invoices"]["delete_me"].append({"id": invoice["id"]})

            # Invoice Messages (BUG See cls._master)
            LOGGER.info("  Creating Invoice Messages")
            invoice_message = create_invoice_message(invoice["id"])
            cls._master["invoice_messages"]["total"] += 1
            cls._master["invoice_messages"]["expected_fields"].update(
                invoice_message.keys()
            )
            cls._master["invoice_messages"]["delete_me"].append(
                {"id": invoice_message["id"]}
            )

            # Invoice Line Items
            cls._master["invoice_line_items"]["total"] += 1
            cls._master["invoice_line_items"]["expected_fields"].update(
                get_fields(invoice["line_items"][0])
            )
            cls._master["invoice_line_items"]["expected_fields"].update({"invoice_id"})

            # Invoice Payments (BUG See cls._master)
            LOGGER.info("  Creating Invoice Payments")
            invoice_payment = create_invoice_payment(invoice["id"])
            cls._master["invoice_payments"]["total"] += 1
            cls._master["invoice_payments"]["expected_fields"].update(
                invoice_payment.keys()
            )
            cls._master["invoice_payments"]["delete_me"].append(
                {"id": invoice_payment["id"]}
            )

        # Expenses
        if cls._master["expenses"]["total"] < rec_count:
            LOGGER.info("  Creating Expense")
            expense = create_expense(get_all("projects")[0]["id"])
            cls._master["expenses"]["total"] += 1
            # NOTE: Expesnes has fields which cannot be generated by api call, so we will set
            # partial expectations manually.
            add_expected = {
                "receipt_url",
                "receipt_file_name",
                "receipt_content_type",
                "receipt_file_size",
                "invoice_id",
            }
            remove_expected = {"receipt", "invoice"}
            expectations = add_expected.union(get_fields(expense) - remove_expected)
            cls._master["expenses"]["expected_fields"].update(expectations)
            cls._master["expenses"]["delete_me"].append({"id": expense["id"]})

        # Invoice Item Categories
        if cls._master["invoice_item_categories"]["total"] < rec_count:
            LOGGER.info("  Creating Invoice Item Category")
            invoice_category = create_invoice_item_category()
            cls._master["invoice_item_categories"]["total"] += 1
            cls._master["invoice_item_categories"]["expected_fields"].update(
                invoice_category.keys()
            )
            cls._master["invoice_item_categories"]["delete_me"].append(
                {"id": invoice_category["id"]}
            )

        # Expense Categories
        if cls._master["expense_categories"]["total"] < rec_count:
            LOGGER.info("  Creating Expense Category")
            category = create_expense_category()
            cls._master["expense_categories"]["total"] += 1
            cls._master["expense_categories"]["expected_fields"].update(category.keys())
            cls._master["expense_categories"]["delete_me"].append(
                {"id": category["id"]}
            )

        # Estimate Item Categories
        if cls._master["estimate_item_categories"]["total"] < rec_count:
            LOGGER.info("  Creating Estimate Item Category")
            category = create_estimate_item_category()
            cls._master["estimate_item_categories"]["total"] += 1
            cls._master["estimate_item_categories"]["expected_fields"].update(
                category.keys()
            )
            cls._master["estimate_item_categories"]["delete_me"].append(
                {"id": category["id"]}
            )


def tear_down(cls):
    """
    Delete all the streams records.
    (Except tasks and time_entries as they can not be deleted.)
    """
    LOGGER.info("Start Teardown")

    # Estimates
    for estimate in cls._master["estimates"]["delete_me"]:
        delete_stream("estimates", estimate["id"])
    for category in cls._master["estimate_item_categories"]["delete_me"]:
        delete_stream("estimate_item_categories", category["id"])
    ### Time Entries and Tasks can not be deleted as they are time tracked
    # Invoices
    for invoice in cls._master["invoices"]["delete_me"]:
        delete_stream("invoices", invoice["id"])
    for category in cls._master["invoice_item_categories"]["delete_me"]:
        delete_stream("invoice_item_categories", category["id"])
    # Expenses
    for expense in cls._master["expenses"]["delete_me"]:
        delete_stream("expenses", expense["id"])
    # Projects
    for project in cls._master["projects"]["delete_me"]:
        delete_stream("projects", project["id"])
    # Contacts
    for contact in cls._master["contacts"]["delete_me"]:
        delete_stream("contacts", contact["id"])
    # Clients
    for client in cls._master["clients"]["delete_me"]:
        delete_stream("clients", client["id"])
    # Expense Categories
    for expense_category in cls._master["expense_categories"]["delete_me"]:
        delete_stream("expense_categories", expense_category["id"])
    # Roles
    for role in cls._master["roles"]["delete_me"]:
        delete_stream("roles", role["id"])


def update_streams(cls, expected=None):
    """A common function to update streams records.

    Args:
        expected (dict, optional): Dictionary containaing streams name with list to append updated records. Defaults to None.

    Returns:
        dict: Returns 'expected' dictionary.
    """
    if not expected:
        expected = {stream: [] for stream in cls.expected_streams()}

    LOGGER.info("Updating clients")
    client_id = cls._master["clients"]["delete_me"][0]["id"]
    updated_client = update_client(client_id)
    expected["clients"].append({"id": client_id})
    LOGGER.info(updated_client["updated_at"])

    LOGGER.info("Updating contacts")
    contact_id = cls._master["contacts"]["delete_me"][0]["id"]
    updated_contact = update_contact(contact_id)
    expected["contacts"].append({"id": contact_id})
    LOGGER.info(updated_contact["updated_at"])

    LOGGER.info("Updating estimates")
    estimate_id = cls._master["estimates"]["delete_me"][-1]["id"]
    updated_estimate = update_estimate(estimate_id)
    expected["estimates"].append({"id": estimate_id})
    LOGGER.info(updated_estimate["updated_at"])

    LOGGER.info("'Updating' estimate_messages")
    updated_estimate_message = update_estimate_message(estimate_id)
    expected["estimate_messages"].append({"id": updated_estimate_message["id"]})
    LOGGER.info(updated_estimate_message["updated_at"])

    LOGGER.info("Updating estimate_line_items")
    expected["estimate_line_items"].append(
        {"id": updated_estimate["line_items"][0]["id"]}
    )

    LOGGER.info("Updating estimate_item_categories")
    category_id = cls._master["estimate_item_categories"]["delete_me"][0]["id"]
    updated_category = update_estimate_item_category(category_id)
    expected["estimate_item_categories"].append({"id": category_id})
    LOGGER.info(updated_category["updated_at"])

    LOGGER.info("Updating invoices")
    invoice_id = cls._master["invoices"]["delete_me"][0]["id"]
    updated_invoice = update_invoice(invoice_id)
    expected["invoices"].append({"id": invoice_id})
    LOGGER.info(updated_invoice["updated_at"])

    LOGGER.info("Updating invoice_payments")
    updated_payment = update_invoice_payment(invoice_id)
    expected["invoice_payments"].append({"id": updated_payment["id"]})
    LOGGER.info(updated_payment["updated_at"])

    LOGGER.info("Updating invoice_messages")
    updated_message = update_invoice_message(invoice_id)
    expected["invoice_messages"].append({"id": updated_message["id"]})

    LOGGER.info("Updating invoice_line_items")
    expected["invoice_line_items"].append(
        {"id": updated_invoice["line_items"][0]["id"]}
    )
    LOGGER.info(updated_message["updated_at"])

    LOGGER.info("Updating invoice_item_categories")
    category_id = cls._master["invoice_item_categories"]["delete_me"][0]["id"]
    updated_category = update_invoice_item_category(category_id)
    expected["invoice_item_categories"].append({"id": category_id})
    LOGGER.info(updated_category["updated_at"])

    LOGGER.info("Updating roles")
    role_id = cls._master["roles"]["delete_me"][0]["id"]
    updated_role = update_role(role_id)
    expected["roles"].append({"id": role_id})
    LOGGER.info(updated_role["updated_at"])

    LOGGER.info("Updating user_roles")
    user_id = get_random("users")
    update_user_role = update_role(role_id, [])
    updated_user_role = update_role(role_id, [user_id])
    expected["user_roles"].append({"user_id": user_id, "role_id": role_id})
    LOGGER.info(updated_user_role["updated_at"])

    LOGGER.info("Updating projects")
    project_id = cls._master["projects"]["delete_me"][0]["id"]
    updated_project = update_project(project_id)
    expected["projects"].append({"id": project_id})
    LOGGER.info(updated_project["updated_at"])

    # # TODO - Why is this the same as user_roles update ^
    # # LOGGER.info("Updating user_project_tasks")
    # user_id = get_random('users')
    # update_user_role = update_role(role_id, [])
    # updated_user_role = update_role(role_id, [user_id])
    # expected['user_roles'].append({"roles": user_id, "role_id": role_id})
    # LOGGER.info(updated_user_role['updated_at'])

    LOGGER.info("Updating expenses")
    expense_id = cls._master["expenses"]["delete_me"][0]["id"]
    updated_expense = update_expense(expense_id)
    expected["expenses"].append({"id": expense_id})
    LOGGER.info(updated_expense["updated_at"])

    LOGGER.info("Updating expense_categories")
    category_id = cls._master["expense_categories"]["delete_me"][0]["id"]
    updated_category = update_expense_category(category_id)
    expected["expense_categories"].append({"id": category_id})
    LOGGER.info(updated_category["updated_at"])

    LOGGER.info("Updating tasks")
    task_id = cls._master["tasks"]["delete_me"][0]["id"]
    updated_task = update_task(task_id)
    expected["tasks"].append({"id": task_id})
    LOGGER.info(updated_task["updated_at"])

    # This is commented because we are not testing this stream and it affects time_entries and it's child streams
    LOGGER.info("Updating project_users")
    project_id = cls._master["projects"]["delete_me"][0]["id"]
    project_user_id = cls._master["project_users"]["delete_me"][0]["id"]
    updated_project_user = update_project_user(project_id, project_user_id)
    expected["project_users"].append({"id": updated_project_user["id"]})
    LOGGER.info(updated_project_user["updated_at"])

    LOGGER.info("Updating project_tasks (task_assignments)")
    project_id = cls._master["projects"]["delete_me"][0]["id"]
    project_task_id = cls._master["project_tasks"]["delete_me"][0]["id"]
    updated_project_task = update_project_task(project_id, project_task_id)
    expected["project_tasks"].append({"id": updated_project_task["id"]})
    expected["user_project_tasks"].append(
        {"project_task_id": updated_project_task["id"], "user_id": get_random("users")}
    )
    LOGGER.info(updated_project_task["updated_at"])

    LOGGER.info("Updating time_entries")
    time_entry_id = cls._master["time_entries"]["delete_me"][0]["id"]
    updated_time_entry = update_time_entry(time_entry_id)
    expected["time_entries"].append({"id": time_entry_id})
    LOGGER.info(updated_time_entry["updated_at"])

    LOGGER.info("Updating external_reference (time_entries)")
    external_reference_id = updated_time_entry["external_reference"]["id"]
    expected["external_reference"].append({"id": external_reference_id})

    LOGGER.info("Updating time_entry_external_reference (time_entries)")
    expected["time_entry_external_reference"].append(
        {"time_entry_id": time_entry_id, "external_reference_id": external_reference_id}
    )

    LOGGER.info(f"Updated Expectations : \n{expected}")

    return expected


def insert_one_record(cls, expected):
    """Function to insert one record to all streams."""

    LOGGER.info("Inserting roles")
    inserted_role = create_role()
    expected["roles"].append({"id": inserted_role["id"]})
    cls._master["roles"]["delete_me"].append({"id": inserted_role["id"]})

    LOGGER.info("'Inserting' (update_role) user_roles")
    user_id = get_random("users")
    inserted_user_role = update_role(inserted_role["id"], [user_id])
    expected["user_roles"].append({"user_id": user_id, "role_id": inserted_role["id"]})

    LOGGER.info("Inserting clients")
    inserted_client = create_client()
    expected["clients"].append({"id": inserted_client["id"]})
    cls._master["clients"]["delete_me"].append({"id": inserted_client["id"]})

    LOGGER.info("Inserting contacts")
    inserted_contact = create_contact(inserted_client["id"])
    expected["contacts"].append({"id": inserted_contact["id"]})
    cls._master["contacts"]["delete_me"].append({"id": inserted_contact["id"]})

    LOGGER.info("Inserting estimates")
    inserted_estimate = create_estimate(inserted_client["id"])
    expected["estimates"].append({"id": inserted_estimate["id"]})
    cls._master["estimates"]["delete_me"].append({"id": inserted_estimate["id"]})

    LOGGER.info("Inserting estimate_item_categories")
    inserted_category = create_estimate_item_category()
    expected["estimate_item_categories"].append({"id": inserted_category["id"]})
    cls._master["estimate_item_categories"]["delete_me"].append(
        {"id": inserted_category["id"]}
    )

    LOGGER.info("Inserting estimate_line_items")
    updated_estimate_line_item = inserted_estimate["line_items"][0]
    expected["estimate_line_items"].append({"id": updated_estimate_line_item["id"]})

    LOGGER.info("Inserting estimate_messages")
    inserted_estimate_message = create_estimate_message(inserted_estimate["id"])
    expected["estimate_messages"].append({"id": inserted_estimate_message["id"]})

    LOGGER.info("Inserting invoices")
    client_id = cls._master["clients"]["delete_me"][0]["id"]
    inserted_invoice = create_invoice(client_id=client_id)
    expected["invoices"].append({"id": inserted_invoice["id"]})
    cls._master["invoices"]["delete_me"].append({"id": inserted_invoice["id"]})

    LOGGER.info("Inserting invoice_payments")
    inserted_payment = create_invoice_payment(inserted_invoice["id"])
    expected["invoice_payments"].append({"id": inserted_payment["id"]})

    LOGGER.info("Inserting invoice_line_items")
    updated_invoice_line_item = inserted_invoice["line_items"][0]
    expected["invoice_line_items"].append({"id": updated_invoice_line_item["id"]})

    LOGGER.info("Inserting invoice_messages")
    inserted_message = create_invoice_message(inserted_invoice["id"])
    expected["invoice_messages"].append({"id": inserted_message["id"]})
    cls._master["invoice_messages"]["delete_me"].append({"id": inserted_message["id"]})

    LOGGER.info("Inserting invoice_item_categories")
    inserted_category = create_invoice_item_category()
    expected["invoice_item_categories"].append({"id": inserted_category["id"]})
    cls._master["invoice_item_categories"]["delete_me"].append(
        {"id": inserted_category["id"]}
    )

    LOGGER.info("Inserting expenses")
    project_id = cls._master["projects"]["delete_me"][0]["id"]
    inserted_expense = create_expense(project_id)
    expected["expenses"].append({"id": inserted_expense["id"]})
    cls._master["expenses"]["delete_me"].append({"id": inserted_expense["id"]})

    LOGGER.info("Inserting expense_categories")
    inserted_category = create_expense_category()
    expected["expense_categories"].append({"id": inserted_category["id"]})
    cls._master["expense_categories"]["delete_me"].append(
        {"id": inserted_category["id"]}
    )

    LOGGER.info("Inserting tasks")
    inserted_task = create_task()
    expected["tasks"].append({"id": inserted_task["id"]})
    cls._master["tasks"]["delete_me"].append({"id": inserted_task["id"]})

    LOGGER.info("Inserting project_users)")
    inserted_project_user = create_project_user(project_id, get_random("users"))
    expected["project_users"].append({"id": inserted_project_user["id"]})

    LOGGER.info("Inserting project_tasks (task_assingments)")
    inserted_project_task = create_project_task(project_id, inserted_task["id"])
    expected["project_tasks"].append({"id": inserted_project_task["id"]})

    expected["user_project_tasks"].append(
        {"project_task_id": inserted_project_task["id"], "user_id": get_random("users")}
    )

    LOGGER.info("Inserting time_entries")
    task_id = inserted_task["id"]
    inserted_time_entry = create_time_entry(project_id, task_id)
    expected["time_entries"].append({"id": inserted_time_entry["id"]})
    cls._master["time_entries"]["delete_me"].append({"id": inserted_time_entry["id"]})

    LOGGER.info("Inserting external_reference (time_entries)")
    inserted_external_reference = inserted_time_entry["external_reference"]
    expected["external_reference"].append({"id": inserted_external_reference["id"]})

    LOGGER.info("Inserting time_entry_external_reference (time_entries)")
    expected["time_entry_external_reference"].append(
        {
            "time_entry_id": inserted_time_entry["id"],
            "external_reference_id": inserted_external_reference["id"],
        }
    )

    return expected
