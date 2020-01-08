"""
Harvest API Response, Access Token and Refresh Token
"""

import os
import random
import requests
import time
import logging
import datetime

from datetime import datetime as dt
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse

from spec import TapSpec
from base import BaseTapTest


_ACCESS_TOKEN = None
_ACCOUNT_ID = None
UPDATED_SINCE = datetime.datetime.strptime(TapSpec.DEFAULT_START_DATE,
                                           "%Y-%m-%d %H:%M:%S").isoformat() + "Z"

##############################
#  Access Token Methods      #
##############################


def _make_refresh_token_request():
    return requests.request('POST',
                            url="https://id.getharvest.com/api/v2/oauth2/token",
                            json={
                                'client_id': BaseTapTest.get_credentials(None)["client_id"],
                                'client_secret': BaseTapTest.get_credentials(None)["client_secret"],
                                'refresh_token': BaseTapTest.get_credentials(None)["refresh_token"],
                                'grant_type': 'refresh_token',
                            },
                            headers={"User-Agent": "dev@talend.com","Content-Type": "application/json"})


def _refresh_access_token():
    print("Refreshing access token")
    resp = _make_refresh_token_request()
    resp_json = {}
    try:
        resp_json = resp.json()
        global _ACCESS_TOKEN
        _ACCESS_TOKEN = resp_json['access_token']
    except KeyError as key_err:
        if resp_json.get('error'):
            logging.critical(resp_json.get('error'))
        if resp_json.get('error_description'):
            logging.critical(resp_json.get('error_description'))
        raise key_err
    print("Got refreshed access token")


def get_access_token():
    global _ACCESS_TOKEN
    if _ACCESS_TOKEN is not None:
        return _ACCESS_TOKEN
    _refresh_access_token()
    print(_ACCESS_TOKEN)
    return _ACCESS_TOKEN


def get_account_id():
    global _ACCOUNT_ID
    global _ACCESS_TOKEN
    if _ACCOUNT_ID is not None:
        return _ACCOUNT_ID
    response = requests.request('GET',
                                url="https://id.getharvest.com/api/v2/accounts",
                                data={
                                    'access_token': _ACCESS_TOKEN,
                                },
                                headers={"User-Agent": "dev@talend.com"})
    _ACCOUNT_ID = str(response.json()['accounts'][0]['id'])
    return _ACCOUNT_ID


HEADERS = {
    "Authorization":"Bearer {}".format(get_access_token()),
    "Harvest-Account-Id": get_account_id(),
    "User-Agent": "dev@talend.com",
    "Content-Type": "application/json"}


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
#         logging.warn("A user with the email {} already exists. Removing user...")
#         response = requests.get(url="https://api.harvestapp.com/v2/users", headers=HEADERS)
#         all_users = response.json()['users']
#         for user in all_users:
#             if user['email'] == email:
#                 response = delete_user(user['id'])
#         response = requests.post(url="https://api.harvestapp.com/v2/users", headers=HEADERS, json=data)
#         if response.status_code >= 400:
#             logging.warn("Unable to delete this user")
#             logging.warn("create_user: {} {}".format(response.status_code, response.text))
#             assert None
#     return response.json()


def create_client():
    """required | name"""
    data = {"name":"New {} Client {}".format(random.randint(0,1000000), random.randint(0,1000000)),"currency":"EUR"}
    response = requests.post(url="https://api.harvestapp.com/v2/clients", headers=HEADERS, json=data)
    if response.status_code >= 400:
        if data['name'] in [client['name'] for client in  get_all('clients')]:
            data['name'] = "Retry Client"
            response = requests.post(url="https://api.harvestapp.com/v2/clients", headers=HEADERS, json=data)
            if response.status_code >= 400:
                logging.warn("create_client: {} {}".format(response.status_code, response.text))
                assert None
            return response.json()
        logging.warn("create_client: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_contact(client_id):
    """required | first_name, client_id"""
    data= {"client_id":client_id,"first_name":"George","last_name":"Frank",
           "email":"george{}{}@example.com".format(random.randint(0,1000000), random.randint(0,1000000))}
    response = requests.post(url="https://api.harvestapp.com/v2/contacts", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_contact: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_estimate(client_id):
    """used for estimate_line_items as well"""    
    line_items = [{"kind":"Service","description":"estimate description {}".format(random.randint(0,1000000)),"unit_price":random.randint(1,1000000)}]
    data = {"client_id":client_id,"subject":"ABC{} Project Quote".format(random.randint(0,100)),"line_items":line_items}
    response = requests.post(url="https://api.harvestapp.com/v2/estimates", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_estimate: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_estimate_item_category():
    data = {"name": "Random {} Category {}".format(random.randint(0,1000000), random.randint(0,1000000))}
    response = requests.post(url="https://api.harvestapp.com/v2/estimate_item_categories", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_estimate_item_category: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_estimate_message(estimate_id):
    """required | recipients"""
    rand = random.randint(0,1000000)
    data = {"subject":"Estimate #{}_{}".format(estimate_id, random.randint(0,1000000)),
            "body":"Here is our estimate.","send_me_a_copy":False,
            "recipients":[{"name":"Rando {}".format(rand), "email":"rando{}@example.com".format(rand)}]}
    response = requests.post(url="https://api.harvestapp.com/v2/estimates/{}/messages".format(estimate_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_estimate_message: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_expense(project_id):
    """required | project_id, expense_category_id, spent_date"""
    rand_month = random.randint(1,5)
    spent_date = date.today() - relativedelta(months=rand_month)
    data = {"project_id":project_id,"expense_category_id":get_random('expense_categories'),
            "spent_date":str(spent_date),"total_cost":random.randint(1,10000000)}
    receipt_file = None
    response = None
    # NOTE we cannot attach files on the free Harvest plan
    # with open(os.getcwd() +'/harvest_test_receipt.gif') as receipt:
    #     receipt_file = {"harvest_test_receipt.gif": receipt}
    #     response = requests.post(url="https://api.harvestapp.com/v2/expenses", headers=HEADERS, json=data, files=receipt_file) 
    #     if response.status_code >= 400:
    #         logging.warn("create_expense: {} {}".format(response.status_code, response.text))
    #         assert None
    response = requests.post(url="https://api.harvestapp.com/v2/expenses", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_expense: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_expense_category():
    """required | name"""
    data = {"name": "Expense {} category {}".format(random.randint(0,1000000), random.randint(0,1000000))}
    response = requests.post(url="https://api.harvestapp.com/v2/expense_categories", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_expense_category: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_invoice(client_id, estimate_id: str = "", project_id: str = ""):
    if estimate_id == "":
        estimate_id = get_random('estimates')
    if project_id == "":
        project_id = get_random('projects')
    rand_month = random.randint(1,12) 
    due_date = date.today() + relativedelta(months=rand_month)
    line_items = [{"kind":"Service","description":"ABC{} Project".format(random.randint(0,1000000)),
                   "unit_price":random.randint(1,1000000),"project_id": project_id}]
    data = {"client_id":client_id,"subject":"ABC Project Quote","due_date":"{}".format(due_date),"line_items":line_items,
            "estimate_id":estimate_id}
    response = requests.post(url="https://api.harvestapp.com/v2/invoices", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_invoice: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_invoice_message(invoice_id):
    data = {"subject":"Invoice #{}".format(invoice_id),"body":"The invoice is attached below.","attach_pdf":True,
            "send_me_a_copy":False,"recipients":[{"name":"Richard Roe","email":"richardroe@example.com"}]}
    response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_invoice_message: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_invoice_payment(invoice_id):
    """amount | required"""
    invoice = requests.get(url="https://api.harvestapp.com/v2/invoices/{}".format(invoice_id), headers=HEADERS)
    amount = invoice.json()['amount']
    rand_month = random.randint(1,12) 
    paid_date = date.today() - relativedelta(months=rand_month)
    data = {"amount":random.randint(1,amount-1),"paid_at":str(paid_date),"notes":"Paid by phone"}
    response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/payments".format(invoice_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_invoice_payment: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_invoice_item_category():
    """amount | requireed"""
    rand_1 = random.randint(0,1000000)
    rand_2 = random.randint(0,1000000)
    data = {"name": "Category {} {}".format(rand_1, rand_2)}
    response = requests.post(url="https://api.harvestapp.com/v2/invoice_item_categories", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_invoice_item_category: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_project(client_id):
    data = {"client_id":client_id,"name":"Created {} Project {}".format(random.randint(0, 1000000),random.randint(0, 1000000)),"is_billable":True,"bill_by":"Project",
            "hourly_rate":100.0,"budget_by":"project","budget":10000}
    response = requests.post(url="https://api.harvestapp.com/v2/projects", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_project: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_project_task(project_id, task_id):
    """a.k.a 'task_assignment"""
    data = {"task_id": task_id, "hourly_rate":random.randint(0,100)}
    response = requests.post(url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(project_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_task_assignment: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_project_user(project_id, user_id):
    data = {"user_id":user_id,"use_default_rates":False,"hourly_rate":42.0, "budget":random.randint(100,1000000)} 
    response = requests.post(url="https://api.harvestapp.com/v2/projects/{}/user_assignments".format(project_id),
                             headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('create_project_user: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def create_role():
    """required | name"""
    data = {"name":"Manger #{}-{}".format(random.randint(0,1000000),random.randint(0,1000000)),
            "user_ids":[get_random('users')]}
    response = requests.post(url="https://api.harvestapp.com/v2/roles", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_roles: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_task():
    rand_1 = random.randint(0,1000000)
    rand_2 = random.randint(0,1000000)
    rand_3 = random.randint(0,1000000)
    data = {"name":"{} Task {}  Name {}".format(rand_1, rand_2, rand_3),"hourly_rate":42.0}
    response = requests.post(url="https://api.harvestapp.com/v2/tasks", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_task: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def create_time_entry(project_id, task_id):
    """required | project_id, task_id, spend_date"""
    rand_month = random.randint(1,12)
    spent_date = date.today() - relativedelta(months=rand_month)
    data = {"project_id":project_id,"task_id":task_id,"spent_date":str(spent_date),"hours":1.0,
            "external_reference":{"id":random.randint(5, 1000000),"group_id":random.randint(1,1000000),
                                  "permalink": 'https://help.getharvest.com/api-v2/'}}
    response = requests.post(url="https://api.harvestapp.com/v2/time_entries", headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('create_time_entry: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


####################
# Update Methods   #
####################
# return value: response from a patch

def update_client(client_id):
    data = {"name":"client test name #{}".format(random.randint(0,10000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/clients/{}".format(client_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_client: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_contact(contact_id):
    data = {"title": "Title{}".format(random.randint(0,1000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/contacts/{}".format(contact_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_contact: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_estimate(estimate_id):
    """used for estimate_line_items as well"""
    data = {"subject": "Subject {}".format(random.randint(0,1000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/estimates/{}".format(estimate_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_estimate: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


# def update_estimate_line_items(estimate_id):
#     line_items = [{"kind":"Service","description":"updated estimate description {}".format(random.randint(0,1000000)),
#                    "unit_price":42.0}]
#     data = {"line_items":line_items}
#     response = requests.patch(url="https://api.harvestapp.com/v2/estimates/{}".format(estimate_id), headers=HEADERS, json=data)
#     if response.status_code >= 400:
#         logging.warn('update_estimate_line_items: {} {}'.format(response.status_code, response.text))
#         assert None
#     return response.json()


def update_estimate_message(estimate_id):
    #mark = ["accept", "decline", "sent", "re-open", "view", "invoice"]
    data = {"event_type": "accept"}
    response = requests.post(url="https://api.harvestapp.com/v2/estimates/{}/messages".format(estimate_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_estimate_message: {} {}'.format(response.status_code, response.text))
        data = {"event_type": "view"}
        response = requests.post(url="https://api.harvestapp.com/v2/estimates/{}/messages".format(estimate_id), headers=HEADERS, json=data)
        if response.status_code >= 400:
            logging.warn('update_estimate_message: {} {}'.format(response.status_code, response.text))
            assert None
    return response.json()


def update_estimate_item_category(category_id):
    data = {"name": "Updated Category {}".format(random.randint(0, 1000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/estimate_item_categories/{}".format(category_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_estimate_item_category: {} {}'.format(response.status_code, response.text))
        if response.status_code == 422: # Name is in use
            data = {"name": "Different Updated Category {}".format(0, 1000000)}
            response = requests.patch(url="https://api.harvestapp.com/v2/estimate_item_categories/{}".format(category_id),
                                      headers=HEADERS, json=data)
            return response.json()
        assert None
    return response.json()


def update_expense(expense_id):
    spent_date = date.today() - relativedelta(months=random.randint(7,12))
    data = {"spent_date":str(spent_date)}
    response = requests.patch(url="https://api.harvestapp.com/v2/expenses/{}".format(expense_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_expense: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_expense_category(category_id):
    data = {"name":"Updated Category {}".format(random.randint(0,1000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/expense_categories/{}".format(category_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_expense_category: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_invoice(invoice_id):
    data = {"purchase_order":"{}".format(random.randint(1000,10000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/invoices/{}".format(invoice_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_invoice: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_invoice_message(invoice_id):
    #mark = ["close", "send", "re-open", "view", "draft"]
    data = {"event_type": "close"}
    response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_invoice_message: {} {}'.format(response.status_code, response.text))
        data = {"event_type": "re-open"}
        response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS, json=data)
        if response.status_code >= 400:
            logging.warn('update_invoice_message: {} {}'.format(response.status_code, response.text))
            data = {"event_type": "draft"}
            response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS, json=data)
            if response.status_code >= 400:
                logging.warn('update_invoice_message: {} {}'.format(response.status_code, response.text))
                data = {"event_type": "send"}
                response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS, json=data)
                if response.status_code >= 400:
                    logging.warn('update_invoice_message: {} {}'.format(response.status_code, response.text))
                    data = {"event_type": "open"}
                    response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS, json=data)
                    if response.status_code >= 400:
                        logging.warn('update_invoice_message: {} {}'.format(response.status_code, response.text))
                        assert None
    return response.json()


def update_invoice_payment(invoice_id):
    rand_month = random.randint(1,12) 
    paid_date = date.today() - relativedelta(months=rand_month)
    data = {"amount":random.randint(15000, 75000)/100,"paid_at":str(paid_date),"notes":"Paid by phone"}
    response = requests.post(url="https://api.harvestapp.com/v2/invoices/{}/payments".format(invoice_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn("create_invoice_payment: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()


def update_invoice_item_category(category_id):
    data = {"name": "Updated Category {}".format(random.randint(0,1000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/invoice_item_categories/{}".format(category_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_invoice_item_category: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()

    
def update_project(project_id):
    data = {"hourly_rate": str(random.randint(20,90))}
    response = requests.patch(url="https://api.harvestapp.com/v2/projects/{}".format(project_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        assert None
    return response.json()


def update_project_task(project_id, task_assignment_id):
    """a.k.a. task_assignment"""
    data = {'budget': random.randint(1000,1000000000)}
    response = requests.patch(url="https://api.harvestapp.com/v2/projects/{}/" \
                                  "task_assignments/{}".format(project_id, task_assignment_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('ERROR in Updating task {}'.format(task_id))
        assert None
    return response.json()


def update_project_user(project_id, project_user_id):
    data = {"budget": random.randint(15, 80)}
    response = requests.patch(url="https://api.harvestapp.com/v2/projects/{}/user_assignments/{}".format(
        project_id, project_user_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_project_user: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_role(role_id, user_ids = None):
    data = {"name": "Update Mangager #{}".format(random.randint(0,1000000))}
    if user_ids is not None:
        data["user_ids"] = user_ids
    response = requests.patch(url="https://api.harvestapp.com/v2/roles/{}".format(role_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_role: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


def update_task(task_id):
    data = {"name":"Updated Task Name {}".format(random.randint(0,1000000))}
    response = requests.patch(url="https://api.harvestapp.com/v2/tasks/{}".format(task_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        data = {"name":"Updated Task {} Name {}".format(random.randint(0,1000000), random.randint(0,1000000))}
        response = requests.patch(url="https://api.harvestapp.com/v2/tasks/{}".format(task_id), headers=HEADERS, json=data)
        if response.status_code >= 400:
            logging.warn('update_task: {} {}'.format(response.status_code, response.text))
            assert None
    return response.json()


def update_time_entry(time_entry_id):
    data = {"notes":"Updated Time Entry Note {}".format(random.randint(0,1000000)),
            "external_reference":{"id":random.randint(5, 1000000),"group_id":random.randint(100,500),
                                  "permalink":"https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/"}}
    response = requests.patch(url="https://api.harvestapp.com/v2/time_entries/{}".format(time_entry_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_time_entry: {} {}'.format(response.status_code, response.text))
        data = {"notes":"Updated Time Entry Note {}".format(random.randint(0,1000000)),
                "external_reference":{"id":random.randint(5, 1000000),"group_id":random.randint(100,500),
                                      "permalink":"https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/"}}
        response = requests.patch(url="https://api.harvestapp.com/v2/time_entries/{}".format(time_entry_id), headers=HEADERS, json=data)
        if response.status_code >= 400:
            logging.warn('update_time_entry: {} {}'.format(response.status_code, response.text))
            assert None
    return response.json()
    

def update_user(user_id, role_names = None):
    rand_1 = random.randint(0,1000000)
    rand_2 = random.randint(0,1000000)
    data = {"last_name":"{} {}".format(rand_1, rand_2)}
    if role_names is not None:
        data['roles'] = role_names
    response = requests.patch(url="https://api.harvestapp.com/v2/users/{}".format(user_id), headers=HEADERS, json=data)
    if response.status_code >= 400:
        logging.warn('update_user: {} {}'.format(response.status_code, response.text))
        assert None
    return response.json()


####################
# Get All          #
####################
# return value: list of current streams
def get_all(stream):
    response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_{}: {} {}".format(stream, response.status_code, response.text))
        assert None
    return response.json()[stream]
            
# def get_all(stream):
#     return_response =[]
#     response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS)
#     pages = response.json()["total_pages"]
#     for _ in pages:
#         if response.status_code >= 400:
#             logging.warn("get_all_{}: {} {}".format(stream, response.status_code, response.text))
#             assert None
#         return_response.append(response.json()[stream])
#         response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS)
            
# Complex streams require their own functions
def get_user_projects(user_id):
    response = requests.get(url="https://api.harvestapp.com/v2/users/{}/project_assignments".format(user_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_project_users: {} {}".format(stream, response.status_code, response.text))
        assert None
    return response.json()["project_assignments"]

def get_user_projects_all():
    response = requests.get(url="https://api.harvestapp.com/v2/users/me/project_assignments", headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_project_users: {} {}".format(stream, response.status_code, response.text))
        assert None
    return response.json()["project_assignments"]

def get_all_estimate_messages(estimate_id):
    response = requests.get(url="https://api.harvestapp.com/v2/estimates/{}/messages".format(estimate_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_estimate_messages: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()[stream]

def get_all_invoice_messages(invoice_id):
    response = requests.get(url="https://api.harvestapp.com/v2/invoices/{}/messages".format(invoice_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_invoice_messages: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()['invoice_messages']

def get_all_invoice_payments(invoice_id):
    response = requests.get(url="https://api.harvestapp.com/v2/invoices/{}/payments".format(invoice_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_invoice_payments: {} {}".format(response.status_code, response.text))
        assert None
    return response.json()['invoice_payments']

def get_all_project_tasks(project_id):
    response = requests.get(url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(project_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_all_project_tasks: {} {}".format(response.status_code, response.text))
        assert None
    num_project_tasks = len(response.json()['task_assignments']) -1
    return response.json()['task_assignments'][random.randint(0, num_project_tasks)]['id']


####################
# Get Counts       #
####################
# return value: tuple (number of pages, number of records)
def get_stream_counts(stream, parameters = [("updated_since", UPDATED_SINCE)]):
    if stream == "estimate_line_items":
        stream = "estimates" # All estimates that are created by us have a line_items field
    if stream == "invoice_line_items":
        stream = "invoices" # All invoices that are created by us have a line_items field
    if stream == "project_tasks":
        stream = "task_assignments"
    if stream == "time_entry_external_reference":
        stream = "time_entries" # All invoices that are created by us have a line_items field
    if stream == "external_reference":
        stream = "time_entries" # All invoices that are created by us have a line_items field
    response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS, params=parameters)
    if response.status_code >= 400:
        logging.warn("get_{}_count: {} {}".format(stream, response.status_code, response.text))
        assert None
    return response.json()["total_pages"], response.json()["total_entries"]


####################
# Get Random       #
####################
# Works for simple streams (simple = has one pk and does not rely on other streams for access)
# return value: ID value from the stream
def get_random(stream):
    """get random instance of stream | role, contact, estimate, invoice, client ..."""
    response = requests.get(url="https://api.harvestapp.com/v2/{}".format(stream), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_random_{}: {} {}".format(stream, response.status_code, response.text))
        assert None
    num_streams = len(response.json()[stream]) - 1
    return response.json()[stream][random.randint(0, num_streams)]['id']


# Complex streams require their own functions
def get_random_project_user(project_id):
    response = requests.get(url="https://api.harvestapp.com/v2/projects/{}/user_assignments".format(project_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_random_project_user: {} {}".format(response.status_code, response.text))
        assert None
    num_users = len(response.json()['user_assignments']) - 1
    return response.json()['user_assignments'][random.randint(0, num_users)]['id']


def get_random_project_task(project_id):
    response = requests.get(url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(project_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_random_project_taks: {} {}".format(response.status_code, response.text))
        assert None
    num_project_tasks = len(response.json()['task_assignments']) -1
    return response.json()['task_assignments'][random.randint(0, num_project_tasks)]['id']


def get_random_task(project_id: str = None):
    if project_id:
        response = requests.get(url="https://api.harvestapp.com/v2/projects/{}/task_assignments".format(project_id), headers=HEADERS)
        if response.status_code >= 400:
            logging.warn("get_random_task: {} {}".format(response.status_code, response.text))
            assert None
        num_project_tasks = len(response.json()['task_assignments']) -1
        return response.json()['task_assignments'][random.randint(0, num_project_tasks)]['task']['id']
    response = requests.get(url="https://api.harvestapp.com/v2/tasks", headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("get_random_task: {} {}".format(response.status_code, response.text))
        assert None
    num_tasks = len(response.json()['tasks']) - 1
    return response.json()['tasks'][random.randint(0, num_tasks)]['id']
    

####################
# Get Random       #
####################
# Use to alter our field expectations to mimic tap field naming based
# on things like client: {id, name} != client_id
def get_fields(stream):
    """
    Checks a stream's keys (using the json response from an api call) for 
    values that are dictionaries. This indicates it has subfields.
    returns the keys with necessary id-adjusted key names
    """
    keys = set(stream.keys())

    reformed = set()
    removed = set()

    # Find the fields which have subfields
    has_sub_fields = [key for key in keys if type(stream[key]) is dict]    

    # Some fields are actually child streams
    is_stream = ['line_items'] #, 'external_reference']
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
        elif 'id' in stream[field].keys():
            reformed.add(field + "_id")
            continue

        # Otherwise keep all subfields
        for key in stream[field].keys():
            reformed.add(field + "_" + key)

    # Some fields cannot be generated via api b/c of Harvest limitations, so
    # ensure fields which have subfields but have values are null are stil captured
    has_sub_fields = ['retainer']

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
    response = requests.delete(url="https://api.harvestapp.com/v2/{}/{}".format(stream, stream_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("delete_{}: {} {}".format(stream, response.status_code, response.text))
    return response.json()


# Complex streams require their own functions
def delete_project_user(project_id, project_user_id):
    response = requests.delete(url="https://api.harvestapp.com/v2/projects/{}/user_assignments{}".format(
        project_id, project_user_id), headers=HEADERS)
    if response.status_code >= 400:
        logging.warn("delete_project_user: {} {}".format(response.status_code, response.text))
    return response.json()
