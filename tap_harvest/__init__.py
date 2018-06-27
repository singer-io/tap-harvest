#!/usr/bin/env python3

import datetime
import os

import backoff
import requests
import pendulum

import singer
from singer import Transformer, utils

LOGGER = singer.get_logger()
SESSION = requests.Session()
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "refresh_token",
    "client_id",
    "client_secret",
    "user_agent",
    "account_id",
]

BASE_URL = "https://api.harvestapp.com/v2/"
CONFIG = {}
STATE = {}
AUTH = {}

class Auth:
    def __init__(self, client_id, client_secret, refresh_token):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._refresh_access_token()

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException),
        max_tries=5,
        giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
        factor=2)
    def _make_refresh_token_request(self):
        return requests.request('POST',
                                url='https://id.getharvest.com/api/v2/oauth2/token',
                                data={
                                    'client_id': self._client_id,
                                    'client_secret': self._client_secret,
                                    'refresh_token': self._refresh_token,
                                    'grant_type': 'refresh_token',
                                },
                                headers={"User-Agent": CONFIG.get("user_agent")})

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
        if (self._access_token is not None and self._expires_at > pendulum.now()):
            return self._access_token

        self._refresh_access_token()
        return self._access_token


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


def load_and_write_schema(name, bookmark_property='updated_at'):
    schema = load_schema(name)
    singer.write_schema(name,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])
    return schema

def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint):
    return BASE_URL + endpoint

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException),
    max_tries=5,
    giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
    factor=2)
@utils.ratelimit(100, 15)
def request(url, params=None):
    params = params or {}
    access_token = AUTH.get_access_token()
    headers = {"Accept": "application/json",
               "Harvest-Account-Id": CONFIG.get("account_id"),
               "Authorization": "Bearer " + access_token,
               "User-Agent": CONFIG.get("user_agent")}
    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()
    return resp.json()

def append_times_to_dates(item, date_fields):
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] += "T00:00:00Z"

def sync_endpoint(schema_name, endpoint=None, path=None, date_fields=None, with_updated_since=True, for_each_handler=None, map_handler=None, object_to_id=[]):
    schema = load_schema(schema_name)
    bookmark_property = 'updated_at'

    singer.write_schema(schema_name,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start(schema_name)
    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = get_url(endpoint or schema_name)
    params = {"updated_since": updated_since} if with_updated_since else {}
    data = request(url, params)
    path = path or schema_name
    data = data[path]
    time_extracted = utils.now()

    with Transformer() as transformer:
        for row in data:
            if map_handler is not None:
                row = map_handler(row)

            for key in object_to_id:
                if row[key] is not None:
                    row[key + '_id'] = row[key]['id']

            item = transformer.transform(row, schema)

            append_times_to_dates(item, date_fields)

            if item[bookmark_property] >= start:
                singer.write_record(schema_name,
                                    item,
                                    time_extracted=time_extracted)

                # take any additional actions required for the currently loaded endpoint
                if for_each_handler is not None:
                    for_each_handler(row, time_extracted=time_extracted)

                utils.update_state(STATE, schema_name, item[bookmark_property])

    singer.write_state(STATE)


def sync_time_entries():
    def for_each_time_entry(time_entry, time_extracted):
        # Extract external_reference
        external_reference_schema = load_and_write_schema("external_reference")
        time_entry_external_reference_schema = load_and_write_schema("time_entry_external_reference")
        if time_entry['external_reference'] is not None:
            with Transformer() as transformer:
                external_reference = time_entry['external_reference']
                external_reference = transformer.transform(external_reference, external_reference_schema)

                singer.write_record("external_reference",
                                    external_reference,
                                    time_extracted=time_extracted)

                # Create pivot row for time_entry and external_reference
                pivot_row = {
                    'time_entry_id': time_entry['id'],
                    'external_reference': external_reference['id']
                }

                singer.write_record("time_entry_external_reference",
                                    pivot_row,
                                    time_extracted=time_extracted)

    sync_endpoint("time_entries", for_each_handler=for_each_time_entry,
                  object_to_id=['user', 'user_assignment', 'client', 'project', 'task', 'task_assignment', 'external_reference', 'invoice'])

def sync_invoices():
    def for_each_invoice_message(message, time_extracted):
        # Extract all invoice_recipients
        recipients_schema = load_and_write_schema("invoice_recipients")
        with Transformer() as transformer:
            for recipient in message['recipients']:
                recipient['invoice_message_id'] = message['id']
                recipient = transformer.transform(recipient, recipients_schema)

                singer.write_record("invoice_recipients",
                                    recipient,
                                    time_extracted=time_extracted)

    def for_each_invoice(invoice, time_extracted):
        def map_invoice_message(message):
            message['invoice_id'] = invoice['id']
            return message

        def map_invoice_payment(payment):
            payment['invoice_id'] = invoice['id']
            payment['payment_gateway_id'] = payment['payment_gateway']['id']
            payment['payment_gateway_name'] = payment['payment_gateway']['name']
            return payment

        # Sync invoice messages
        sync_endpoint("invoice_messages",
                      endpoint=("invoices/{}/messages".format(invoice['id'])),
                      path="invoice_messages",
                      with_updated_since=False,
                      map_handler=map_invoice_message,
                      for_each_handler=for_each_invoice_message,
                      date_fields=["send_reminder_on"],
                      )

        # Sync invoice payments
        sync_endpoint("invoice_payments",
                      endpoint=("invoices/{}/payments".format(invoice['id'])),
                      path="invoice_payments",
                      with_updated_since=False,
                      map_handler=map_invoice_payment,
                      date_fields=["send_reminder_on"],
                      )

        # Extract all invoice_line_items
        line_items_schema = load_and_write_schema("invoice_line_items")
        with Transformer() as transformer:
            for line_item in invoice['line_items']:
                line_item['invoice_id'] = invoice['id']
                line_item = transformer.transform(line_item, line_items_schema)

                singer.write_record("estimate_line_items",
                                    line_item,
                                    time_extracted=time_extracted)

    sync_endpoint("invoices", for_each_handler=for_each_invoice, date_fields=[
                "period_start",
                "period_end",
                "issued_date",
                "due_date",
                "paid_date",
            ], object_to_id=['client', 'estimate', 'retainer', 'creator'])

def sync_estimates():
    def for_each_estimate_message(message, time_extracted):
        # Extract all estimate_recipients
        recipients_schema = load_and_write_schema("estimate_recipients")

        with Transformer() as transformer:
            for recipient in message['recipients']:
                recipient['estimate_message_id'] = message['id']
                recipient = transformer.transform(recipient, recipients_schema)

                singer.write_record("estimate_recipients",
                                    recipient,
                                    time_extracted=time_extracted)

    def map_estimate_message(message):
        message['estimate_id'] = message['id']
        return message

    def for_each_estimate(estimate, time_extracted):
        # Sync estimate messages
        sync_endpoint("estimate_messages",
                      endpoint=("estimates/{}/messages".format(estimate['id'])),
                      path="estimate_messages",
                      with_updated_since=False,
                      for_each_handler=for_each_estimate_message,
                      date_fields=["send_reminder_on"],
                      map_handler=map_estimate_message,
                      )

        # Extract all estimate_line_items
        line_items_schema = load_and_write_schema("estimate_line_items")
        with Transformer() as transformer:
            for line_item in estimate['line_items']:
                line_item['estimate_id'] = estimate['id']
                line_item = transformer.transform(line_item, line_items_schema)

                singer.write_record("estimate_line_items",
                                    line_item,
                                    time_extracted=time_extracted)

    sync_endpoint("estimates", for_each_handler=for_each_estimate, date_fields=["issued_date"], object_to_id=['client', 'user'])

def sync_roles():
    def for_each_role(role, time_extracted):
        # Extract external_reference
        load_and_write_schema("user_roles")
        for user_id in role['user_ids']:
            pivot_row = {
                'role_id': role['id'],
                'user_id': user_id
            }

            singer.write_record("user_roles",
                                pivot_row,
                                time_extracted=time_extracted)

    sync_endpoint("roles", for_each_handler=for_each_role)

def sync_users():
    def for_each_user(user, time_extracted):
        def map_user_projects(project_assignment):
            project_assignment['user'] = user
            return project_assignment

        sync_endpoint("user_projects",
                      endpoint=("users/{}/project_assignments".format(user['id'])),
                      path="project_assignments",
                      with_updated_since=False,
                      object_to_id=['project', 'client', 'user'],
                      map_handler=map_user_projects
                      )

    sync_endpoint("users", for_each_handler=for_each_user)


def do_sync():
    LOGGER.info("Starting sync")

    # Grab all clients and client contacts. Contacts have client FKs so grab
    # them last.
    sync_endpoint("clients")
    sync_endpoint("contacts", object_to_id=['client'])
    sync_roles()

    # Get all people and tasks before grabbing the projects. When we grab the
    # projects we will grab the project_users, project_tasks, and time_entries
    # for each.
    sync_users()
    sync_endpoint("tasks")
    sync_endpoint("projects", object_to_id=['client'])

    # Sync related project objects
    sync_endpoint("project_tasks", endpoint='task_assignments', path='task_assignments', object_to_id=['project', 'task'])
    sync_endpoint("project_users", endpoint='user_assignments', path='user_assignments', object_to_id=['project', 'user'])

    # Sync expenses and their categories
    sync_endpoint("expense_categories")
    sync_endpoint("expenses", date_fields=["spent_at"], object_to_id=['client', 'project', 'expense_category', 'user', 'user_assignment', 'invoice'])

    # Sync invoices and all related records
    sync_endpoint("invoice_item_categories")
    sync_invoices()

    # Sync estimates and all related records
    sync_endpoint("estimate_item_categories")
    sync_estimates()

    # Sync Time Entries along with their external reference objects
    sync_time_entries()

    LOGGER.info("Sync complete")


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(args.config)
    global AUTH # pylint: disable=global-statement
    AUTH = Auth(CONFIG['client_id'], CONFIG['client_secret'], CONFIG['refresh_token'])
    STATE.update(args.state)
    do_sync()

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
