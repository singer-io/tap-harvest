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

def sync_endpoint(schema_name, endpoint=None, path=None, date_fields=None):
    schema = load_schema(schema_name)
    bookmark_property = 'updated_at'

    singer.write_schema(schema_name,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start(schema_name)

    url = get_url(endpoint or schema_name)
    data = request(url)
    data = data[path or schema_name]
    time_extracted = utils.now()

    with Transformer() as transformer:
        for row in data:
            item = transformer.transform(row, schema)

            append_times_to_dates(item, date_fields)

            if item[bookmark_property] >= start:
                singer.write_record(schema_name,
                                    item,
                                    time_extracted=time_extracted)

                utils.update_state(STATE, schema_name, item[bookmark_property])

    singer.write_state(STATE)


def sync_time_entries():
    external_reference_schema = load_schema("external_reference")
    bookmark_property = 'updated_at'
    singer.write_schema("external_reference",
                        external_reference_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    time_entry_external_reference_schema = load_schema("time_entry_external_reference")
    singer.write_schema("time_entry_external_reference",
                        time_entry_external_reference_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    schema = load_schema("time_entries")
    singer.write_schema("time_entries",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("time_entries")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = get_url("time_entries")
    with Transformer() as transformer:
        data = request(url, {"updated_since": updated_since})['time_entries']
        time_entries_time_extracted = utils.now()

        for row in data:
            item = row
            item = transformer.transform(item, schema)
            append_times_to_dates(item, ["spent_date"])

            singer.write_record("time_entries",
                                item,
                                time_extracted=time_entries_time_extracted)

            utils.update_state(STATE, "time_entries", item['updated_at'])

            # Extract external_reference
            if row['external_reference'] is not None:
                external_reference = row['external_reference']
                external_reference = transformer.transform(external_reference, external_reference_schema)

                singer.write_record("external_reference",
                                    external_reference,
                                    time_extracted=time_entries_time_extracted)

                # Create pivot row for time_entry and external_reference
                pivot_row = {
                    'time_entry_id': row['id'],
                    'external_reference': external_reference['id']
                }

                singer.write_record("time_entry_external_reference",
                                    pivot_row,
                                    time_extracted=time_entries_time_extracted)

            singer.write_state(STATE)

        singer.write_state(STATE)

def sync_invoices():
    messages_schema = load_schema("invoice_messages")
    bookmark_property = 'updated_at'
    singer.write_schema("invoice_messages",
                        messages_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    payments_schema = load_schema("invoice_payments")
    singer.write_schema("invoice_payments",
                        payments_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    recipients_schema = load_schema("invoice_recipients")
    singer.write_schema("invoice_recipients",
                        recipients_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    line_items_schema = load_schema("invoice_line_items")
    singer.write_schema("invoice_line_items",
                        line_items_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    schema = load_schema("invoices")
    singer.write_schema("invoices",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("invoices")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = get_url("invoices")
    with Transformer() as transformer:
        data = request(url, {"updated_since": updated_since})['invoices']
        invoices_time_extracted = utils.now()

        for row in data:
            item = transformer.transform(row, schema)
            append_times_to_dates(item, [
                "period_start",
                "period_end",
                "issued_date",
                "due_date",
                "paid_date",
            ])

            singer.write_record("invoices",
                                item,
                                time_extracted=invoices_time_extracted)

            utils.update_state(STATE, "invoices", item['updated_at'])

            # Extract all invoice_line_items
            for line_item in row['line_items']:
                line_item['invoice_id'] = row['id']
                line_item = transformer.transform(line_item, line_items_schema)

                singer.write_record("invoice_line_items",
                                    line_item,
                                    time_extracted=invoices_time_extracted)

            # Load all invoice_messages
            suburl = url + "/{}/messages".format(item['id'])
            messages_data = request(suburl)['invoice_messages']
            messages_time_extracted = utils.now()
            for subrow in messages_data:
                subrow['invoice_id'] = row['id']
                message = transformer.transform(subrow, messages_schema)
                if message['updated_at'] >= start:
                    append_times_to_dates(message, ["send_reminder_on"])
                    singer.write_record("invoice_messages",
                                        message,
                                        time_extracted=messages_time_extracted)

                # Extract all invoice_recipients
                for recipient in subrow['recipients']:
                    recipient['invoice_message_id'] = message['id']
                    recipient = transformer.transform(recipient, recipients_schema)

                    singer.write_record("invoice_recipients",
                                        recipient,
                                        time_extracted=invoices_time_extracted)

            # Load all invoice_payments
            suburl = url + "/{}/payments".format(item['id'])
            payments_data = request(suburl)['invoice_payments']
            payments_time_extracted = utils.now()

            for subrow in payments_data:
                subrow['payment_gateway_id'] = subrow['payment_gateway']['id']
                subrow['payment_gateway_name'] = subrow['payment_gateway']['name']
                payment = transformer.transform(subrow, payments_schema)
                if payment['updated_at'] >= start:
                    singer.write_record("invoice_payments",
                                        payment,
                                        time_extracted=payments_time_extracted)

            singer.write_state(STATE)

        singer.write_state(STATE)

def sync_estimates():
    messages_schema = load_schema("estimate_messages")
    bookmark_property = 'updated_at'
    singer.write_schema("estimate_messages",
                        messages_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    recipients_schema = load_schema("estimate_recipients")
    singer.write_schema("estimate_recipients",
                        recipients_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    line_items_schema = load_schema("estimate_line_items")
    singer.write_schema("estimate_line_items",
                        line_items_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    schema = load_schema("estimates")
    singer.write_schema("estimates",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("estimates")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = get_url("estimates")
    with Transformer() as transformer:
        data = request(url, {"updated_since": updated_since})['estimates']
        estimates_time_extracted = utils.now()

        for row in data:
            item = transformer.transform(row, schema)
            append_times_to_dates(item, ["issued_date"])

            singer.write_record("estimates",
                                item,
                                time_extracted=estimates_time_extracted)

            utils.update_state(STATE, "estimates", item['updated_at'])

            # Extract all estimate_line_items
            for line_item in row['line_items']:
                line_item['estimate_id'] = row['id']
                line_item = transformer.transform(line_item, line_items_schema)

                singer.write_record("estimate_line_items",
                                    line_item,
                                    time_extracted=estimates_time_extracted)

            # Load all estimate_messages
            suburl = url + "/{}/messages".format(item['id'])
            messages_data = request(suburl)['estimate_messages']
            messages_time_extracted = utils.now()
            for subrow in messages_data:
                subrow['estimate_id'] = row['id']
                message = transformer.transform(subrow, messages_schema)
                if message['updated_at'] >= start:
                    append_times_to_dates(message, ["send_reminder_on"])
                    singer.write_record("estimate_messages",
                                        message,
                                        time_extracted=messages_time_extracted)

                # Extract all estimate_recipients
                for recipient in subrow['recipients']:
                    recipient['estimate_message_id'] = message['id']
                    recipient = transformer.transform(recipient, recipients_schema)

                    singer.write_record("estimate_recipients",
                                        recipient,
                                        time_extracted=estimates_time_extracted)

            singer.write_state(STATE)

        singer.write_state(STATE)

def sync_roles():
    user_roles_schema = load_schema("user_roles")
    bookmark_property = 'updated_at'
    singer.write_schema("user_roles",
                        user_roles_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    schema = load_schema("roles")
    singer.write_schema("roles",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("roles")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = get_url("roles")
    with Transformer() as transformer:
        data = request(url, {"updated_since": updated_since})['roles']
        time_entries_time_extracted = utils.now()

        for row in data:
            item = transformer.transform(row, schema)

            singer.write_record("roles",
                                item,
                                time_extracted=time_entries_time_extracted)

            utils.update_state(STATE, "roles", item['updated_at'])

            # Extract external_reference
            for user_id in row['user_ids']:
                pivot_row = {
                    'role_id': row['id'],
                    'user_id': user_id
                }

                singer.write_record("time_entry_external_reference",
                                    pivot_row,
                                    time_extracted=time_entries_time_extracted)

            singer.write_state(STATE)

        singer.write_state(STATE)

def sync_users():
    user_projects_schema = load_schema("user_projects")
    bookmark_property = 'updated_at'
    singer.write_schema("user_projects",
                        user_projects_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    schema = load_schema("users")
    singer.write_schema("users",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("users")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    url = get_url("users")
    with Transformer() as transformer:
        data = request(url, {"updated_since": updated_since})['users']
        users_time_extracted = utils.now()

        for row in data:
            item = row
            item = transformer.transform(item, schema)

            singer.write_record("users",
                                item,
                                time_extracted=users_time_extracted)

            utils.update_state(STATE, "users", item['updated_at'])

            # Load all user_projects
            suburl = url + "/{}/project_assignments".format(item['id'])
            project_assignments_data = request(suburl)['project_assignments']
            payments_time_extracted = utils.now()

            for subrow in project_assignments_data:
                user_project = transformer.transform(subrow, user_projects_schema)
                if user_project['updated_at'] >= start:
                    singer.write_record("user_projects",
                                        user_project,
                                        time_extracted=payments_time_extracted)

            singer.write_state(STATE)

        singer.write_state(STATE)


def do_sync():
    LOGGER.info("Starting sync")

    # Grab all clients and client contacts. Contacts have client FKs so grab
    # them last.
    sync_endpoint("clients")
    sync_endpoint("contacts")
    sync_roles()

    # Get all people and tasks before grabbing the projects. When we grab the
    # projects we will grab the project_users, project_tasks, and time_entries
    # for each.
    sync_users()
    sync_endpoint("tasks")
    sync_endpoint("projects")

    # Sync related project objects
    sync_endpoint("project_tasks", endpoint='task_assignments', path='task_assignments')
    sync_endpoint("project_users", endpoint='user_assignments', path='user_assignments')

    # Sync expenses and their categories
    sync_endpoint("expense_categories")
    sync_endpoint("expenses", date_fields=["spent_at"])

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
