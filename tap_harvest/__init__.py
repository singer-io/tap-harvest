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
    "account_name",
]

BASE_URL = "https://{}.harvestapp.com/"
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
                                url='https://api.harvestapp.com/oauth2/token',
                                data={'client_id': self._client_id,
                                      'client_secret': self._client_secret,
                                      'refresh_token': self._refresh_token,
                                      'grant_type': 'refresh_token'})

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
    return BASE_URL.format(CONFIG['account_name']) + endpoint

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

def sync_endpoint(endpoint, path, date_fields=None):
    schema = load_schema(endpoint)
    bookmark_property = 'updated_at'

    singer.write_schema(endpoint,
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start(endpoint)

    url = get_url(endpoint)
    data = request(url)
    time_extracted = utils.now()

    with Transformer() as transformer:
        for row in data:
            item = row[path]
            item = transformer.transform(item, schema)

            append_times_to_dates(item, date_fields)

            if item[bookmark_property] >= start:
                singer.write_record(endpoint,
                                    item,
                                    time_extracted=time_extracted)

                utils.update_state(STATE, endpoint, item[bookmark_property])

    singer.write_state(STATE)


def sync_projects():
    bookmark_property = 'updated_at'
    tasks_schema = load_schema("project_tasks")
    singer.write_schema("project_tasks",
                        tasks_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    users_schema = load_schema("project_users")
    singer.write_schema("project_users",
                        users_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    entries_schema = load_schema("time_entries")
    singer.write_schema("time_entries",
                        entries_schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    schema = load_schema("projects")
    singer.write_schema("projects",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])
    start = get_start("projects")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%d %H:%M")

    url = get_url("projects")
    projects_data = request(url)
    projects_time_extracted = utils.now()

    with Transformer() as transformer:
        for row in projects_data:
            item = row["project"]
            item = transformer.transform(item, schema)
            date_fields = ["starts_on",
                           "ends_on",
                           "hint_earliest_record_at",
                           "hint_latest_record_at"]

            append_times_to_dates(item, date_fields)

            if item[bookmark_property] >= start:
                singer.write_record("projects",
                                    item,
                                    time_extracted=projects_time_extracted)

                utils.update_state(STATE, "projects", item[bookmark_property])

            suburl = url + "/{}/user_assignments".format(item["id"])
            project_users_data = request(suburl, params={"updated_since": updated_since})
            project_users_time_extracted = utils.now()

            for subrow in project_users_data:
                subitem = subrow["user_assignment"]
                subitem = transformer.transform(subitem, users_schema)
                singer.write_record("project_users",
                                    subitem,
                                    time_extracted=project_users_time_extracted)

            suburl = url + "/{}/task_assignments".format(item["id"])
            task_assignments_data = request(suburl, params={"updated_since": updated_since})
            task_assignments_time_extracted = utils.now()

            for subrow in task_assignments_data:
                subitem = subrow["task_assignment"]
                subitem = transformer.transform(subitem, tasks_schema)
                singer.write_record("project_tasks",
                                    subitem,
                                    time_extracted=task_assignments_time_extracted)

            suburl = url + "/{}/entries".format(item["id"])
            subparams = {
                "from": start_dt.strftime("%Y%m%d"),
                "to": datetime.datetime.utcnow().strftime("%Y%m%d"),
                "updated_since": updated_since,
            }

            time_entries_data = request(suburl, params=subparams)
            time_entries_time_extracted = utils.now()

            for subrow in time_entries_data:
                subitem = subrow["day_entry"]
                subitem = transformer.transform(subitem, entries_schema)
                singer.write_record("time_entries",
                                    subitem,
                                    time_extracted=time_entries_time_extracted)

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

    schema = load_schema("invoices")
    singer.write_schema("invoices",
                        schema,
                        ["id"],
                        bookmark_properties=[bookmark_property])

    start = get_start("invoices")

    start_dt = pendulum.parse(start)
    updated_since = start_dt.strftime("%Y-%m-%d %H:%M")

    url = get_url("invoices")
    with Transformer() as transformer:
        while True:
            data = request(url, {"updated_since": updated_since})
            invoices_time_extracted = utils.now()

            for row in data:
                item = row["invoices"]
                item = transformer.transform(item, schema)
                append_times_to_dates(item, ["issued_at", "due_at"])

                singer.write_record("invoices",
                                    item,
                                    time_extracted=invoices_time_extracted)

                utils.update_state(STATE, "invoices", item['updated_at'])

                suburl = url + "/{}/messages".format(item['id'])
                messages_data = request(suburl)
                messages_time_extracted = utils.now()
                for subrow in messages_data:
                    subitem = subrow["message"]
                    if subitem['updated_at'] >= start:
                        append_times_to_dates(subitem, ["send_reminder_on"])
                        singer.write_record("invoice_messages",
                                            subitem,
                                            time_extracted=messages_time_extracted)

                suburl = url + "/{}/payments".format(item['id'])
                payments_data = request(suburl)
                payments_time_extracted = utils.now()

                for subrow in payments_data:
                    subitem = subrow["payment"]
                    subitem = transformer.transform(subitem, payments_schema)
                    if subitem['updated_at'] >= start:
                        singer.write_record("invoice_payments",
                                            subitem,
                                            time_extracted=payments_time_extracted)

                singer.write_state(STATE)

            if len(data) < 50:
                break

        singer.write_state(STATE)


def do_sync():
    LOGGER.info("Starting sync")

    # Grab all clients and client contacts. Contacts have client FKs so grab
    # them last.
    sync_endpoint("clients", "client")
    sync_endpoint("contacts", "contact")

    # Get all people and tasks before grabbing the projects. When we grab the
    # projects we will grab the project_users, project_tasks, and time_entries
    # for each.
    sync_endpoint("people", "user")
    sync_endpoint("tasks", "task")
    sync_projects()

    # Sync expenses and their categories
    sync_endpoint("expense_categories", "expense_category")
    sync_endpoint("expenses", "expense", date_fields=["spent_at"])

    # Sync invoices and all related records
    sync_endpoint("invoice_item_categories", "invoice_category")
    sync_invoices()

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
