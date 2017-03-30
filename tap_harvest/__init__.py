#!/usr/bin/env python3

import datetime
import os

import requests
import singer

from singer import utils


LOGGER = singer.get_logger()
SESSION = requests.Session()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "access_token",
    "account_name",
]

BASE_URL = "https://{}.harvestapp.com/"
CONFIG = {}
STATE = {}

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

@utils.ratelimit(100, 15)
def request(url, params=None):
    params = params or {}
    params["access_token"] = CONFIG["access_token"]
    headers = {"Accept": "application/json"}
    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    LOGGER.info("GET {}".format(req.url))
    resp = SESSION.send(req)
    resp.raise_for_status()
    return resp.json()


def sync_endpoint(endpoint, path, date_fields=None):
    schema = load_schema(endpoint)
    singer.write_schema(endpoint, schema, ["id"])
    start = get_start(endpoint)

    url = get_url(endpoint)
    for row in request(url):
        item = row[path]

        if date_fields:
            for date_field in date_fields:
                if item.get(date_field):
                    item[date_field] += "T00:00:00Z"

        if item['updated_at'] >= start:
            singer.write_record(endpoint, item)
            utils.update_state(STATE, endpoint, item['updated_at'])

    singer.write_state(STATE)


def sync_projects():
    tasks_schema = load_schema("project_tasks")
    singer.write_schema("project_tasks", tasks_schema, ["id"])

    users_schema = load_schema("project_users")
    singer.write_schema("project_users", users_schema, ["id"])

    schema = load_schema("projects")
    singer.write_schema("projects", schema, ["id"])
    start = get_start("projects")

    start_dt = utils.strptime(start)
    updated_since = start_dt.strftime("%Y-%m-%d %H:%M")

    url = get_url("projects")
    for row in request(url):
        item = row["project"]
        date_fields = ["starts_on", "ends_on", "hint_earliest_record_at", "hint_latest_record_at"]
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] += "T00:00:00Z"

        if item['updated_at'] >= start:
            singer.write_record("projects", item)
            utils.update_state(STATE, "projects", item['updated_at'])

        suburl = url + "/{}/user_assignments".format(item["id"])
        for subrow in request(suburl, params={"updated_since": updated_since}):
            subitem = subrow["user_assignment"]
            singer.write_record("project_users", subitem)

        suburl = url + "/{}/task_assignments".format(item["id"])
        for subrow in request(suburl, params={"updated_since": updated_since}):
            subitem = subrow["task_assignment"]
            singer.write_record("project_tasks", subitem)

    singer.write_state(STATE)


def sync_time_entries():
    schema = load_schema("time_entries")
    singer.write_schema("time_entries", schema, ["id"])
    start = get_start("time_entries")

    endpoint = "daily/{day_of_year}/{year}"
    params = {"slim": 1}

    start_date = utils.strptime(start).date()
    today = datetime.datetime.utcnow().date()
    while start_date <= today:
        year = start_date.timetuple().tm_year
        day_of_year = start_date.timetuple().tm_yday
        url = get_url(endpoint.format(day_of_year=day_of_year, year=year))
        data = request(url, params)
        for item in data["day_entries"]:
            if "spent_at" in item:
                item["spent_at"] += "T00:00:00Z"

            singer.write_record("time_entries", item)

        start_date += datetime.timedelta(days=1)

    utils.update_state(STATE, "time_entries", utils.strftime(today))


def sync_invoices():
    payments_schema = load_schema("invoice_payments")
    singer.write_schema("invoice_payments", payments_schema, ["id"])

    schema = load_schema("invoices")
    singer.write_schema("invoices", schema, ["id"])
    start = get_start("invoices")

    start_dt = utils.strptime(start)
    updated_since = start_dt.strftime("%Y-%m-%d %H:%M")

    url = get_url("invoices")
    while True:
        data = request(url, {"updated_since": updated_since})
        for row in data:
            item = row["invoices"]
            for date_field in ["issued_at", "due_at"]:
                if item.get(date_field):
                    item[date_field] += "T00:00:00Z"

            singer.write_record("invoices", item)
            utils.update_state(STATE, "invoices", item['updated_at'])

            suburl = url + "/{}/messages"
            for subrow in request(suburl):
                item = subrow["message"]
                if item['updated_at'] >= start:
                    singer.write_record("invoice_messages", item)

            suburl = url + "/{}/payments"
            for subrow in request(suburl):
                item = subrow["payment"]
                if item['updated_at'] >= start:
                    singer.write_record("invoice_payments", item)

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
    # projects we will grab the project_users and project_tasks for each.
    sync_endpoint("people", "user")
    sync_endpoint("tasks", "task")
    sync_projects()

    # Sync time entries one day at a time
    sync_time_entries()

    # Sync expenses and their categories
    sync_endpoint("expense_categories", "expense_category")
    sync_endpoint("expenses", "expense", date_fields=["spent_at"])

    # Sync invoices and all related records
    sync_endpoint("invoice_item_categories", "invoice_category")
    sync_invoices()

    LOGGER.info("Sync complete")


def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == "__main__":
    main()
