#!/usr/bin/env python3

import datetime

import requests
import singer

from tap_harvest import utils


logger = singer.get_logger()
session = requests.Session()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "subdomain",
]

BASE_URL = "https://{}.harvestapp.com/"
CONFIG = {}
STATE = {}


def get_start(key):
    if key not in STATE:
        STATE[key] = CONFIG['start_date']

    return STATE[key]


def get_url(endpoint):
    return BASE_URL.format(CONFIG['subdomain']) + endpoint


def refresh_token():
    payload = {
        "refresh_token": CONFIG["refresh_token"],
        "client_id": CONFIG["client_id"],
        "client_secret": CONFIG["client_secret"],
        "grant_type": "refresh_token",
    }

    logger.info("Refreshing token")
    url = get_url("oauth2/token")
    resp = requests.post(url, data=payload)
    resp.raise_for_status()
    auth = resp.json()

    expires = datetime.datetime.utcnow() + datetime.timedelta(seconds=auth['expires_in'])
    logger.info("Token refreshed. Expires at {}".format(expires))

    CONFIG["access_token"] = auth["access_token"]
    CONFIG["token_expires"] = expires - datetime.timedelta(seconds=600)


@utils.ratelimit(100, 15)
def request(url, params=None):
    if CONFIG.get("token_expires") is None or CONFIG["token_expires"] < datetime.datetime.utcnow():
        refresh_token()

    params = params or {}
    params["access_token"] = CONFIG["access_token"]
    headers = {"Accept": "application/json"}
    req = requests.Request("GET", url=url, params=params, headers=headers).prepare()
    logger.info("GET {}".format(req.url))
    resp = session.send(req)
    resp.raise_for_status()
    return resp.json()


def sync_endpoint(endpoint, path, table_name=None, date_fields=None):
    table_name = table_name or endpoint
    schema = utils.load_schema(endpoint)
    singer.write_schema(table_name, schema, ["id"])
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
    tasks_schema = utils.load_schema("project_tasks")
    singer.write_schema("project_tasks", tasks_schema, ["id"])

    users_schema = utils.load_schema("project_users")
    singer.write_schema("project_users", users_schema, ["id"])

    schema = utils.load_schema("projects")
    singer.write_schema("projects", schema, ["id"])
    start = get_start("projects")

    start_dt = utils.strptime(start)
    updated_since = start_dt.strftime("%Y-%m-%d %H:%M")

    url = get_url("projects")
    for row in request(url):
        item = row["project"]
        for date_field in ["starts_on", "ends_on", "hint_earliest_record_at", "hint_latest_record_at"]:
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
    schema = utils.load_schema("time_entries")
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
    payments_schema = utils.load_schema("invoice_payments")
    singer.write_schema("invoice_payments", payments_schema, ["id"])

    schema = utils.load_schema("invoices")
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
    logger.info("Starting sync")

    # Grab all clients and client contacts. Contacts have client FKs so grab
    # them last.
    sync_endpoint("clients", "client")
    sync_endpoint("contacts", "contact")

    # Get all people and tasks before grabbing the projects. When we grab the
    # projects we will grab the project_users and project_tasks for each.
    sync_endpoint("people", "user", table_name="users")
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

    logger.info("Sync complete")


def main():
    config, state = utils.parse_args(REQUIRED_CONFIG_KEYS)
    CONFIG.update(config)
    STATE.update(state)
    do_sync()


if __name__ == "__main__":
    main()
