#!/usr/bin/env python3

import os
import argparse
import requests
import singer
import json
import sys
import backoff

from datetime import datetime
from datetime import timedelta
from dateutil import tz

session = requests.Session()
logger = singer.get_logger()

HARVEST_HOST = "https://api.harvestapp.com"
HARVEST_CLIENT_ID = None
HARVEST_CLIENT_SECRET = None
HARVEST_REDIRECT_URI = None
HARVEST_AUTH_CODE = None
HARVEST_ACCESS_TOKEN = None
HARVEST_REFRESH_TOKEN = None
HARVEST_TOKEN_EXPIRES = None

DATETIME_FMT = "%Y-%m-%dT%H:%M:%SZ"
HARVEST_DATE_FMT = "%Y-%m-%d+%H:%M"

default_start_date = datetime(2000, 1, 1).strftime(DATETIME_FMT)
schemas = [
    'clients',
    'contacts',
    'invoices',
    'invoice_item_categories',
    'invoice_messages',
    'invoice_payments',
    'expenses',
    'expense_categories',
    'projects',
    'project_users',
    'tasks',
    'project_tasks',
    'people',
    'time_entries'
]
# Assign a default start date to each schema
state = {}
for i in schemas:
    state[i] = default_start_date

# Harvest API Details: http://help.getharvest.com/api/
endpoints = {
    "clients": "/clients",
    "client_detail": "/clients/{client_id}",
    "contacts": "/contacts",
    "contact_detail": "/contacts/{contact_id}",
    "client_contacts": "/clients/{client_id}/contacts",

    "invoices": "/invoices",
    "invoice_detail": "/invoices/{invoice_id}",
    "invoice_item_categories": "/invoice_item_categories",
    # Invoice Category Detail is for PUT only
    # "invoice_category_detail":  "/invoice_item_categories/{category_id}",
    "invoice_messages": "/invoices/{invoice_id}/messages",
    "invoice_message_detail": "/invoices/{invoice_id}/message/{message_id}",
    "invoice_payments": "/invoices/{invoice_id}/payments",
    "invoice_payment_detail": "/invoices/{invoice_id}/payments/{payment_id}",

    "expenses": "/expenses",
    "expense_detail": "/expenses/{expense_id}",
    # Receipts are only Update / Delete
    # "expense_receipt": "/expenses/{expense_id}/receipt",
    "expense_categories": "/expense_categories",
    "expense_category_detail": "/expense_categories/{expense_category_id}",

    "projects": "/projects",
    "project_detail": "/projects/{project_id}",
    "project_users": "/projects/{project_id}/user_assignments",
    "project_users_detail": "/projects/{project_id}/user_assignments/{user_assignment_id}",

    "tasks": "/tasks",
    "task_detail": "/tasks/{task_id}",
    "project_tasks": "/projects/{project_id}/task_assignments",
    "project_task_detail": "/projects/{project_id}/task_assignments/{task_assignment_id}",

    "people": "/people",
    "people_detail": "/people/{user_id}",

    # Reports
    "time_entries_by_project": "/projects/{project_id}/entries",
    "time_entries": "/people/{user_id}/entries",
    "expenses_by_project": "/projects/{project_id}/expenses",
    "expenses_by_user": "/people/{user_id}/expenses"
}

# Assign a default key to each entity schema
# for now "id" should suffice
entity_keys = {}
for i in schemas:
    entity_keys[i] = "id"


# Override specific entity_keys as needed
# e.g. entity_keys[people] = ["id", "email"]

def update_state(key, dt):
    if dt is None:
        return

    if isinstance(dt, datetime):
        dt = dt.strftime(DATETIME_FMT)

    if dt > state[key]:
        state[key] = dt


def get_url(endpoint, **kwargs):
    if endpoint not in endpoints:
        raise ValueError("Invalid endpoint {}".format(endpoint))

    return HARVEST_HOST + endpoints[endpoint].format(**kwargs)


def get_field_type_schema(field_type):
    if field_type == "bool":
        return {"type": ["null", "boolean"]}

    elif field_type == "datetime":
        return {
            "anyOf": [
                {
                    "type": "string",
                    "format": "date-time",
                },
                {
                    "type": "null",
                },
            ],
        }

    elif field_type == "number":
        return {"type": ["null", "number"]}

    else:
        return {"type": ["null", "string"]}


def get_field_schema(field_type, extras=False):
    if extras:
        return {
            "type": ["null", "object"],
            "properties": {
                "value": get_field_type_schema(field_type),
                "timestamp": get_field_type_schema("datetime"),
                "source": get_field_type_schema("string"),
                "sourceId": get_field_type_schema("string"),
            }
        }
    else:
        return get_field_type_schema(field_type)


def parse_custom_schema(entity_name, data):
    extras = entity_name != "contacts"
    return {field['name']: get_field_schema(field['type'], extras) for field in data}


def get_custom_schema(entity_name):
    data = request(get_url(entity_name + "_properties")).json()
    return parse_custom_schema(entity_name, data)


def transform_timestamp(timestamp):
    return datetime.strptime(timestamp, DATETIME_FMT)


def transform_field(value, schema):
    if "array" in schema['type']:
        tmp = []
        for v in value:
            tmp.append(transform_field(v, schema['items']))

        return tmp

    elif "object" in schema['type']:
        tmp = {}
        for field_name, field_schema in schema['properties'].items():
            if field_name in value:
                tmp[field_name] = transform_field(value[field_name], field_schema)

        return tmp

    elif "format" in schema:
        if schema['format'] == "date-time" and value:
            return transform_timestamp(value)

    elif "integer" in schema['type'] and value:
        return int(value)

    elif "number" in schema['type'] and value:
        return float(value)


def transform_record(record, schema):
    return {field_name: transform_field(record[field_name], field_schema)
            for field_name, field_schema in schema['properties'].items()
            if field_name in record}


def get_authorization():
    # For the purposes of this script, authorization is a manual process
    # This is a simple helper function to get the authorizatin code
    # @TODO - Move this into another python script to be run separately
    # @TODO - Move this into python stitch tap starter scaffolding
    global HARVEST_REDIRECT_URI
    global HARVEST_CLIENT_ID
    global HARVEST_CLIENT_SECRET

    response = requests.get(
        url=HARVEST_HOST + '/oauth2/authorize',
        data={
            'redirect_uri': HARVEST_REDIRECT_URI,
            'client_id': HARVEST_CLIENT_ID,
            'client_secret': HARVEST_CLIENT_SECRET,
            'state': 'optional-csrf-token',
            'response_type': 'code'
        }
    )

    # Print the URL for the authorization URL
    # It should look like:
    expected_url = 'https://api.harvestapp.com/oauth2/authorize?client_id=' + HARVEST_CLIENT_ID + '&redirect_uri=https%3A%2F%2Flocalhost:8080%2Foauth_redirect&state=optional-csrf-token&response_type=refresh_token'

    print(response)


def get_access_token():
    # For the purposes of this script, getting the first access_token is
    # a manual process. This is a helper function to get the authorization code
    # @TODO - Move this into another python script to be run separately
    # @TODO - Move this into python stitch tap starter scaffolding
    global HARVEST_AUTH_CODE
    global HARVEST_ACCESS_TOKEN
    global HARVEST_REFRESH_TOKEN
    global HARVEST_TOKEN_EXPIRES
    global HARVEST_CLIENT_ID
    global HARVEST_CLIENT_SECRET

    print('clientid: ' + HARVEST_CLIENT_ID)
    print('authcode: ' + HARVEST_AUTH_CODE)
    print('redirecturi: ' + HARVEST_REDIRECT_URI)
    print('secret: ' + HARVEST_CLIENT_SECRET)
    # print('accesstoken: ' + HARVEST_ACCESS_TOKEN)

    response = requests.post(
        url=HARVEST_HOST + '/oauth2/token',
        data={
            'code': HARVEST_AUTH_CODE,
            'client_id': HARVEST_CLIENT_ID,
            'client_secret': HARVEST_CLIENT_SECRET,
            'redirect_uri': HARVEST_REDIRECT_URI,
            'grant_type': 'authorization_code'
        },
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
    )

    if response.status_code == 400:
        # failed to auth
        logger.info("Failed to get_refresh_token with status code %s", response.status_code)
        sys.exit(1)

    print(response.json())
    if 'access_token' in response:
        HARVEST_ACCESS_TOKEN = response.json()['access_token']
        HARVEST_REFRESH_TOKEN = response.json()['refresh_token']
        session.headers.update({'Authorization': 'Bearer ' + HARVEST_ACCESS_TOKEN})
    else:
        raise Exception('Access Token Retrieval Failed: ' + str(response.json()))
    return HARVEST_ACCESS_TOKEN


def get_refresh_token():
    # @TODO - See function in example: https://github.com/harvesthq/harvest_api_samples/blob/master/oauth/harvest_api_oauth_sample.rb#L79
    # Refresh token never expires
    # To start the stream, you must manually request the authorization_code at
    # https://api.harvestapp.com/oauth2/authorize?client_id={some_id}&redirect_uri=https%3A%2F%2Flocalhost:8080%2Foauth_redirect&state=optional-csrf-token&response_type=token
    global HARVEST_ACCESS_TOKEN
    global HARVEST_REFRESH_TOKEN
    global HARVEST_TOKEN_EXPIRES
    global HARVEST_CLIENT_ID
    global HARVEST_CLIENT_SECRET

    url = HARVEST_HOST + '/oauth2/token'
    data = {
        'refresh_token': HARVEST_REFRESH_TOKEN,
        'client_id': HARVEST_CLIENT_ID,
        'client_secret': HARVEST_CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }

    response = requests.post(url, data, headers)

    if response.status_code == 400:
        # failed to auth
        logger.info("Failed to get_refresh_token with status code %s and data: %s", response.status_code, data)
        sys.exit(1)

    HARVEST_ACCESS_TOKEN = response.json()['access_token']
    HARVEST_REFRESH_TOKEN = response.json()['refresh_token']
    HARVEST_TOKEN_EXPIRES = datetime.utcnow() + timedelta(seconds=response.json()['expires_in'])
    logger.info("Token expires at %s", HARVEST_TOKEN_EXPIRES)

    return HARVEST_ACCESS_TOKEN


def load_schema(entity_name):
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        "tap_harvest_schemas",
                        "{}.json".format(entity_name))
    with open(path) as f:
        schema = json.loads(f.read())

    return schema


def load_key(entity_name):
    return entity_keys[entity_name]


def sync_schema(entity_name):
    # if entity is special, load from an entity detail instead of matching
    # entity_name in endpoints dict

    if state[entity_name]:
        logger.info('Replicating commits since %s for %s from %s', state[entity_name], entity_name,
                    endpoints[entity_name])
    else:
        logger.info('Replicating all commits for %s from %s', entity_name, endpoints[entity_name])

    schema = load_schema(entity_name)
    key_property = load_key(entity_name)
    stream('schema', schema, entity_name, key_property)

    return


def load_state(state_file):
    with open(state_file) as f:
        state = json.load(f)

    state.update(state)


@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException),
                      max_tries=5,
                      giveup=lambda e: e.response is not None and 400 <= e.response.status_code < 500,
                      factor=2)
# Abstract requests.get to allow for recursive reauth
# Not to be used with OAuth2 flow, just data requests
def request(url, params=None, reauth=False):
    if datetime.utcnow() >= HARVEST_TOKEN_EXPIRES:
        refresh_token()

    params = params or {}
    params["access_token"] = HARVEST_ACCESS_TOKEN

    response = requests.get(
        url,
        params=params,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
    )
    response.raise_for_status()
    return response


def stream(method, data, entity_type=None, key_property=None):
    if method == "state":
        singer.write_state(data)
    elif method == "schema":
        singer.write_schema(entity_type, data, key_properties=key_property)
    elif method == "records":
        singer.write_record(entity_type, data)
    else:
        raise ValueError("Unknown method {}".format(method))


# @TODO - Collapse sync_records, sync_nested_records, and sync_records_from_list into one def
def sync_records(sync_attributes, params=None):
    schema = sync_attributes["schema"]
    # parent_endpoint = sync_attributes["parent_endpoint"]
    # parent_properties_in = sync_attributes["parent_properties_in"]
    # key_from_parent = sync_attributes["key_from_parent"]
    endpoint = sync_attributes["endpoint"]
    endpoint_detail = sync_attributes["endpoint_detail"]
    endpoint_detail_key = sync_attributes["endpoint_detail_key"]
    properties_in = sync_attributes["properties_in"]

    last_sync = datetime.strptime(state[schema], DATETIME_FMT)
    days_since_sync = (datetime.utcnow() - last_sync).days

    logger.info("Syncing all %s", endpoint)
    sync_schema(schema)

    harvest_updated_since = last_sync.strftime(HARVEST_DATE_FMT)
    logger.info("Harvest %s tap hasn't been updated since: %s", endpoint, harvest_updated_since)
    params = params or {
        "updated_since": harvest_updated_since
    }

    # @TODO - if sync_attributes requires parent_endpoint, then iterate through that endpoint

    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint, params=params))
        # logger.info("Response from %s endpoint: %s", endpoint, resp)
        data = resp.json()
        # logger.info("JSON Response from %s endpoint: %s", endpoint, data)

        # This endpoint always returns all results
        has_more = False

        for record in data:
            record = record[properties_in]
            # @TODO - Load id based on endpoint_detail_key required variable
            resp = request(get_url(endpoint_detail, **{endpoint_detail_key: record['id']}))
            resp = resp.json()[properties_in]

            if 'updated_at' in record:
                modified_time = transform_timestamp(resp['updated_at'])
            elif 'created_at' in record:
                modified_time = transform_timestamp(resp['created_at'])
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                stream('records', resp, schema)
                persisted_count += 1

            update_state(endpoint, modified_time)

        stream('state', state)

    return persisted_count

def sync_nested_records(sync_attributes, params=None):
    schema = sync_attributes["schema"]
    parent_endpoint = sync_attributes["parent_endpoint"]
    parent_properties_in = sync_attributes["parent_properties_in"]
    key_from_parent = sync_attributes["key_from_parent"]
    endpoint = sync_attributes["endpoint"]
    endpoint_detail = sync_attributes["endpoint_detail"]
    endpoint_detail_key = sync_attributes["endpoint_detail_key"]
    properties_in = sync_attributes["properties_in"]

    last_sync = datetime.strptime(state[schema], DATETIME_FMT)
    days_since_sync = (datetime.utcnow() - last_sync).days

    logger.info("Syncing all %s", endpoint)
    sync_schema(schema)

    harvest_updated_since = last_sync.strftime(HARVEST_DATE_FMT)
    logger.info("Harvest %s tap hasn't been updated since: %s", endpoint, harvest_updated_since)

    params = params or {
        'updated_since': harvest_updated_since
    }

    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url(parent_endpoint), params=None)
        # logger.info("Response from %s endpoint: %s", parent_endpoint, resp)
        data = resp.json()
        # logger.info("JSON Response from %s endpoint: %s", parent_endpoint, data)

        # Get all time entries by user
        for record in data:
            # Records are inside of an object
            record = record[parent_properties_in]
            # logger.info("User Record: %s", record)
            # Request the time entries using the key_from_parent, in this case user_id as a key
            respb = request(get_url(endpoint, **{endpoint_detail_key: record[key_from_parent]}), params)

            # Iterate through each record returned
            # In time_entries case, each response is contained in a day_entry
            for entry in respb.json():
                # logger.info("Print the entry %s", entry)
                entry = entry[properties_in]

                if 'updated_at' in entry:
                    modified_time = transform_timestamp(entry['updated_at'])
                elif 'created_at' in entry:
                    modified_time = transform_timestamp(entry['created_at'])
                else:
                    modified_time = None

                if not modified_time or modified_time >= last_sync:
                    stream('records', entry, schema)
                    persisted_count += 1

                update_state(endpoint, modified_time)

        stream('state', state)
        # No pagination, set has_more to False
        has_more = False

    return persisted_count


def sync_records_from_list(sync_attributes, params=None):
    schema = sync_attributes["schema"]
    endpoint = sync_attributes["endpoint"]
    properties_in = sync_attributes["properties_in"]

    last_sync = datetime.strptime(state[schema], DATETIME_FMT)
    days_since_sync = (datetime.utcnow() - last_sync).days

    logger.info("Syncing all %s", endpoint)
    sync_schema(schema)

    harvest_updated_since = last_sync.strftime(HARVEST_DATE_FMT)
    logger.info("Harvest %s tap hasn't been updated since: %s", endpoint, harvest_updated_since)

    params = params or {
        'updated_since': harvest_updated_since
    }

    has_more = True
    persisted_count = 0
    while has_more:
        resp = request(get_url(endpoint), params)
        # logger.info("Response from %s endpoint: %s", parent_endpoint, resp)
        data = resp.json()
        # logger.info("JSON Response from %s endpoint: %s", parent_endpoint, data)

        for entry in data:
            # logger.info("Print the entry %s", entry)
            entry = entry[properties_in]

            if 'updated_at' in entry:
                modified_time = transform_timestamp(entry['updated_at'])
            elif 'created_at' in entry:
                modified_time = transform_timestamp(entry['created_at'])
            else:
                modified_time = None

            if not modified_time or modified_time >= last_sync:
                stream('records', entry, schema)
                persisted_count += 1

            update_state(endpoint, modified_time)

        stream('state', state)
        # No pagination, set has_more to False
        has_more = False

    return persisted_count


def sync_clients():
    sync_attributes = {
        "schema": "clients",
        "endpoint": "clients",
        "endpoint_detail": "client_detail",
        "endpoint_detail_key": "client_id",
        "properties_in": "client"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_contacts():
    sync_attributes = {
        "schema": "contacts",
        "endpoint": "contacts",
        "endpoint_detail": "contact_detail",
        "endpoint_detail_key": "contact_id",
        "properties_in": "contact"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_expense_categories():
    # ref: http://help.getharvest.com/api/clients-api/clients/using-the-client-contacts-api/
    sync_attributes = {
        "schema": "expense_categories",
        "endpoint": "expense_categories",
        "endpoint_detail": "expense_category_detail",
        "endpoint_detail_key": "expense_category_id",
        "properties_in": "expense_category"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_expenses():
    # ref: http://help.getharvest.com/api/expenses-api/expenses/add-update-expenses/
    sync_attributes = {
        "schema": "expenses",
        "endpoint": "expenses",
        "endpoint_detail": "expense_detail",
        "endpoint_detail_key": "expense_id",
        "properties_in": "expense"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_invoice_item_categories():
    sync_attributes = {
        "schema": "invoice_item_categories",
        "endpoint": "invoice_item_categories",
        "properties_in": "invoice_category"
    }

    persisted_count = sync_records_from_list(sync_attributes)

    return persisted_count


# We have to solve for catching both types
def sync_invoices():
    sync_attributes = {
        "schema": "invoices",
        "endpoint": "invoices",
        "properties_in": "invoices"
    }

    persisted_count = sync_records_from_list(sync_attributes)

    return persisted_count


def sync_invoice_messages():
    sync_attributes = {
        "schema": "invoice_messages",
        "parent_endpoint": "invoices",
        "parent_properties_in": "invoices",
        "key_from_parent": "id",
        "endpoint": "invoice_messages",
        "endpoint_detail": "",
        "endpoint_detail_key": "invoice_id",
        "properties_in": "message"
    }

    persisted_count = sync_nested_records(sync_attributes)

    return persisted_count


def sync_invoice_payments():
    sync_attributes = {
        "schema": "invoice_payments",
        "parent_endpoint": "invoices",
        "parent_properties_in": "invoices",
        "key_from_parent": "id",
        "endpoint": "invoice_payments",
        "endpoint_detail": "",
        "endpoint_detail_key": "invoice_id",
        "properties_in": "payment"
    }

    persisted_count = sync_nested_records(sync_attributes)

    return persisted_count


def sync_people():
    sync_attributes = {
        "schema": "people",
        "endpoint": "people",
        "endpoint_detail": "people_detail",
        "endpoint_detail_key": "user_id",
        "properties_in": "user"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_tasks():
    sync_attributes = {
        "schema": "tasks",
        "endpoint": "tasks",
        "endpoint_detail": "task_detail",
        "endpoint_detail_key": "task_id",
        "properties_in": "task"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_projects():
    sync_attributes = {
        "schema": "projects",
        "endpoint": "projects",
        "endpoint_detail": "project_detail",
        "endpoint_detail_key": "project_id",
        "properties_in": "project"
    }

    persisted_count = sync_records(sync_attributes)

    return persisted_count


def sync_project_tasks():
    sync_attributes = {
        "schema": "project_tasks",
        "parent_endpoint": "projects",
        "parent_properties_in": "project",
        "key_from_parent": "id",
        "endpoint": "project_tasks",
        "endpoint_detail": "",
        "endpoint_detail_key": "project_id",
        "properties_in": "task_assignment"
    }

    persisted_count = sync_nested_records(sync_attributes)

    return persisted_count


def sync_project_users():
    sync_attributes = {
        "schema": "project_users",
        "parent_endpoint": "projects",
        "parent_properties_in": "project",
        "key_from_parent": "id",
        "endpoint": "project_users",
        "endpoint_detail": "",
        "endpoint_detail_key": "project_id",
        "properties_in": "user_assignment"
    }

    persisted_count = sync_nested_records(sync_attributes)

    return persisted_count


def sync_time_entries():
    sync_attributes = {
        "schema": "time_entries",
        "parent_endpoint": "people",
        "parent_properties_in": "user",
        "key_from_parent": "id",
        "endpoint": "time_entries",
        "endpoint_detail": "",
        "endpoint_detail_key": "user_id",
        "properties_in": "day_entry"
    }

    last_sync = datetime.strptime(state[schema], DATETIME_FMT)
    days_since_sync = (datetime.utcnow() - last_sync).days

    harvest_updated_since = last_sync.strftime(HARVEST_DATE_FMT)
    logger.info("Harvest %s tap hasn't been updated since: %s", endpoint, harvest_updated_since)

    params = {
        'from': 20000101,
        'to': datetime.utcnow().strftime("%Y%m%d"),
        'updated_since': harvest_updated_since
    }

    # @TODO - Re-enable this once technical debt has been resolved for two double-star arguments in the endpoint_detail
    persisted_count = sync_nested_records(sync_attributes, params)

    return persisted_count


def do_check(args):
    raise Exception("check command is not supported yet")


def do_sync():
    persisted_count = 0
    persisted_count += sync_clients()
    persisted_count += sync_contacts()
    persisted_count += sync_expense_categories()
    persisted_count += sync_expenses()
    persisted_count += sync_invoice_item_categories()
    persisted_count += sync_invoices()
    persisted_count += sync_invoice_messages()
    persisted_count += sync_invoice_payments()
    persisted_count += sync_people()
    persisted_count += sync_tasks()
    persisted_count += sync_projects()
    persisted_count += sync_project_tasks()
    persisted_count += sync_project_users()
    persisted_count += sync_time_entries()
    return persisted_count


def main():
    global HARVEST_REFRESH_TOKEN
    global HARVEST_CLIENT_ID
    global HARVEST_CLIENT_SECRET
    global HARVEST_REDIRECT_URI
    global HARVEST_HOST
    global HARVEST_AUTH_CODE
    global HARVEST_ACCESS_TOKEN

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    parser_check = subparsers.add_parser('check')
    parser_check.set_defaults(func=do_check)

    parser_sync = subparsers.add_parser('sync')
    parser_sync.set_defaults(func=do_sync)

    for subparser in [parser_check, parser_sync]:
        subparser.add_argument(
            '-c', '--config', help='Config file', required=True)
        subparser.add_argument(
            '-s', '--state', help='State file')

    args = parser.parse_args()

    # Load State or use default
    if args.state:
        logger.info("Loading state from " + args.state)
        with open(args.state) as f:
            state.update(json.load(f))
    else:
        logger.info("Using default state")

    # Load config
    logger.info("Authorizing")
    with open(args.config) as f:
        config = json.load(f)
    # logger.info("Config loaded: %s", config)

    # Check config for all required keys
    missing_keys = []
    for key in ['harvest_client_id', 'harvest_client_secret', 'harvest_redirect_uri', 'harvest_host']:
        if key not in config:
            missing_keys += [key]

    if len(missing_keys) > 0:
        logger.fatal("Missing required configuration keys: {}".format(missing_keys))

    # Load Globals from config, where available
    HARVEST_AUTH_CODE = config['harvest_auth_code']
    HARVEST_CLIENT_ID = config['harvest_client_id']
    HARVEST_CLIENT_SECRET = config['harvest_client_secret']
    HARVEST_REDIRECT_URI = config['harvest_redirect_uri']
    HARVEST_HOST = config['harvest_host']
    HARVEST_ACCESS_TOKEN = config['harvest_access_token']
    HARVEST_REFRESH_TOKEN = config['harvest_refresh_token']

    # get access and refresh tokens
    get_refresh_token()
    # Write the access token and refresh token back to config
    config['harvest_access_token'] = HARVEST_ACCESS_TOKEN
    config['harvest_refresh_token'] = HARVEST_REFRESH_TOKEN
    config['harvest_token_expires'] = str(HARVEST_TOKEN_EXPIRES)
    with open(args.config, 'w') as outfile:
        json.dump(config, outfile)

    try:
        logger.info("Starting sync")
        persisted_count = do_sync()
        logger.info("%s total records synced", persisted_count)
    except Exception as e:
        logger.exception("Error occurred during sync. Aborting.")
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
