from singer.catalog import Catalog
from tap_harvest.schema import get_schemas, STREAMS

COMMON_STREAMS = [
    'clients',
    'contacts',
    'user_roles',
    'roles',
    'projects',
    'tasks',
    'project_tasks',
    'project_users',
    'user_project_tasks',
    'user_projects',
    'users',
    'external_reference',
    'time_entry_external_reference',
    'time_entries'
]

EXPENSES_STREAMS = [
    'expense_categories',
    'expenses'
]

INVOICES_STREAMS = [
    'invoice_item_categories',
    'invoice_line_items',
    'invoice_messages',
    'invoice_payments',
    'invoices'
]

ESTIMATES_STREAMS = [
    'estimate_item_categories',
    'estimate_line_items',
    'estimate_messages',
    'estimates'
]

BASE_API_URL = "https://api.harvestapp.com/v2/"

def discover(client):
    # Discover schemas, build metadata for all the steams and return catalog
    schemas, field_metadata = get_schemas()
    catalog_entries = []

    # API call of the company for the selection of streams.
    company = client.request(BASE_API_URL + 'company')

    available_streams = COMMON_STREAMS

    if company['expense_feature']:
        available_streams += EXPENSES_STREAMS
    if company['invoice_feature']:
        available_streams += INVOICES_STREAMS
    if company['estimate_feature']:
        available_streams += ESTIMATES_STREAMS

    for stream_name in available_streams:

        # create and add catalog entry
        schema = schemas[stream_name]
        mdata = field_metadata[stream_name]

        catalog_entry = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": mdata,
            "key_properties": STREAMS[stream_name].key_properties
        }
        catalog_entries.append(catalog_entry)

    return Catalog.from_dict({"streams": catalog_entries})
