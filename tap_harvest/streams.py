from datetime import datetime

import singer
import pendulum
from singer import Transformer, utils, metadata

LOGGER = singer.get_logger()
BASE_API_URL = "https://api.harvestapp.com/v2/"
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

def remove_empty_date_times(item, schema):
    """
    Any date-times values can either be a string or a null.
    If null, parsing the date results in an error.
    Instead, removing the attribute before parsing ignores this error.
    """
    fields = []

    # Loop through all keys of schema
    for key in schema['properties']:
        subschema = schema['properties'][key]

        # Append key with datatpye date-time into the `fields` list 
        if subschema.get('format') == 'date-time':
            fields.append(key)

    for field in fields:
        # Remove field with datatype date-time from record if value is None
        if item.get(field) is None:
            del item[field]

def get_url(endpoint):
    """
    Return URL which contain BASE_API_URL and endpoint path itself.
    """
    return BASE_API_URL + endpoint

def append_times_to_dates(item, date_fields):
    """
    Convert date field into the standard format
    """
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] = utils.strftime(utils.strptime_with_tz(item[date_field]))

def get_bookmark(stream_name, state, default):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream_name, default)
    )

class Stream:
    tap_stream_id = None
    replication_method="FULL_TABLE"
    replication_keys=[]
    key_properties=["id"]
    object_to_id = None
    parent = None
    parent_id = None
    with_updated_since = True
    children = []
    endpoint = None
    path = None
    date_fields = None

    def add_field_at_1st_level(self, row=None, parent_row=None):
        return row

    def write_schema(self, catalog):
        """
        Write the schema for the selected stream.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        try:
            singer.write_schema(self.tap_stream_id, schema, stream.key_properties)
        except OSError as err:
            LOGGER.info('OS Error writing schema for: %s', self.tap_stream_id)
            raise err

    def get_min_bookmark(self, stream, selected_streams, bookmark, start_date, state):
        """
        Get the minimum bookmark from the parent and its corresponding child bookmarks.
        """

        stream_obj = STREAMS[stream]()
        min_bookmark = bookmark
        if stream in selected_streams:
            # Get minimum of stream's bookmark(start date in case of no bookmark) and min_bookmark
            stream_name_in_state = f'{stream}_parent' if stream_obj.parent else stream
            min_bookmark = min(min_bookmark, get_bookmark(stream_name_in_state, state, start_date))
            LOGGER.debug("New minimum bookmark is %s", min_bookmark)

        for child in stream_obj.children:
            # Iterate through all children and return minimum bookmark among all.
            min_bookmark = min(min_bookmark, self.get_min_bookmark(child, selected_streams, min_bookmark, start_date, state))

        return min_bookmark

    def get_schema_and_metadata(self, catalog):
        """
        Return schema and metadata data of given stream.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict() # Get schema
        stream_metadata = metadata.to_map(stream.metadata) # Get metadata

        return schema, stream_metadata

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):
        """
        """

        schema, stream_metadata = self.get_schema_and_metadata(catalog)

        bookmark_field = next(iter(self.replication_keys)) if self.replication_keys else None

        children = self.children
        current_time = datetime.today().strftime(DATE_FORMAT)
        # Get the latest bookmark for the stream and set the last_datetime
        min_bookmark_among_parent_child = self.get_min_bookmark(self.tap_stream_id, selected_streams, current_time, config['start_date'], state)
        start_dt = pendulum.parse(min_bookmark_among_parent_child)

        last_datetime = get_bookmark(self.tap_stream_id, state, config['start_date'])

        updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        with Transformer() as transformer:
            page = 1
            while page is not None:
                url = get_url(self.endpoint or self.tap_stream_id).format(parent_row.get(self.parent_id))
                params = {"updated_since": updated_since} if self.with_updated_since else {}
                params['page'] =  page
                
                response = client.request(url, params)
                path = self.path or self.tap_stream_id
                data = response[path]
                time_extracted = utils.now()

                for row in data:

                    row = self.add_field_at_1st_level(row=row, parent_row=parent_row)
                    if self.parent_id:
                        row[self.parent[:-1]+'_id'] = parent_row.get(self.parent_id)
 
                    if self.object_to_id is not None:
                        for key in self.object_to_id:
                            if row[key] is not None:
                                row[key + '_id'] = row[key]['id']
                            else:
                                row[key + '_id'] = None

                    remove_empty_date_times(row, schema)

                    transformed_record = transformer.transform(row, schema, stream_metadata)
                    append_times_to_dates(transformed_record, self.date_fields)
                    singer.write_record(self.tap_stream_id, transformed_record, time_extracted=time_extracted)

                    # Loop thru parent batch records for each children objects
                    for child_stream_name in children:
                        if child_stream_name in selected_streams:
                            child_obj = STREAMS[child_stream_name]()
                            child_obj.sync_endpoint(client, catalog, config, state, tap_state, selected_streams, row)

                    if bookmark_field:
                        bookmark_dttm = transformed_record[bookmark_field]
                        if bookmark_dttm > last_datetime:
                            last_datetime = bookmark_dttm
                        utils.update_state(tap_state['bookmarks'], self.tap_stream_id, bookmark_dttm)

                page = response['next_page']
        
        for child_stream_name in children:
            if child_stream_name in selected_streams and bookmark_field:
                utils.update_state(tap_state['bookmarks'], f'{child_stream_name}_parent', last_datetime)
                if child_stream_name in tap_state['bookmarks'] and tap_state['bookmarks'][child_stream_name] > current_time:
                    tap_state['bookmarks'][child_stream_name] = current_time

        singer.write_state(tap_state)

class Clients(Stream):
    """
    https://help.getharvest.com/api-v2/clients-api/clients/clients/#list-all-clients
    """
    tap_stream_id="clients"
    replication_keys=["updated_at"]
    replication_method="INCREMENTAL"

class Contacts(Stream):
    """
    https://help.getharvest.com/api-v2/clients-api/clients/contacts/#list-all-contacts
    """
    tap_stream_id = 'contacts'
    replication_keys=["updated_at"]
    replication_method="INCREMENTAL"
    object_to_id = ['client']

class UserRoles(Stream):
    """
    
    """
    tap_stream_id = 'user_roles'
    key_properties = ["user_id", "role_id"]
    parent="roles"

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):

        for user_id in parent_row['user_ids']:
            time_extracted = utils.now()

            pivot_row = {
                'role_id': parent_row['id'],
                'user_id': user_id
            }
            singer.write_record("user_roles", pivot_row, time_extracted=time_extracted)

class Roles(Stream):
    tap_stream_id = 'roles'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    children = ["user_roles"]

class Projects(Stream):
    tap_stream_id = 'projects'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    object_to_id = ['client']

class Tasks(Stream):
    tap_stream_id = 'tasks'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class ProjectTasks(Stream):
    tap_stream_id = 'project_tasks'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    endpoint = 'task_assignments'
    path = 'task_assignments'
    object_to_id = ['project', 'task']

class ProjectUsers(Stream):
    tap_stream_id = 'project_users'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'user_assignments'
    object_to_id = ['project', 'user']
    endpoint = 'user_assignments'


class UserProjectTasks(Stream):
    tap_stream_id = 'user_project_tasks'
    key_properties = ["user_id", "project_task_id"]
    parent="users"
    parent_id = 'id'

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):

        for project_task in parent_row['task_assignments']:

            time_extracted = utils.now()
            pivot_row = {
                'user_id': project_task['user_id'],
                'project_task_id': project_task['id']
            }

            singer.write_record(self.tap_stream_id, pivot_row, time_extracted=time_extracted)

class UserProjects(Stream):
    tap_stream_id = 'user_projects'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'project_assignments'
    with_updated_since=False
    endpoint = "users/{}/project_assignments"
    parent="users"
    parent_id = 'id'
    object_to_id = ['project', 'client', 'user']


class Users(Stream):
    tap_stream_id = 'users'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    children=["user_projects"]

class ExpenseCategories(Stream):
    """
    https://help.getharvest.com/api-v2/expenses-api/expenses/expense-categories/#list-all-expense-categories
    """
    tap_stream_id = 'expense_categories'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class Expenses(Stream):
    """
    https://help.getharvest.com/api-v2/expenses-api/expenses/expenses/#list-all-expenses
    """
    tap_stream_id = 'expenses'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    object_to_id =['client', 'project', 'expense_category', 'user', 'user_assignment', 'invoice']

class InvoiceItemCategories(Stream):
    """
    https://help.getharvest.com/api-v1/invoices-api/invoices/invoice-messages-payments/#show-all-categories
    """
    tap_stream_id = 'invoice_item_categories'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class InvoiceMessages(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-messages/
    """
    tap_stream_id = 'invoice_messages'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    endpoint="invoices/{}/messages"
    path = 'invoice_messages'
    parent = "invoices"
    parent_id = "id"

class InvoicePayments(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-payments/#list-all-payments-for-an-invoice
    """
    tap_stream_id = 'invoice_payments'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'invoice_payments'
    endpoint="invoices/{}/payments"
    parent = "invoices"
    parent_id = "id"
    date_fields = ["send_reminder_on"]

    def add_field_at_1st_level(self, row=None, parent_row=None):
        row['payment_gateway_id'] = row['payment_gateway']['id']
        row['payment_gateway_name'] = row['payment_gateway']['name']
        return row

class InvoiceLineItems(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoices/#the-invoice-line-item-object
    """
    tap_stream_id = 'invoice_line_items'
    parent = "invoices"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):
        schema, stream_metadata = self.get_schema_and_metadata(catalog)

        with Transformer() as transformer:
            for line_item in parent_row['line_items']:

                time_extracted = utils.now()

                line_item['invoice_id'] = parent_row['id']
                if line_item['project'] is not None:
                    line_item['project_id'] = line_item['project']['id']
                else:
                    line_item['project_id'] = None
                line_item = transformer.transform(line_item, schema, stream_metadata)

                singer.write_record("invoice_line_items", line_item, time_extracted=time_extracted)


class Invoices(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoices/#list-all-invoices
    """
    tap_stream_id = 'invoices'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    object_to_id = ['client', 'estimate', 'retainer', 'creator']
    children = ["invoice_messages", "invoice_payments", "invoice_line_items"]

class EstimateItemCategories(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-item-categories/#list-all-estimate-item-categories
    """
    tap_stream_id = 'estimate_item_categories'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class EstimateMessages(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/#list-all-messages-for-an-estimate
    """
    tap_stream_id = 'estimate_messages'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'estimate_messages'
    date_fields = ["send_reminder_on"]
    endpoint="estimates/{}/messages"
    parent = "estimates"
    parent_id = "id"

class EstimateLineItems(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/#the-estimate-line-item-object
    """
    tap_stream_id = 'estimate_line_items'
    parent = "estimates"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):
        schema, stream_metadata = self.get_schema_and_metadata(catalog)

        with Transformer() as transformer:
            for line_item in parent_row['line_items']:

                time_extracted = utils.now()
                line_item['estimate_id'] = parent_row['id']
                line_item = transformer.transform(line_item, schema, stream_metadata)


                singer.write_record(self.tap_stream_id, line_item, time_extracted=time_extracted)

class Estimates(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/#list-all-messages-for-an-estimate
    """
    tap_stream_id = 'estimates'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    date_fields = ["issue_date"]
    object_to_id = ['client', 'creator']
    children = ["estimate_messages", "estimate_line_items"]

class ExternalReferences(Stream):
    tap_stream_id = 'external_reference'
    parent = "time_entries"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):
        schema, stream_metadata = self.get_schema_and_metadata(catalog)

        if parent_row['external_reference']:
            with Transformer() as transformer:

                external_reference = parent_row['external_reference']
                time_extracted = utils.now()

                line_item = transformer.transform(external_reference, schema, stream_metadata)


                singer.write_record(self.tap_stream_id, line_item, time_extracted=time_extracted)

class TimeEntryExternalReferences(Stream):
    tap_stream_id = 'time_entry_external_reference'
    key_properties = ["time_entry_id", "external_reference_id"]
    parent = "time_entries"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state, tap_state, selected_streams, parent_row={}):
        schema, stream_metadata = self.get_schema_and_metadata(catalog)



        if parent_row['external_reference']:
            external_reference = parent_row['external_reference']
            time_extracted = utils.now()

            # Create pivot row for time_entry and external_reference
            pivot_row = {
                'time_entry_id': parent_row['id'],
                'external_reference_id': external_reference['id']
            }

            singer.write_record("time_entry_external_reference", pivot_row, time_extracted=time_extracted)

class TimeEntries(Stream):
    tap_stream_id = 'time_entries'
    replication_keys=["updated_at"]
    replication_method="INCREMENTAL"
    object_to_id = ['user', 'user_assignment', 'client', 'project', 'task',
                    'task_assignment', 'external_reference', 'invoice']
    children = ["external_reference", "time_entry_external_reference"]

STREAMS = {
    'clients': Clients,
    'contacts': Contacts,
    'user_roles': UserRoles,
    'roles': Roles,
    'projects': Projects,
    'tasks': Tasks,
    'project_tasks': ProjectTasks,
    'project_users': ProjectUsers,
    'user_project_tasks': UserProjectTasks,
    'user_projects': UserProjects,
    'users': Users,
    'expense_categories': ExpenseCategories,
    'expenses': Expenses,
    'invoice_item_categories': InvoiceItemCategories,
    'invoice_messages': InvoiceMessages,
    'invoice_payments': InvoicePayments,
    'invoice_line_items': InvoiceLineItems,
    'invoices': Invoices,
    'estimate_item_categories': EstimateItemCategories,
    'estimate_messages': EstimateMessages,
    'estimate_line_items': EstimateLineItems,
    'estimates': Estimates,
    'external_reference': ExternalReferences,
    'time_entry_external_reference': TimeEntryExternalReferences,
    'time_entries': TimeEntries
}