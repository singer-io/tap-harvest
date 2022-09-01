import singer
import pendulum
import copy
from singer import Transformer, utils, metadata

LOGGER = singer.get_logger()
BASE_API_URL = "https://api.harvestapp.com/v2/"

def remove_empty_date_times(item, schema):
    # Any date-times values can either be a string or a null.
    # If null, parsing the date results in an error.
    # Instead, removing the attribute before parsing ignores this error.
    fields = []

    for key in schema['properties']:
        subschema = schema['properties'][key]
        if subschema.get('format') == 'date-time':
            fields.append(key)

    for field in fields:
        if item.get(field) is None:
            del item[field]

def get_url(endpoint):
    return BASE_API_URL + endpoint

def append_times_to_dates(item, date_fields):
    # Convert date field into the standard format
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] = utils.strftime(utils.strptime_with_tz(item[date_field]))

class Stream:
    tap_stream_id = None
    replication_method="FULL_TABLE"
    replication_keys=[]
    key_properties=["id"]
    object_to_id = None
    foreign_key = None
    path = None
    parent = None
    data_key = None
    children = []
    count = None
    params = {}


    def get_bookmark(self, state, default):
        """
        Return bookmark value if available in the state otherwise return start date
        """
        if (state is None) or ('bookmarks' not in state):
            return default
        return (
            state
            .get('bookmarks', {})
            .get(self.tap_stream_id, default)
        )

    def sync_endpoint(self, client, catalog, config, state, selected_streams, parent_id):

        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)

        # Write schema for streams
        singer.write_schema(self.tap_stream_id, catalog, self.key_properties, self.replication_keys)

        # Get the latest bookmark for the stream and set the last_datetime
        last_datetime = self.get_bookmark(state, config['start_date'])
        start_dt = pendulum.parse(last_datetime)
        updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        with Transformer() as transformer:
            page = 1
            while page is not None:
                url = self.get_url(self.endpoint or self.tap_stream_id)
                params = {"updated_since": updated_since, 'page': page}
                
                response = self.client.request(url, params)
                path = self.path or self.tap_stream_id
                data = response[path]
                time_extracted = utils.now()

                for row in data:
                    
                    if self.object_to_id is not None:
                        for key in self.object_to_id:
                            if row[key] is not None:
                                row[key + '_id'] = row[key]['id']
                            else:
                                row[key + '_id'] = None

                    remove_empty_date_times(row, schema)

                    # Transform is changing order of elements inside list of 'types
                    # so passing copy of schema
                    schema_copy = copy.deepcopy(schema)
                    item = transformer.transform(row, schema_copy, stream_metadata)

                    append_times_to_dates(item, self.date_fields)
                    singer.write_record(self.tap_stream_id, item, time_extracted=time_extracted)

            page = response['next_page']

        #singer.write_state(tap_state)


class Clients(Stream):
    tap_stream_id="clients"
    replication_keys=["updated_at"]
    replication_method="INCREMENTAL"

class Contacts(Stream):
    tap_stream_id = 'contacts'
    replication_keys=["updated_at"]
    replication_method="INCREMENTAL"
    object_to_id = ['client']

class UserRoles(Stream):
    tap_stream_id = 'user_roles'
    key_properties = ["user_id", "role_id"]

class Roles(Stream):
    tap_stream_id = 'roles'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

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
    endpoint = 'user_assignments'
    path = 'user_assignments'
    object_to_id = ['project', 'user']

class UserProjectTasks(Stream):
    tap_stream_id = 'user_project_tasks'
    key_properties = ["user_id", "project_task_id"]

class UserProjects(Stream):
    tap_stream_id = 'user_projects'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'project_assignments'
    with_updated_since=False
    object_to_id = ['project', 'client', 'user']

class Users(Stream):
    tap_stream_id = 'users'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class ExpenseCategories(Stream):
    tap_stream_id = 'expense_categories'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class Expenses(Stream):
    tap_stream_id = 'expenses'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    object_to_id =['client', 'project', 'expense_category', 'user', 'user_assignment', 'invoice']

class InvoiceItemCategories(Stream):
    tap_stream_id = 'invoice_item_categories'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class InvoiceMessages(Stream):
    tap_stream_id = 'invoice_messages'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'invoice_messages'
    with_updated_since = False

class InvoicePayments(Stream):
    tap_stream_id = 'invoice_payments'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'invoice_payments'
    with_updated_since = False
    date_fields = ["send_reminder_on"]
    key_properties = ['id']

class InvoiceLineItems(Stream):
    tap_stream_id = 'invoice_line_items'

class Invoices(Stream):
    tap_stream_id = 'invoices'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    object_to_id = ['client', 'estimate', 'retainer', 'creator']

class EstimateItemCategories(Stream):
    tap_stream_id = 'estimate_item_categories'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]

class EstimateMessages(Stream):
    tap_stream_id = 'estimate_messages'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    path = 'estimate_messages'
    with_updated_since = False
    date_fields = ["send_reminder_on"]

class EstimateLineItems(Stream):
    tap_stream_id = 'estimate_line_items'

class Estimates(Stream):
    tap_stream_id = 'estimates'
    replication_method="INCREMENTAL"
    replication_keys=["updated_at"]
    date_fields = ["issue_date"]
    object_to_id = ['client', 'creator']

class ExternalReferences(Stream):
    tap_stream_id = 'external_reference'

class TimeEntryExternalReferences(Stream):
    tap_stream_id = 'time_entry_external_reference'
    key_properties = ["time_entry_id", "external_reference_id"]

class TimeEntries(Stream):
    tap_stream_id = 'time_entries'
    replication_keys=["updated_at"]
    replication_method="INCREMENTAL"
    object_to_id = ['user', 'user_assignment', 'client', 'project', 'task',
                    'task_assignment', 'external_reference', 'invoice']

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