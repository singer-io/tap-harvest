import singer
import pendulum
import copy
from singer import Transformer, utils

from tap_harvest.client import HarvestClient

LOGGER = singer.get_logger()
BASE_API_URL = "https://api.harvestapp.com/v2/"

# Any date-times values can either be a string or a null.
# If null, parsing the date results in an error.
# Instead, removing the attribute before parsing ignores this error.
def remove_empty_date_times(item, schema):
    fields = []

    for key in schema['properties']:
        subschema = schema['properties'][key]
        if subschema.get('format') == 'date-time':
            fields.append(key)

    for field in fields:
        if item.get(field) is None:
            del item[field]


def append_times_to_dates(item, date_fields):
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] = utils.strftime(utils.strptime_with_tz(item[date_field]))

def get_url(endpoint):
    return BASE_API_URL + endpoint

def get_start(stream, config, state):

    if stream not in state:
        return config['start_date']

    return state[stream]


class BaseStream:
    tap_stream_id = None
    replication_method = 'INCREMENTAL'
    bookmark_key = 'updated_at'
    key_properties = ['id']
    valid_replication_keys = ['updated_at']
    endpoint = None
    path = None
    date_fields = None
    with_updated_since = True
    for_each_handler = None
    map_handler = None
    object_to_id = None

    def __init__(self, client: HarvestClient):
        self.client = client

    def sync_endpoint(self, schema, mdata, config, state, tap_state):

        singer.write_schema(self.tap_stream_id,
                            schema,
                            self.key_properties,
                            self.valid_replication_keys)

        start = get_start(self.tap_stream_id, config, state)

        start_dt = pendulum.parse(start)
        updated_since = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        with Transformer() as transformer:
            page = 1
            while page is not None:
                url = get_url(self.endpoint or self.tap_stream_id)
                params = {"updated_since": updated_since} if self.with_updated_since else {}
                params['page'] = page
                response = self.client.request(url, params)
                path = self.path or self.tap_stream_id
                data = response[path]
                time_extracted = utils.now()

                for row in data:
                    if self.map_handler is not None:
                        row = self.map_handler(row) #pylint: disable=not-callable

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
                    item = transformer.transform(row, schema_copy, mdata)

                    append_times_to_dates(item, self.date_fields)

                    if item[self.bookmark_key] >= start:
                        singer.write_record(self.tap_stream_id,
                                            item,
                                            time_extracted=time_extracted)

                        # take any additional actions required for the currently loaded endpoint
                        if self.for_each_handler is not None:
                            self.for_each_handler(row, time_extracted=time_extracted) #pylint: disable=not-callable

                        utils.update_state(tap_state, self.tap_stream_id, item[self.bookmark_key])
                page = response['next_page']

        singer.write_state(tap_state)

    def sync(self, schema, mdata, config, state, tap_state):
        self.sync_endpoint(schema, mdata, config, state, tap_state)

class Clients(BaseStream):
    tap_stream_id = 'clients'

class Contacts(BaseStream):
    tap_stream_id = 'contacts'
    object_to_id = ['client']

class UserRoles(BaseStream):
    tap_stream_id = 'user_roles'
    key_properties = ["user_id", "role_id"]
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync_user_roles(self, user_roles_schema, role, time_extracted):
        # Extract user_roles
        singer.write_schema('user_roles',
                            user_roles_schema,
                            self.key_properties)

        for user_id in role['user_ids']:
            pivot_row = {
                'role_id': role['id'],
                'user_id': user_id
            }

            singer.write_record("user_roles",
                                pivot_row,
                                time_extracted=time_extracted)

class Roles(BaseStream):
    tap_stream_id = 'roles'

    def sync(self, schema, mdata, config, state, tap_state):
        def for_each_role(role, time_extracted):

            if schema.get('user_roles'):
                # Extract user_roles
                user_roles_obj = UserRoles(self.client)
                user_roles_obj.sync_user_roles(schema.get('user_roles'), role, time_extracted)

        self.for_each_handler = for_each_role
        self.sync_endpoint(schema.get('roles'), mdata.get('roles'), config, state, tap_state)

class Projects(BaseStream):
    tap_stream_id = 'projects'
    object_to_id = ['client']

class Tasks(BaseStream):
    tap_stream_id = 'tasks'

class ProjectTasks(BaseStream):
    tap_stream_id = 'project_tasks'
    endpoint = 'task_assignments'
    path = 'task_assignments'
    object_to_id = ['project', 'task']

class ProjectUsers(BaseStream):
    tap_stream_id = 'project_users'
    endpoint = 'user_assignments'
    path = 'user_assignments'
    object_to_id = ['project', 'user']

class UserProjectTasks(BaseStream):
    tap_stream_id = 'user_project_tasks'
    key_properties = ["user_id", "project_task_id"]
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync_tasks(self, schema, user, user_project_assignment, time_extracted):
        # Extract user_project_tasks
        singer.write_schema('user_project_tasks',
                            schema,
                            self.key_properties)

        for project_task in user_project_assignment['task_assignments']:
            pivot_row = {
                'user_id': user['id'],
                'project_task_id': project_task['id']
            }

            singer.write_record("user_project_tasks",
                                pivot_row,
                                time_extracted=time_extracted)

class UserProjects(BaseStream):
    tap_stream_id = 'user_projects'
    path = 'project_assignments'
    with_updated_since=False
    object_to_id = ['project', 'client', 'user']

class Users(BaseStream):
    tap_stream_id = 'users'

    def sync(self, schema, mdata, config, state, tap_state):
        def for_each_user(user, time_extracted): #pylint: disable=unused-argument
            def map_user_projects(project_assignment):
                project_assignment['user'] = user
                return project_assignment

            def for_each_user_project(user_project_assignment, time_extracted):
                if schema.get('user_project_tasks'):
                # Extract user_project_tasks
                    project_task_obj = UserProjectTasks(self.client)
                    project_task_obj.sync_tasks(schema.get('user_project_tasks'),
                                                user,
                                                user_project_assignment,
                                                time_extracted)

            if schema.get('user_projects'):
                # Sync User Projects
                user_projects_obj = UserProjects(self.client)
                user_projects_obj.map_handler = map_user_projects
                user_projects_obj.for_each_handler = for_each_user_project
                user_projects_obj.endpoint = f"users/{user['id']}/project_assignments"
                user_projects_obj.sync(schema.get('user_projects'),
                                       mdata.get('user_projects'),
                                       config,
                                       state,
                                       tap_state)

        self.for_each_handler = for_each_user
        self.sync_endpoint(schema.get('users'), mdata.get('users'), config, state, tap_state)

class ExpenseCategories(BaseStream):
    tap_stream_id = 'expense_categories'

class Expenses(BaseStream):
    tap_stream_id = 'expenses'
    object_to_id =['client', 'project', 'expense_category', 'user', 'user_assignment', 'invoice']

    def sync(self, schema, mdata, config, state, tap_state):
        def map_expense(expense):
            if expense['receipt'] is None:
                expense['receipt_url'] = None
                expense['receipt_file_name'] = None
                expense['receipt_file_size'] = None
                expense['receipt_content_type'] = None
            else:
                expense['receipt_url'] = expense['receipt']['url']
                expense['receipt_file_name'] = expense['receipt']['file_name']
                expense['receipt_file_size'] = expense['receipt']['file_size']
                expense['receipt_content_type'] = expense['receipt']['content_type']
            return expense

        self.map_handler = map_expense
        self.sync_endpoint(schema, mdata, config, state, tap_state)

class InvoiceItemCategories(BaseStream):
    tap_stream_id = 'invoice_item_categories'

class InvoiceMessages(BaseStream):
    tap_stream_id = 'invoice_messages'
    path = 'invoice_messages'
    with_updated_since = False

class InvoicePayments(BaseStream):
    tap_stream_id = 'invoice_payments'
    path = 'invoice_payments'
    with_updated_since = False
    date_fields = ["send_reminder_on"]
    key_properties = ['id']

class InvoiceLineItems(BaseStream):
    tap_stream_id = 'invoice_line_items'
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync_line_items(self, line_items_schema, line_items_mdata, invoice, time_extracted):
        singer.write_schema('invoice_line_items',
                            line_items_schema,
                            self.key_properties)

        with Transformer() as transformer:
            for line_item in invoice['line_items']:
                line_item['invoice_id'] = invoice['id']
                if line_item['project'] is not None:
                    line_item['project_id'] = line_item['project']['id']
                else:
                    line_item['project_id'] = None
                schema_copy = copy.deepcopy(line_items_schema)
                line_item = transformer.transform(line_item, schema_copy, line_items_mdata)

                singer.write_record("invoice_line_items",
                                    line_item,
                                    time_extracted=time_extracted)

class Invoices(BaseStream):
    tap_stream_id = 'invoices'
    object_to_id = ['client', 'estimate', 'retainer', 'creator']

    def sync(self, schema, mdata, config, state, tap_state):
        def for_each_invoice(invoice, time_extracted):
            def map_invoice_message(message):
                message['invoice_id'] = invoice['id']
                return message

            def map_invoice_payment(payment):
                payment['invoice_id'] = invoice['id']
                payment['payment_gateway_id'] = payment['payment_gateway']['id']
                payment['payment_gateway_name'] = payment['payment_gateway']['name']
                return payment

            if schema.get('invoice_messages'):
                # Sync invoice messages
                invoice_message_obj = InvoiceMessages(self.client)
                invoice_message_obj.map_handler = map_invoice_message
                invoice_message_obj.endpoint = f"invoices/{invoice['id']}/messages"
                invoice_message_obj.sync(schema.get('invoice_messages'),
                                         mdata.get('invoice_messages'),
                                         config,
                                         state,
                                         tap_state)

            if schema.get('invoice_payments'):
                # Sync invoice payments
                invoice_payment_obj = InvoicePayments(self.client)
                invoice_payment_obj.map_handler = map_invoice_payment
                invoice_payment_obj.endpoint = f"invoices/{invoice['id']}/payments"
                invoice_payment_obj.sync(schema.get('invoice_payments'),
                                         mdata.get('invoice_payments'),
                                         config,
                                         state,
                                         tap_state)

            if schema.get('invoice_line_items'):
                # Extract all invoice_line_items
                invoice_line_items_obj = InvoiceLineItems(self.client)
                invoice_line_items_obj.sync_line_items(schema.get('invoice_line_items'),
                                                       mdata.get('invoice_line_items'),
                                                       invoice,
                                                       time_extracted)

        self.for_each_handler = for_each_invoice
        self.sync_endpoint(schema.get('invoices'), mdata.get('invoices'), config, state, tap_state)

class EstimateItemCategories(BaseStream):
    tap_stream_id = 'estimate_item_categories'

class EstimateMessages(BaseStream):
    tap_stream_id = 'estimate_messages'
    path = 'estimate_messages'
    with_updated_since = False
    date_fields = ["send_reminder_on"]

class EstimateLineItems(BaseStream):
    tap_stream_id = 'estimate_line_items'
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync_line_items(self, line_items_schema, line_items_mdata, estimate, time_extracted):

        singer.write_schema('estimate_line_items',
                            line_items_schema,
                            self.key_properties)

        with Transformer() as transformer:
            for line_item in estimate['line_items']:
                line_item['estimate_id'] = estimate['id']
                schema_copy = copy.deepcopy(line_items_schema)
                line_item = transformer.transform(line_item, schema_copy, line_items_mdata)

                singer.write_record("estimate_line_items",
                                    line_item,
                                    time_extracted=time_extracted)

class Estimates(BaseStream):
    tap_stream_id = 'estimates'
    date_fields = ["issue_date"]
    object_to_id = ['client', 'creator']

    def sync(self, schema, mdata, config, state, tap_state):
        def for_each_estimate(estimate, time_extracted):
            # create "estimate_id" field in the child stream records
            # and set estimate id as value
            def map_estimate_message(message):
                message['estimate_id'] = estimate['id']
                return message

            if schema.get('estimate_messages'):
                estimate_message_obj = EstimateMessages(self.client)
                estimate_message_obj.map_handler = map_estimate_message
                estimate_message_obj.endpoint = f"estimates/{estimate['id']}/messages"
                estimate_message_obj.sync(schema.get('estimate_messages'),
                                          mdata.get('estimate_messages'),
                                          config,
                                          state,
                                          tap_state)

            if schema.get('estimate_line_items'):
                estimate_line_items_obj = EstimateLineItems(self.client)
                estimate_line_items_obj.sync_line_items(schema.get('estimate_line_items'),
                                                        mdata.get('estimate_line_items'),
                                                        estimate,
                                                        time_extracted)

        self.for_each_handler=for_each_estimate
        self.sync_endpoint(schema.get('estimates'),
                           mdata.get('estimates'),
                           config,
                           state,
                           tap_state)

class ExternalReferences(BaseStream):
    tap_stream_id = 'external_reference'
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync_references(self, external_reference_schema, ref_mdata, time_entry, time_extracted):
        singer.write_schema('external_reference',
                            external_reference_schema,
                            self.key_properties)

        with Transformer() as transformer:
            external_reference = time_entry['external_reference']
            schema_copy = copy.deepcopy(external_reference_schema)
            external_reference = transformer.transform(external_reference,
                                                       schema_copy,
                                                       ref_mdata)

            singer.write_record("external_reference",
                                external_reference,
                                time_extracted=time_extracted)

class TimeEntryExternalReferences(BaseStream):
    tap_stream_id = 'time_entry_external_reference'
    key_properties = ["time_entry_id", "external_reference_id"]
    replication_method = 'FULL_TABLE'
    valid_replication_keys = []

    def sync_references(self, external_reference_schema, time_entry, time_extracted):
        singer.write_schema('time_entry_external_reference',
                            external_reference_schema,
                            self.key_properties)

        external_reference = time_entry['external_reference']

        # Create pivot row for time_entry and external_reference
        pivot_row = {
            'time_entry_id': time_entry['id'],
            'external_reference_id': external_reference['id']
        }

        singer.write_record("time_entry_external_reference",
                            pivot_row,
                            time_extracted=time_extracted)

class TimeEntries(BaseStream):
    tap_stream_id = 'time_entries'
    object_to_id = ['user', 'user_assignment', 'client', 'project', 'task',
                    'task_assignment', 'external_reference', 'invoice']

    def sync(self, schema, mdata, config, state, tap_state):
        def for_each_time_entry(time_entry, time_extracted):
            # Extract external_reference

            if time_entry['external_reference'] is not None:

                if schema.get('external_reference'):
                    external_reference_obj = ExternalReferences(self.client)
                    external_reference_obj.sync_references(schema.get('external_reference'),
                                                           mdata.get('external_reference'),
                                                           time_entry,
                                                           time_extracted)

                if schema.get('time_entry_external_reference'):
                    time_ext_ref_obj = TimeEntryExternalReferences(self.client)
                    time_ext_ref_obj.sync_references(schema.get('time_entry_external_reference'),
                                                     time_entry,
                                                     time_extracted)

        self.for_each_handler = for_each_time_entry
        self.sync_endpoint(schema.get('time_entries'),
                           mdata.get('time_entries'),
                           config,
                           state,
                           tap_state)


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
