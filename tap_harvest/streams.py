from datetime import datetime
import singer
import copy
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

    # Loop through all keys of the schema
    for key in schema['properties']:
        subschema = schema['properties'][key]

        # Append key with datatype date-time into the `fields` list
        if subschema.get('format') == 'date-time':
            fields.append(key)

    for field in fields:
        # Remove field with datatype date-time from the record if the value is None
        if item.get(field) is None:
            del item[field]


def get_url(endpoint):
    """
    Return URL which contains BASE_API_URL and endpoint path itself.
    """
    return BASE_API_URL + endpoint


def append_times_to_dates(item, date_fields):
    """
    Convert the date field into the standard format
    For example: 2021-02-02 to 2021-02-02T00:00:00Z
    """
    if date_fields:
        for date_field in date_fields:
            if item.get(date_field):
                item[date_field] = utils.strftime(utils.strptime_with_tz(item[date_field]))


def get_bookmark(stream_name, state, default):
    """
    Return bookmark value if available in the state otherwise return start date
    """
    if state is None:
        return default
    return state.get(stream_name, default)


class Stream:
    """
    A base class representing tap-harvest streams
    """
    tap_stream_id = None
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ["id"]
    object_to_id = []
    parent = ""
    parent_id_key = ""
    parent_id = None
    with_updated_since = True
    children = []
    endpoint = None
    path = None
    date_fields = None

    def add_field_at_1st_level(self, row=None):
        """Method to add fields at first level."""
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
        if stream in selected_streams and stream_obj.replication_keys:
            # Get minimum of stream's bookmark(start date in case of no bookmark) and min_bookmark
            if stream_obj.parent:
                stream_name_in_state = stream+'_parent'
            else:
                stream_name_in_state = stream
            min_bookmark = min(min_bookmark, get_bookmark(
                stream_name_in_state, state, start_date))
            LOGGER.debug("New minimum bookmark is %s", min_bookmark)

        for child in stream_obj.children:
            # Iterate through all children and return minimum bookmark among all.
            min_bookmark = min(min_bookmark,
                               self.get_min_bookmark(child, selected_streams, min_bookmark, start_date, state))

        return min_bookmark

    def get_schema_and_metadata(self, catalog):
        """
        Return schema and metadata data of given stream.
        """
        stream = catalog.get_stream(self.tap_stream_id)
        schema = stream.schema.to_dict()  # Get schema
        stream_metadata = metadata.to_map(stream.metadata)  # Get metadata

        return schema, stream_metadata

    def convert_object_to_id(self, row):
        """
        Take the id of the nested object and store it into a separate field ended with _id
        For example, row = {'client': {'id': 1, 'name': 'client_1'}}
        This function updates the row as below,
        row = {'client': {'id': 1, 'name': 'client_1'}, 'client_id': 1}
        """
        if self.object_to_id is not None:
            # Loop through each key of the object_to_id list.
            for key in self.object_to_id:
                key_object = row.get(key)
                if key_object:
                    row[key + '_id'] = key_object.get('id')
                else:
                    row[key + '_id'] = None

    def sync_endpoint(self,
                      client,
                      catalog,
                      config,
                      state,
                      tap_state,
                      selected_streams,
                      parent_row=None):
        """
        A common function to sync incremental streams.
        """
        # Retrieve schema and metadata of stream from the catalog
        schema, stream_metadata = self.get_schema_and_metadata(catalog)
        parent_row = parent_row or {}

        bookmark_field = next(iter(self.replication_keys))\
                            if self.replication_keys else None
        children = self.children

        current_time = datetime.now().strftime(DATE_FORMAT)
        min_bookmark_among_parent_child = self.get_min_bookmark(self.tap_stream_id,
                                                                selected_streams,
                                                                current_time,
                                                                config['start_date'],
                                                                state)

        # Get the latest bookmark for the stream and set the last_datetime
        last_datetime = get_bookmark(self.tap_stream_id, state, config['start_date'])

        with Transformer() as transformer:
            page = 1
            # Loop until the last page.
            while page:

                # Add parent_id in the URL to get records of child stream.
                url = get_url(self.endpoint or self.tap_stream_id).format(
                    parent_row.get(self.parent_id))

                params = {}
                if self.with_updated_since:
                    params = {"updated_since": min_bookmark_among_parent_child}
                params['page'] = page

                # Call API to fetch the records
                response = client.request(url, params)
                path = self.path or self.tap_stream_id
                data = response[path]
                time_extracted = utils.now()
                for row in data:
                    # Add fields at 1st level explicitly
                    row = self.add_field_at_1st_level(row=row)
                    self.convert_object_to_id(row)

                    if self.parent_id:
                        # Remove the last character `s` from the parent stream name and
                        # join it with `_id` to save the parent id in the child record.
                        # For example if a parent is `invoices`, then save parent id in
                        # invoice_id key to the child.
                        row[self.parent_id_key] = parent_row.get(self.parent_id)

                    # Remove empty date-time fields from the record.
                    remove_empty_date_times(row, schema)

                    parent_row = copy.deepcopy(row)
                    transformed_record = transformer.transform(row, schema, stream_metadata)

                    # Convert date field into the standard format
                    append_times_to_dates(transformed_record, self.date_fields)

                    if self.tap_stream_id in selected_streams:
                        # Write the record of a parent if it is selected.
                        singer.write_record(self.tap_stream_id,
                                            transformed_record,
                                            time_extracted=time_extracted)

                    # Loop thru parent batch records for each child's objects
                    for child_stream_name in children:
                        # Sync child stream if it is selected.
                        child_obj = STREAMS[child_stream_name]()
                        child_obj.sync_endpoint(client, catalog, config, state,
                                                tap_state, selected_streams, parent_row)

                    if bookmark_field:
                        # Get replication key value from the record for the incremental stream
                        bookmark_dttm = transformed_record[bookmark_field]
                        if bookmark_dttm > last_datetime:
                            # Update last_datetime if it is less than the current replication key value
                            last_datetime = bookmark_dttm

                        if self.tap_stream_id in selected_streams:
                            # Update bookmark for parent stream if it is selected only.
                            utils.update_state(tap_state, self.tap_stream_id, bookmark_dttm)

                page = response['next_page']

        # Loop through all children
        for child_stream_name in children:
            # Sync child stream if it is selected.
            child_obj = STREAMS[child_stream_name]()
            # Write bookmark if child stream is selected and incremental
            if child_stream_name in selected_streams and child_obj.replication_keys:
                # Update the bookmark of the parent into the following name pattern: `{child_stream_name}_parent`
                # For example, update the bookmark for invoice_meesages to the `invoice_messages_parent` key.
                utils.update_state(tap_state, child_stream_name+'_parent', last_datetime)
                if child_stream_name in tap_state and tap_state[child_stream_name] > current_time:
                    # Reset the child stream's bookmark to current_time if max_bookmark is greater than current_time.
                    tap_state[child_stream_name] = current_time

        singer.write_state(tap_state)


class Clients(Stream):
    """
    https://help.getharvest.com/api-v2/clients-api/clients/clients/#list-all-clients
    """
    tap_stream_id = "clients"
    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"


class Contacts(Stream):
    """
    https://help.getharvest.com/api-v2/clients-api/clients/contacts/#list-all-contacts
    """
    tap_stream_id = 'contacts'
    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"
    object_to_id = ['client']


class UserRoles(Stream):
    """
    https://help.getharvest.com/api-v2/users-api/users/users/
    """
    tap_stream_id = 'user_roles'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    key_properties = ["user_id", "role_id"]
    parent = "roles"

    def sync_endpoint(self, client, catalog, config, state,
                      tap_state, selected_streams, parent_row=None):
        """
        Prepare a record of the user_roles stream using the parent record's fields.
        """
        if self.tap_stream_id in selected_streams:
            # Retrieve schema and metadata of stream from the catalog
            schema, stream_metadata = self.get_schema_and_metadata(catalog)

            with Transformer() as transformer:
                # Loop through all records of the parent
                for user_id in parent_row['user_ids']:
                    time_extracted = utils.now()

                    pivot_row = {
                        'updated_at': parent_row['updated_at'],
                        'role_id': parent_row['id'],
                        'user_id': user_id
                    }
                    transformed_record = transformer.transform(pivot_row, schema, stream_metadata)
                    singer.write_record("user_roles", transformed_record, time_extracted=time_extracted)


class Roles(Stream):
    """
    https://help.getharvest.com/api-v2/roles-api/roles/roles/#list-all-roles
    """
    tap_stream_id = 'roles'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    children = ["user_roles"]


class Projects(Stream):
    """
    https://help.getharvest.com/api-v2/projects-api/projects/projects/#list-all-projects
    """
    tap_stream_id = 'projects'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    object_to_id = ['client']


class Tasks(Stream):
    """
    https://help.getharvest.com/api-v2/tasks-api/tasks/tasks/#list-all-tasks
    """
    tap_stream_id = 'tasks'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]


class ProjectTasks(Stream):
    """
    https://help.getharvest.com/api-v2/projects-api/projects/task-assignments/#list-all-task-assignments
    """
    tap_stream_id = 'project_tasks'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    endpoint = 'task_assignments'
    path = 'task_assignments'
    object_to_id = ['project', 'task']


class ProjectUsers(Stream):
    """
    https://help.getharvest.com/api-v2/projects-api/projects/user-assignments/
    """
    tap_stream_id = 'project_users'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = 'user_assignments'
    object_to_id = ['project', 'user']
    endpoint = 'user_assignments'


class UserProjectTasks(Stream):
    """
    The user_project_tasks table contains pairs of user IDs and project task IDs.
    This stream is updated based on new and updated users.
    """
    tap_stream_id = 'user_project_tasks'
    key_properties = ["user_id", "project_task_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    parent = "users"
    parent_id_key = "user_id"
    parent_id = 'id'

    def sync_endpoint(self, client, catalog, config, state,
                      tap_state, selected_streams, parent_row=None):
        """
        Prepare a record of the `user_project_tasks` stream using the parent record's fields.
        """
        if self.tap_stream_id in selected_streams:
            # Retrieve schema and metadata of stream from the catalog
            schema, stream_metadata = self.get_schema_and_metadata(catalog)
            with Transformer() as transformer:
                user_id = parent_row['user_id']
                # Loop through all records of the parent
                for project_task in parent_row['task_assignments']:

                    time_extracted = utils.now()
                    pivot_row = {
                        'updated_at': parent_row['updated_at'],
                        'user_id': user_id,
                        'project_task_id': project_task['id']
                    }

                    transformed_record = transformer.transform(pivot_row, schema, stream_metadata)
                    singer.write_record(self.tap_stream_id, transformed_record,
                                        time_extracted=time_extracted)


class UserProjects(Stream):
    """
    https://help.getharvest.com/api-v2/users-api/users/project-assignments/
    """
    tap_stream_id = 'user_projects'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = 'project_assignments'
    endpoint = "users/{}/project_assignments"
    parent = "users"
    parent_id_key = "user_id"
    parent_id = 'id'
    object_to_id = ['project', 'client', 'user']
    children = ['user_project_tasks']


class Users(Stream):
    """
    https://help.getharvest.com/api-v2/users-api/users/users/#list-all-users
    """
    tap_stream_id = 'users'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    children = ["user_projects"]


class ExpenseCategories(Stream):
    """
    https://help.getharvest.com/api-v2/expenses-api/expenses/expense-categories/#list-all-expense-categories
    """
    tap_stream_id = 'expense_categories'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]


class Expenses(Stream):
    """
    https://help.getharvest.com/api-v2/expenses-api/expenses/expenses/#list-all-expenses
    """
    tap_stream_id = 'expenses'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    object_to_id = ['client', 'project', 'expense_category',
                    'user', 'user_assignment', 'invoice']

    def add_field_at_1st_level(self, row=None):
        """
        Add fields at 1st level explicitly
        """
        if row['receipt'] is None:
            row['receipt_url'] = None
            row['receipt_file_name'] = None
            row['receipt_file_size'] = None
            row['receipt_content_type'] = None
        else:
            row['receipt_url'] = row['receipt']['url']
            row['receipt_file_name'] = row['receipt']['file_name']
            row['receipt_file_size'] = row['receipt']['file_size']
            row['receipt_content_type'] = row['receipt']['content_type']
        return row


class InvoiceItemCategories(Stream):
    """
    https://help.getharvest.com/api-v1/invoices-api/invoices/invoice-messages-payments/#show-all-categories
    """
    tap_stream_id = 'invoice_item_categories'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]


class InvoiceMessages(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-messages/
    """
    tap_stream_id = 'invoice_messages'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    endpoint = "invoices/{}/messages"
    path = 'invoice_messages'
    parent = "invoices"
    parent_id_key = "invoice_id"
    parent_id = "id"


class InvoicePayments(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoice-payments/#list-all-payments-for-an-invoice
    """
    tap_stream_id = 'invoice_payments'
    path = 'invoice_payments'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    endpoint = "invoices/{}/payments"
    parent = "invoices"
    parent_id_key = "invoice_id"
    parent_id = "id"
    date_fields = ["send_reminder_on"]

    def add_field_at_1st_level(self, row=None):
        """
        Add fields at 1st level explicitly
        """
        row['payment_gateway_id'] = row['payment_gateway'].get('id')
        row['payment_gateway_name'] = row['payment_gateway'].get('name')
        return row


class InvoiceLineItems(Stream):
    """
    https://help.getharvest.com/api-v2/invoices-api/invoices/invoices/#the-invoice-line-item-object
    """
    tap_stream_id = 'invoice_line_items'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    parent = "invoices"
    parent_id_key = "invoice_id"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state,
                      tap_state, selected_streams, parent_row=None):
        """
        Prepare a record of the `invoice_line_items` stream using the parent record's fields
        """
        if self.tap_stream_id in selected_streams:
            schema, stream_metadata = self.get_schema_and_metadata(catalog)

            with Transformer() as transformer:
                # Loop through all records of the parent
                for line_item in parent_row['line_items']:
                    time_extracted = utils.now()

                    # Add parent replication-key and id
                    line_item['updated_at'] = parent_row['updated_at']
                    line_item[self.parent_id_key] = parent_row['id']
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
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    object_to_id = ['client', 'estimate', 'retainer', 'creator']
    children = ["invoice_messages", "invoice_payments", "invoice_line_items"]


class EstimateItemCategories(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-item-categories/#list-all-estimate-item-categories
    """
    tap_stream_id = 'estimate_item_categories'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]


class EstimateMessages(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/#list-all-messages-for-an-estimate
    """
    tap_stream_id = 'estimate_messages'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    path = 'estimate_messages'
    date_fields = ["send_reminder_on"]
    endpoint = "estimates/{}/messages"
    parent = "estimates"
    parent_id_key = "estimate_id"
    parent_id = "id"


class EstimateLineItems(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimates/#the-estimate-line-item-object
    """
    tap_stream_id = 'estimate_line_items'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    parent = "estimates"
    parent_id_key = "estimate_id"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state,
                      tap_state, selected_streams, parent_row=None):
        """
        Prepare a record of the `estimate_line_items` stream using the parent record's fields.
        """
        if self.tap_stream_id in selected_streams:
            schema, stream_metadata = self.get_schema_and_metadata(catalog)

            with Transformer() as transformer:
                # Loop through all records of the parent
                for line_item in parent_row['line_items']:
                    time_extracted = utils.now()

                    # Add parent replication-key and id
                    line_item['updated_at'] = parent_row['updated_at']
                    line_item['estimate_id'] = parent_row['id']
                    line_item = transformer.transform(line_item, schema, stream_metadata)

                    singer.write_record(self.tap_stream_id, line_item, time_extracted=time_extracted)


class Estimates(Stream):
    """
    https://help.getharvest.com/api-v2/estimates-api/estimates/estimate-messages/#list-all-messages-for-an-estimate
    """
    tap_stream_id = 'estimates'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    date_fields = ["issue_date"]
    object_to_id = ['client', 'creator']
    children = ["estimate_messages", "estimate_line_items"]


class ExternalReferences(Stream):
    """
    The external_references table contains info about external references.
    This stream is updated based on new and updated `time_entries`.
    """
    tap_stream_id = 'external_reference'
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    parent = "time_entries"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state,
                      tap_state, selected_streams, parent_row=None):
        """
        Prepare a record of the `external_reference` stream using the parent record's fields.
        """
        schema, stream_metadata = self.get_schema_and_metadata(catalog)

        if self.tap_stream_id in selected_streams and parent_row['external_reference']:
            with Transformer() as transformer:
                time_extracted = utils.now()

                # Create record for external_reference
                external_reference = parent_row['external_reference']
                # Add parent replication-key
                external_reference['updated_at'] = parent_row['updated_at']

                transformed_external_reference = transformer.transform(external_reference, schema, stream_metadata)
                singer.write_record(self.tap_stream_id, transformed_external_reference, time_extracted=time_extracted)


class TimeEntryExternalReferences(Stream):
    """
    The `time_entry_external_reference` table contains pairs of time entry IDs and external reference IDs.
    This stream is updated based on new and updated `time_entries`.
    """
    tap_stream_id = 'time_entry_external_reference'
    key_properties = ["time_entry_id", "external_reference_id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["updated_at"]
    parent = "time_entries"
    parent_id = "id"

    def sync_endpoint(self, client, catalog, config, state,
                      tap_state, selected_streams, parent_row=None):
        """
        Prepare a record of the `time_entry_external_reference` stream using
        the parent record's fields.
        """
        if self.tap_stream_id in selected_streams and parent_row['external_reference']:
            external_reference = parent_row['external_reference']
            time_extracted = utils.now()

            # Create a record for time_entry
            pivot_row = {
                'updated_at': parent_row['updated_at'],
                'time_entry_id': parent_row['id'],
                'external_reference_id': external_reference['id']
            }

            singer.write_record("time_entry_external_reference", pivot_row,
                                time_extracted=time_extracted)


class TimeEntries(Stream):
    """
    https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries#list-all-time-entries
    """
    tap_stream_id = 'time_entries'
    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"
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
