import datetime
from unittest import mock
from singer import utils
from parameterized import parameterized
import unittest
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from tap_harvest.streams import remove_empty_date_times, append_times_to_dates, get_bookmark, Invoices, TimeEntries, Roles, Users, Expenses, Estimates
from tap_harvest.client import HarvestClient

CATALOG = Catalog(streams=[
    CatalogEntry(
        stream='invoices',
        tap_stream_id='invoices',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','updated_at']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','name']}
    ]),
    CatalogEntry(
        stream='invoice_messages',
        tap_stream_id='invoice_messages',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','updated_at']},
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','invoice_id']}
    ]),
    CatalogEntry(
        stream='invoice_payments',
        tap_stream_id='invoice_payments',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','updated_at']},
            {'metadata': {'inclusion': 'available'},'breadcrumb': ['properties','invoice_id']}
    ]),
    CatalogEntry(
        stream='invoice_line_items',
        tap_stream_id='invoice_line_items',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'available'},'breadcrumb': ['properties','invoice_id']},
            {'metadata': {'inclusion': 'available'},'breadcrumb': ['properties','project_id']}
    ]),
    CatalogEntry(
        stream='user_roles',
        tap_stream_id='user_roles',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),
                'name': Schema(type='string')}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','user_id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','role_id']}
    ]),
    CatalogEntry(
        stream='roles',
        tap_stream_id='roles',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
    ]),
    CatalogEntry(
        stream='users',
        tap_stream_id='users',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
    ]),
    CatalogEntry(
        stream='user_projects',
        tap_stream_id='user_projects',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
    ]),
    CatalogEntry(
        stream='user_project_tasks',
        tap_stream_id='user_project_tasks',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','user_id']},
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','project_task_id']}
    ]),
    CatalogEntry(
        stream='expenses',
        tap_stream_id='expenses',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']}
    ]),
    CatalogEntry(
        stream='estimates',
        tap_stream_id='estimates',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']}
    ]),
    CatalogEntry(
        stream='estimate_messages',
        tap_stream_id='estimate_messages',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
    ]),
    CatalogEntry(
        stream='estimate_line_items',
        tap_stream_id='estimate_line_items',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
            {'metadata': {'inclusion': 'available'},'breadcrumb': ['properties','estimate_id']},
    ]),
    CatalogEntry(
        stream='external_reference',
        tap_stream_id='external_reference',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
    ]),
    CatalogEntry(
        stream='time_entry_external_reference',
        tap_stream_id='time_entry_external_reference',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'available'},'breadcrumb': ['properties','external_reference']},
    ]),
    CatalogEntry(
        stream='time_entries',
        tap_stream_id='time_entries',
        schema=Schema(
            properties={
                'id': Schema(type='integer'),}),
        metadata=[
            {'metadata': {'inclusion': 'automatic'},'breadcrumb': ['properties','id']},
    ])])

class TestStreamsUtils(unittest.TestCase):
    """
    Test all utility functions of streams module
    """
    
    @parameterized.expand([
        ['test_null_date_time_field', {'created': None, 'id': 1}, {'properties': {'id': {'type': ['int']}, 'created': {'format': 'date-time', 'type': ['string']}}}, {'id': 1}],
        ['test_not_null_date_time_field', {'created': 'valid_date', 'id': 1}, {'properties': {'id': {'type': ['int']}, 'created': {'format': 'date-time', 'type': ['string']}}}, {'created': 'valid_date', 'id': 1}],
        ['test_no_datetime_field', {'id': 1}, {'properties': {'id': {'type': ['int']}}}, {'id': 1}]
    ])
    def test_remove_empty_date_times(self, name, actual_record, schema, expected_record):
        """
        """
        remove_empty_date_times(actual_record, schema)
        
        # Verify expetced record
        self.assertEqual(expected_record, actual_record)
    
    @parameterized.expand([
        ['test_no_date_fields_in_tap', [], {'id': 1}, {'id': 1}],
        ['test_single_date_fields', ['issue_date'], {'issue_date': '2022-08-30', 'resolve_date': '2021-08-30'}, {'issue_date': '2022-08-30T00:00:00.000000Z', 'resolve_date': '2021-08-30'}],
        ['test_multiple_date_fields', ['issue_date', 'resolve_date'], {'issue_date': '2022-08-30', 'resolve_date': '2021-08-30'}, {'issue_date': '2022-08-30T00:00:00.000000Z', 'resolve_date': '2021-08-30T00:00:00.000000Z'}],
        ['test_no_date_fields_in_record', ['issue_date'], {'id': 1}, {'id': 1}]
    ])
    def test_append_times_to_dates(self, name, date_fields, record, expected_record):
        """
        """
        append_times_to_dates(record, date_fields)
        
        # Verify expected record
        self.assertEqual(record, expected_record)
    
    
    @parameterized.expand([
        ['test_none_state', 'a', None, "default"],
        ['test_empty_state', 'a', {}, "default"],
        ['test_empty_bookmark', 'a', {'b': 'bookmark_a'}, "default"],
        ['test_non_empty_bookmark', 'a', {'a': 'bookmark_a'}, "bookmark_a"]
    ])
    def test_get_bookmark(self, name, stream_name, state, expected_output):
        """
        Test get_bookmark function for the following scenrios,
        Case 1: Return default value if state is None
        Case 2: Return default value if `bookmarks` key is not found in the state
        Case 3: Return default value if stream_name is not found in the bookmarks
        Cas 4: Return actual bookmark value if it is found in the state
        """
        
        actual_output = get_bookmark(stream_name, state, "default")
        
        self.assertEqual(expected_output, actual_output)

START_DATE = '2022-07-30T00:00:00.000000Z'
CONFIG = {'start_date': START_DATE,
          'client_id': 'client_id',
          'client_secret': 'client_secret',
          'refresh_token': 'refresh_token',
          'user_agent': 'user_agent'}
ID_OBJECT = {'id': 1}

class TestStream(unittest.TestCase):

    @parameterized.expand([
        ['test_parent_only_with_state', ['invoices'], {'invoices': '2022-08-30T00:00:00.000000Z'}, '2022-08-30T00:00:00.000000Z'],
        ['test_child_only_with_state', ['invoice_messages'], {'invoice_messages_parent': '2022-08-30T00:00:00.000000Z'}, '2022-08-30T00:00:00.000000Z'],
        ['test_parent_only_without_state', ['invoices'],  {}, START_DATE],
        ['test_child_only_without_state', ['invoice_messages'],  {}, START_DATE],
        ['test_min_parent_bookmark_single_child', ['invoices', 'invoice_messages'], {'invoice_messages_parent': '2022-08-30T00:00:00.000000Z', 'invoices': '2022-07-30T00:00:00.000000Z'}, '2022-07-30T00:00:00.000000Z'],
        ['test_min_child_bookmark_single_child', ['invoices', 'invoice_messages'], {'invoice_messages_parent': '2022-07-30T00:00:00.000000Z', 'invoices': '2022-08-30T00:00:00.000000Z'}, '2022-07-30T00:00:00.000000Z'],
        ['test_min_child_bookmark_multiple_child', ['invoices', 'invoice_messages', 'invoice_payments'], {'invoice_messages_parent': '2022-07-30T00:00:00.000000Z', 'invoices': '2022-08-30T00:00:00.000000Z', 'invoice_payments_parent': '2022-06-30T00:00:00.000000Z'}, '2022-06-30T00:00:00.000000Z'],
        ['test_multiple_child_only_bookmark', ['invoice_messages', 'invoice_payments'], {'invoice_messages_parent': '2022-08-30T00:00:00.000000Z', 'invoice_payments_parent': '2022-07-30T00:00:00.000000Z'}, '2022-07-30T00:00:00.000000Z'],
        ['test_multiple_child_empty_bookmark', ['invoice_messages', 'invoice_payments'], {'invoice_messages_parent': '2022-11-00T00:00:00.000000Z'}, START_DATE]

    ])
    def test_get_min_bookmark(self, name, selected_streams, state, expected_bookmark):
        """
        Test that the `get_min_bookmark` function returns the minimum bookmark from the parent and its corresponding child bookmarks.
        """
        invoice_obj = Invoices()
        
        actual_bookmark = invoice_obj.get_min_bookmark('invoices', selected_streams, '2022-11-30T00:00:00.000000Z', START_DATE, state)
        
        # Verify expected bookmark value
        self.assertEqual(actual_bookmark, expected_bookmark)

@mock.patch('singer.write_record')
@mock.patch('tap_harvest.client.HarvestClient.request')
@mock.patch('singer.write_state')
class TestSyncEndpoint(unittest.TestCase):
    """
    Test sync endpoint method of different streams.
    """

    @parameterized.expand([
        ['test_only_parent_selected_with_1_record', ['invoices'], 
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None},
          {'invoice_messages': [], 'next_page': None},
          {'invoice_payments': [], 'next_page': None}], 
         {'invoices': '2022-08-30T10:08:18Z'}, {}, {}, 3, 1],
        
        ['test_only_parent_selected_with_0_record', ['invoices'], 
         [{'invoices': [], 'next_page': None}], {}, {}, {}, 1, 0],
        
        ['test_only_single_child_selected_with_no_bookmark', ['invoice_messages'],
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-00T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}, 
          {'invoice_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None},
          {'invoice_payments': [], 'next_page': None}], 
         {'invoice_messages': '2022-08-30T10:08:18Z', 'invoice_messages_parent': '2022-08-00T10:08:18Z'}, {}, {}, 3, 1],
        
        ['test_only_multiple_child_selected_with_no_bookmark', ['invoice_messages', 'invoice_payments'],
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-00T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}, 
          {'invoice_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None},
          {'invoice_payments': [{'id': 1, 'updated_at': '2022-07-30T10:08:18Z', 'payment_gateway': {'id': 1, 'name': 'abc'}}], 'next_page': None}], 
         {'invoice_messages': '2022-08-30T10:08:18Z', 'invoice_messages_parent': '2022-08-00T10:08:18Z',
          'invoice_payments_parent': '2022-08-00T10:08:18Z', 'invoice_payments': '2022-07-30T10:08:18Z'}, {}, {}, 3, 2],
        
        ['test_both_parent_child_selected_with_no_bookmark', ['invoice_messages', 'invoices'],
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-00T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}, 
          {'invoice_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None},
          {'invoice_payments': [], 'next_page': None}], 
         {'invoice_messages': '2022-08-30T10:08:18Z', 'invoice_messages_parent': '2022-08-00T10:08:18Z', 
          'invoices': '2022-08-00T10:08:18Z'}, {}, {}, 3, 2]
    ])
    def test_sync_endpoint_for_parent_child_streams(self, mock_singer, mock_request, mock_write_record, name, selected_streams, 
                                                    response, expected_bookmark, state, tap_state, write_state_call_count, 
                                                    write_record_call_count):
        """
        Test sync_endpoint function for parent and child streams.
        """
        client = HarvestClient(CONFIG)
        obj = Invoices()

        mock_request.side_effect = response
        obj.sync_endpoint(client, CATALOG, CONFIG, state, tap_state, selected_streams)
        
        # Verify that tap writes the expected bookmark for each stream.
        mock_singer.assert_called_with(expected_bookmark)
        # Verify request method is called for the expected no of time.
        self.assertEqual(mock_request.call_count, write_state_call_count)
        # Verify that write_record is called for the expected no of time.
        self.assertEqual(mock_write_record.call_count, write_record_call_count)


    def test_sync_endpoint_for_user_roles_stream(self, mock_singer, mock_request, mock_write_record):
        """
        Test sync_endpoint function for `roles` stream
        """
        client = HarvestClient(CONFIG)
        obj = Roles()
        expected_record = {'role_id': 1, 'user_id': '2'}
        mock_request.side_effect = [{'roles': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z', 'user_ids': ['1', '2']}], 'next_page': None}]
        
        obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['user_roles'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(expected_record, args[1])

    def test_sync_endpoint_for_user_project_tasks_stream(self, mock_singer, mock_request, mock_write_record):
        """
        Test sync_endpoint function for `project_tasks` stream
        """
        client = HarvestClient(CONFIG)
        obj = Users()
        expected_record = {'project_task_id': 1, 'user_id': 2}
        mock_request.side_effect = [{'users': [{'id': 2, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None},
                                    {'project_assignments': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z', 'task_assignments': [ID_OBJECT]}], 'next_page': None}]
        
        x = obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['user_project_tasks'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(args[1], expected_record)

    @parameterized.expand([
        ['test_none_receipt_value', [{'expenses': [{'id':1, 'updated_at': '2022-08-30T10:08:18Z', 'receipt': None}], 'next_page': None}],
         {'id':1, 'updated_at': '2022-08-30T10:08:18Z', 'receipt_url': None, 'receipt_file_name': None, 'receipt_file_size': None, 'receipt_content_type': None,
          'receipt': None, 'client_id': None, 'project_id': None, 'expense_category_id': None, 'user_id': None, 'user_assignment_id': None, 'invoice_id': None}],
        ['test_not_none_receipt_value', [{'expenses': [{'id':1, 'updated_at': '2022-08-30T10:08:18Z', 
            'receipt': {'url': 'URL', 'file_name': 'abc.txt', 'file_size': '10mb', 'content_type': 'text'}}], 'next_page': None}],
        {'id':1, 'updated_at': '2022-08-30T10:08:18Z', 'receipt_url': 'URL', 'receipt_file_name': 'abc.txt', 'receipt_file_size': '10mb', 'receipt_content_type': 'text',
          'receipt': {'url': 'URL', 'file_name': 'abc.txt', 'file_size': '10mb', 'content_type': 'text'}, 
          'client_id': None, 'project_id': None, 'expense_category_id': None, 'user_id': None, 'user_assignment_id': None, 'invoice_id': None}],
    ])
    def test_sync_endpoint_for_expense_stream(self, mock_singer, mock_request, mock_write_record, name, response, expected_record):
        """
        Test sync_endpoint function for `expense` stream
        """
        client = HarvestClient(CONFIG)
        obj = Expenses()
        mock_request.side_effect = response
        
        obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['expenses'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(expected_record, args[1])

    @parameterized.expand([
        ['test_none_project_value', [{'invoices': [{'id':2, 'updated_at': '2022-08-30T10:08:18Z', 'line_items': [{'id': 1, 'project': None}]}], 'next_page': None},
        {'invoice_messages': [{'id':2, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None}, 
        {'invoice_payments': [{'id':2, 'updated_at': '2022-08-30T10:08:18Z', 'payment_gateway': {}}], 'next_page': None}],
         {'id': 1, 'invoice_id': 2, 'project_id': None, 'project': None}],
        ['test_not_none_project_value', [{'invoices': [{'id':2, 'updated_at': '2022-08-30T10:08:18Z', 'line_items': [{'id': 1, 'project': ID_OBJECT}]}], 'next_page': None},
        {'invoice_messages': [{'id':2, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None}, 
        {'invoice_payments': [{'id':2, 'updated_at': '2022-08-30T10:08:18Z', 'payment_gateway': {}}], 'next_page': None}],
         {'id': 1, 'invoice_id': 2, 'project_id': 1, 'project': ID_OBJECT}],
    ])
    def test_sync_endpoint_for_invoice_line_iteams_stream(self, mock_singer, mock_request, mock_write_record, name, response, expected_record):
        """
        Test sync_endpoint function for `invoice_line_iteams` stream
        """
        client = HarvestClient(CONFIG)
        obj = Invoices()
        mock_request.side_effect = response
        
        obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['invoice_line_items'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(expected_record, args[1])

    def test_sync_endpoint_for_estimate_line_items_stream(self, mock_singer, mock_request, mock_write_record):
        """
        Test sync_endpoint function for `estimate_line_items` stream
        """
        client = HarvestClient(CONFIG)
        obj = Estimates()
        expected_record = {'id': 1, 'estimate_id': 2}
        mock_request.side_effect = [{'estimates': [{'id': 2, 'updated_at': '2022-08-30T10:08:18Z', 'line_items': [{'id': 1}]}], 'next_page': None},
                                    {'estimate_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z', }], 'next_page': None}]
        
        obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['estimate_line_items'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(expected_record, args[1])

    def test_sync_endpoint_for_external_reference_stream(self, mock_singer, mock_request, mock_write_record):
        """
        Test sync_endpoint function for `external_reference` stream
        """
        client = HarvestClient(CONFIG)
        obj = TimeEntries()
        expected_record = {'id': 1}
        mock_request.side_effect = [{'time_entries': [{'id': 2, 'updated_at': '2022-08-30T10:08:18Z', 'external_reference': {'id': 1}}], 'next_page': None}]
        
        obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['external_reference'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(expected_record, args[1])

    def test_sync_endpoint_for_time_entry_external_reference_stream(self, mock_singer, mock_request, mock_write_record):
        """
        Test sync_endpoint function for `time_entry_external_reference` stream
        """
        client = HarvestClient(CONFIG)
        obj = TimeEntries()
        expected_record = {'external_reference_id': 1, 'time_entry_id': 2}
        mock_request.side_effect = [{'time_entries': [{'id': 2, 'updated_at': '2022-08-30T10:08:18Z', 'external_reference': {'id': 1}}], 'next_page': None}]
        
        obj.sync_endpoint(client, CATALOG, CONFIG, {}, {}, ['time_entry_external_reference'])
        args, kwargs = mock_write_record.call_args
        # Verify that tap writes the expected record.
        self.assertEqual(expected_record, args[1])