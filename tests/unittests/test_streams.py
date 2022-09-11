import datetime
from unittest import mock
from singer import utils
from parameterized import parameterized
import unittest
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
from tap_harvest.streams import remove_empty_date_times, append_times_to_dates, get_bookmark, Invoices
from tap_harvest.client import HarvestClient

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
            {'metadata': {'inclusion': 'available','selected': True},'breadcrumb': ['properties','invoice_id']}
    ])])

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
        """
        invoice_obj = Invoices()
        
        actual_bookmark = invoice_obj.get_min_bookmark('invoices', selected_streams, '2022-11-30T00:00:00.000000Z', START_DATE, state)
        
        self.assertEqual(actual_bookmark, expected_bookmark)

    @parameterized.expand([
        ['test_only_parent_selected_with_1_record', ['invoices'], 
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}], 
         {'invoices': '2022-08-30T10:08:18Z'}, {}, {}, 1, 1],
        
        ['test_only_parent_selected_with_0_record', ['invoices'], 
         [{'invoices': [], 'next_page': None}], {}, {}, {}, 1, 0],
        
        ['test_only_single_child_selected_with_no_bookmark', ['invoice_messages'],
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-00T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}, 
          {'invoice_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None}], 
         {'invoice_messages': '2022-08-30T10:08:18Z', 'invoice_messages_parent': '2022-08-00T10:08:18Z'}, {}, {}, 2, 1],
        
        ['test_only_multiple_child_selected_with_no_bookmark', ['invoice_messages', 'invoice_payments'],
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-00T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}, 
          {'invoice_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None},
          {'invoice_payments': [{'id': 1, 'updated_at': '2022-07-30T10:08:18Z', 'payment_gateway': {'id': 1, 'name': 'abc'}}], 'next_page': None}], 
         {'invoice_messages': '2022-08-30T10:08:18Z', 'invoice_messages_parent': '2022-08-00T10:08:18Z',
          'invoice_payments_parent': '2022-08-00T10:08:18Z', 'invoice_payments': '2022-07-30T10:08:18Z'}, {}, {}, 3, 2],
        
        ['test_both_parent_child_selected_with_no_bookmark', ['invoice_messages', 'invoices'],
         [{'invoices': [{'id': 1, 'updated_at': '2022-08-00T10:08:18Z', 'client': ID_OBJECT, 'estimate': ID_OBJECT, 'retainer': None, 'creator': ID_OBJECT}], 'next_page': None}, 
          {'invoice_messages': [{'id': 1, 'updated_at': '2022-08-30T10:08:18Z'}], 'next_page': None}], 
         {'invoice_messages': '2022-08-30T10:08:18Z', 'invoice_messages_parent': '2022-08-00T10:08:18Z', 
          'invoices': '2022-08-00T10:08:18Z'}, {}, {}, 2, 2],
    ])
    @mock.patch('singer.write_record')
    @mock.patch('tap_harvest.client.HarvestClient.request')
    @mock.patch('singer.write_state')
    def test_sync_endpoint(self, name, selected_streams, response, expected_bookmark, state, tap_state, write_state_call_count, 
                           write_record_call_count, mock_singer, mock_request, mock_write_record):
        client = HarvestClient(CONFIG)
        obj = Invoices()

        mock_request.side_effect = response
        obj.sync_endpoint(client, CATALOG, CONFIG, state, tap_state, selected_streams)
        
        mock_singer.assert_called_with(expected_bookmark)
        self.assertEqual(mock_request.call_count, write_state_call_count)
        self.assertEqual(mock_write_record.call_count, write_record_call_count)