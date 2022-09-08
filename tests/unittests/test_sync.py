import unittest
from unittest import mock
from parameterized import parameterized
from tap_harvest.sync import write_schemas_recursive, get_streams_to_sync, sync
from singer import Catalog


def get_stream_catalog(stream_name, is_selected = False):
    """Return catalog for stream"""
    return {
                "schema":{},
                "tap_stream_id": stream_name,
                "stream": stream_name,
                "metadata": [
                        {
                            "breadcrumb": [],
                            "metadata":{"selected": is_selected}
                        }
                    ],
                "key_properties": []
            }

def get_catalog(parent=False, child=False):
    """Return complete catalog"""
    
    return Catalog.from_dict({
            "streams": [
                get_stream_catalog("expenses"),
                get_stream_catalog("invoices", parent),
                get_stream_catalog("invoice_payments", child),
                get_stream_catalog("estimates", parent),
                get_stream_catalog("estimate_line_items", child),
                get_stream_catalog("time_entries", parent),
            ]
        })


class TestSyncFunctions(unittest.TestCase):
    """
    Test `sync` function.
    """

    @parameterized.expand([
        ["only_parent_selected", get_catalog(parent=True), ["invoices", "estimates", "time_entries"], 3],
        ["only_child_selected", get_catalog(child=True), ["invoice_payments", "estimate_line_items"], 2],
        ["both_selected", get_catalog(parent=True, child=True), ["invoices", "estimates", "time_entries", "invoice_payments", "estimate_line_items"], 3],
        ["No_streams_selected", get_catalog(), [], 0],
    ])
    @mock.patch("singer.write_state")
    @mock.patch("singer.write_schema")
    @mock.patch("tap_harvest.streams.Stream.sync_endpoint")
    def test_sync(self, name, mock_catalog, selected_streams, synced_streams, mock_sync_endpoint, mock_write_schemas, mock_write_state):
        """
        Test sync function.
        """
        client = mock.Mock()
        sync(client=client, config={}, state={}, catalog=mock_catalog)

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, len(selected_streams))
        for stream in selected_streams:
            mock_write_schemas.assert_any_call(stream, mock.ANY, mock.ANY)

        # Verify sync object was called for syncing parent streams
        self.assertEqual(mock_sync_endpoint.call_count, synced_streams)


class TestGetStreamsToSync(unittest.TestCase):
    """
    Testcase for `get_stream_to_sync` in sync.
    """

    @parameterized.expand([
        ['test_parent_selected', ["estimates"], ["estimates"]],
        ['test_child_selected', ["estimate_messages", "estimate_line_items"], ["estimates"]],
        ['test_both_selected', ["estimate_messages", "invoices", "estimates"], ["invoices", "estimates"]]
    ])
    def test_sync_streams(self, name, selected_streams, expected_streams):
        """
        Test that if an only child is selected in the catalog,
        then `get_stream_to_sync` returns the parent streams if selected stream is child.
        """
        sync_streams = get_streams_to_sync(selected_streams)

        # Verify that the expected list of streams is returned
        self.assertEqual(sync_streams, expected_streams)


class TestWriteSchemas(unittest.TestCase):
    """
    Test `write_schemas` function.
    """

    mock_catalog = Catalog.from_dict({"streams": [
        get_stream_catalog("estimates"),
        get_stream_catalog("estimate_messages"),
        get_stream_catalog("invoices")
    ]})

    @parameterized.expand([
        ["parents_selected", ["estimates"]],
        ["child_selected", ["estimate_messages"]],
        ["parent_and_child_selected", ["estimates", "estimate_messages"]],
    ])
    @mock.patch("singer.write_schema")
    def test_write_schema(self, name, selected_streams, mock_write_schema):
        """
        Test that only schema is written for only selected streams.
        """
        write_schemas_recursive("estimates", self.mock_catalog, selected_streams)
        # Verify write_schema function is called for only selected streams.
        self.assertEqual(mock_write_schema.call_count, len(selected_streams))
        for stream in selected_streams:
            mock_write_schema.assert_any_call(stream, mock.ANY, mock.ANY)
