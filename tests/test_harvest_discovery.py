import re

from base import BaseTapTest
from tap_tester import menagerie

NO_REPLICATION_KEYS_STREAMS = {
    "invoice_line_items",
    "time_entry_external_reference",
    "user_roles",
    "user_project_tasks",
    "estimate_line_items",
    "external_reference",
}


class HarvestDiscovery(BaseTapTest):
    """Test tap discovery mode and metadata."""

    def name(self):
        return "tap_tester_harvest_discovery_qa"

    def do_test(self, conn_id):
        """
        Testing that discovery creates the appropriate catalog with valid metadata.
        • Verify number of actual streams discovered matches expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow the naming convention
          streams should only have lowercase alphas and underscore
        • Verify there is only 1 top level of breadcrumb
        • Verify there is no duplicate metadata entries
        • Verify replication key(s)
        • Verify primary key(s)
        • Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • Verify the actual replication matches our expected replication method
        • Verify that primary, replication and foreign keys are given the inclusion of automatic.
        • Verify that all other fields have the inclusion of available metadata.
        """
        streams_to_test = self.expected_streams()

        found_catalogs = menagerie.get_catalogs(conn_id)
        # Verify stream names follow the naming convention
        # Streams should only have lowercase alphas and underscores
        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}
        self.assertTrue(
            all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
            msg="One or more streams don't follow standard naming",
        )

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify ensure the catalog is found for a given stream
                catalog = next(
                    iter(
                        [
                            catalog
                            for catalog in found_catalogs
                            if catalog["stream_name"] == stream
                        ]
                    )
                )
                self.assertIsNotNone(catalog)

                # Collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = self.expected_automatic_keys()[stream]
                expected_replication_method = self.expected_replication_method()[stream]

                # Collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(
                    conn_id, catalog["stream_id"]
                )
                metadata = schema_and_metadata["metadata"]
                stream_properties = [
                    item for item in metadata if item.get("breadcrumb") == []
                ]
                actual_primary_keys = set(
                    stream_properties[0]
                    .get("metadata", {self.PRIMARY_KEYS: []})
                    .get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0]
                    .get("metadata", {self.REPLICATION_KEYS: []})
                    .get(self.REPLICATION_KEYS, [])
                )

                actual_replication_method = (
                    stream_properties[0]
                    .get("metadata", {self.REPLICATION_METHOD: None})
                    .get(self.REPLICATION_METHOD)
                )

                actual_automatic_fields = {
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                }

                actual_fields = []
                for md_entry in metadata:
                    if md_entry['breadcrumb'] != []:
                        actual_fields.append(md_entry['breadcrumb'][1])

                ##########################################################################
                ### Metadata assertions
                ##########################################################################

                # Verify there is only 1 top-level breadcrumb in metadata
                self.assertTrue(
                    len(stream_properties) == 1,
                    msg="There is NOT only one top level breadcrumb for {}".format(
                        stream
                    )
                    + f"\nstream_properties | {stream_properties}",
                )

                # Verify there is no duplicate metadata entries
                self.assertEqual(len(actual_fields), len(
                    set(actual_fields)), msg="duplicates in the fields retrieved")

                # Verify replication key(s) match expectations
                self.assertSetEqual(expected_replication_keys, actual_replication_keys)

                # Verify primary key(s) match expectations
                self.assertSetEqual(expected_primary_keys, actual_primary_keys)

                # Verify that if there is a replication key then it's INCREMENTAL otherwise FULL TABLE stream
                if actual_replication_keys or stream in NO_REPLICATION_KEYS_STREAMS:
                    self.assertEqual(self.INCREMENTAL, actual_replication_method)
                else:
                    self.assertEqual(self.FULL, actual_replication_method)

                # Verify the replication method matches our expectations
                self.assertEqual(expected_replication_method, actual_replication_method)

                # Verify that primary keys and replication keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_automatic_fields, actual_automatic_fields)

                # Verify that all other fields have the inclusion available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all(
                        {
                            item.get("metadata").get("inclusion") == "available"
                            for item in metadata
                            if item.get("breadcrumb", []) != []
                            and item.get("breadcrumb", ["properties", None])[1]
                            not in actual_automatic_fields
                        }
                    ),
                    msg="Not all non-key properties are set to available in metadata",
                )
