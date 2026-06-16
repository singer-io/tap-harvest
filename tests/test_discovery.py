"""Test tap discovery mode and metadata."""
from typing import Dict
from base import HarvestBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest
from tap_tester import menagerie


class HarvestDiscoveryTest(DiscoveryTest, HarvestBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""

    @staticmethod
    def name():
        return "tap_tester_harvest_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()

    def expected_parent_stream_id(self, stream: str = None) -> Dict:
        """ Function to get the expected parent stream details if it exists in a stream

        Args:
            stream (str, optional): Stream name. Defaults to None.

        Returns:
            Dict: A mapping of stream name and it's parent-tap-stream-id
        """

        parent_stream_keys = {
            table: properties.get(self.PARENT_TAP_STREAM_ID, "")
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return parent_stream_keys

        return parent_stream_keys[stream]

    def test_replication_metadata(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # gather expectations
                expected_replication_keys = self.expected_replication_keys(stream)
                expected_replication_method = self.expected_replication_method(stream)
                expected_parent_tap_stream_id = self.expected_parent_stream_id(stream=stream)

                # gather results
                catalog_entries = [
                    catalog
                    for catalog in self.found_catalogs
                    if catalog["stream_name"] == stream
                ]
                if not catalog_entries:
                    # Stream was excluded from the catalog by the access check
                    # (its parent stream may be inaccessible with the CI credentials).
                    continue
                catalog = catalog_entries[0]
                metadata = menagerie.get_annotated_schema(
                    self.conn_id, catalog["stream_id"]
                )["metadata"]
                stream_properties = [
                    item for item in metadata if item.get("breadcrumb") == []
                ]
                actual_replication_method = (
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.REPLICATION_METHOD, None)
                )
                actual_replication_keys = set(
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.REPLICATION_KEYS, [])
                )

                actual_parent_tap_stream_id = (
                    stream_properties[0]
                    .get("metadata", {})
                    .get(self.PARENT_TAP_STREAM_ID, "")
                )

                # verify the metadata key is in properties
                self.assertIn("metadata", stream_properties[0])
                stream_metadata = stream_properties[0]["metadata"]

                # verify the replication keys metadata key is in metadata
                self.assertIn(self.REPLICATION_METHOD, stream_metadata)
                self.assertTrue(isinstance(actual_replication_method, str))

                # verify actual replication key(s) match expected
                with self.subTest(msg="validating replication keys"):
                    self.assertSetEqual(
                        expected_replication_keys,
                        actual_replication_keys,
                        logging=f"verify {expected_replication_keys} "
                        f"is saved in metadata as a valid-replication-key",
                    )

                # verify the actual replication matches our expected replication method
                with self.subTest(msg="validating replication method"):
                    self.assertEqual(
                        expected_replication_method,
                        actual_replication_method,
                        logging=f"verify the replication method is "
                        f"{expected_replication_method}",
                    )

                # Verify if the parent-tap-stream-id (if applicable) is present in catalog
                if expected_parent_tap_stream_id:
                    self.assertEqual(
                        expected_parent_tap_stream_id,
                        actual_parent_tap_stream_id,
                        logging=f"verify {expected_parent_tap_stream_id} is saved in metadata as parent-tap-stream-id"
                    )

                # verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                # If replication keys are not specified in metadata, skip this check
                with self.subTest(msg="validating expectations consistency"):
                    if expected_replication_keys:
                        self.assertEqual(
                            actual_replication_method,
                            self.INCREMENTAL,
                            logging=f"verify the forced replication method is "
                            f"{self.INCREMENTAL} since there is a "
                            f"replication-key",
                        )
