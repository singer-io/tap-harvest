"""
Test tap pagination of streams
"""
from math import ceil

from base import BaseTapTest
from harvest_api import set_up, tear_down
from tap_tester import LOGGER, menagerie, runner


class PaginationTest(BaseTapTest):
    """Test the tap pagination to get multiple pages of data."""

    @classmethod
    def setUpClass(cls):
        set_up(cls, 101)

    @classmethod
    def tearDownClass(cls):
        tear_down(cls)

    def name(self):
        return f"{super().name()}_pagination_test"

    def do_test(self, conn_id):
        """Verify that for each stream you can get multiple pages of data and
        that when all fields are selected more than the automatic fields are
        replicated.

        PREREQUISITE For EACH stream add enough data that you surpass
        the limit of a single fetch of data.  For instance, if you have
        a limit of 250 records ensure that 251 (or more) records have
        been posted for that stream.
        """

        # Select all streams and all fields within streams
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.select_all_streams_and_fields(
            conn_id, found_catalogs, select_all_fields=True
        )

        # Run a sync job using orchestrator
        record_count_by_stream = self.run_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()
        synced_records = runner.get_records_from_target_output()

        untested_streams = [
            stream for stream in self._master if not self._master[stream]["test"]
        ]

        for stream in self.expected_streams().difference(set(untested_streams)):
            with self.subTest(stream=stream):
                LOGGER.info("Testing " + stream)

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                page_size = self.expected_metadata().get(stream).get(self.API_LIMIT)

                # Collect information for assertions from syncs 1 & 2 base on expected values
                primary_keys_list = [
                    tuple(
                        message.get("data").get(expected_pk)
                        for expected_pk in expected_primary_keys
                    )
                    for message in synced_records.get(stream).get("messages")
                    if message.get("action") == "upsert"
                ]

                # Verify that we can paginate with all fields selected
                self.assertGreater(
                    record_count_by_stream.get(stream, -1),
                    self.expected_metadata().get(stream, {}).get(page_size, 0),
                    msg="The number of records is not over the stream max limit",
                )

                # TODO - change following assertion to assertEqual and capture all fields
                # Note - This ^ is nontrivial for fields which span multiple streams
                #  ex. {evet_type: send} in estimate_messages = {sent_at: time} in estimates

                # Verify the target receives all possible fields for a given stream
                self.assertEqual(
                    set(),
                    self._master[stream]["expected_fields"].difference(
                        actual_fields_by_stream.get(stream, set())
                    ),
                    msg="The fields sent to the target have an extra or missing field",
                )

                # Verify that the automatic fields are sent to the target for non-child streams
                if not self._master[stream]["child"]:
                    self.assertTrue(
                        actual_fields_by_stream.get(stream, set()).issuperset(
                            self.expected_primary_keys().get(stream, set())
                            | self.expected_replication_keys().get(stream, set())
                            | self.expected_foreign_keys().get(stream, set())
                        ),
                        msg="The fields sent to the target don't include all automatic fields",
                    )

                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / page_size)
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # Don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page),
                                msg=f"Other_page_primary_keys={other_page}",
                            )
