from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Invoice_payments(IncrementalStream):
    tap_stream_id = "invoice_payments"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    path = "invoices/{}/payments"
    data_key = "invoice_payments"
    parent = "invoices"
    date_fields = ["send_reminder_on"]

    def sync(
        self,
        state: Dict,
        schema: Dict,
        stream_metadata: Dict,
        transformer: Transformer,
        selected_streams: List,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Incremental` stream."""

        current_max_bookmark_date = bookmark_date = self.get_bookmark(state)

        self.url_endpoint = (
            f"{self.client.base_url}/{self.path.format(parent_obj['id'])}"
        )
        LOGGER.info(f"URL Endpoint: {self.url_endpoint}")
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record['invoice_id'] = parent_obj['id']
                record['payment_gateway_id'] = record['payment_gateway']['id']
                record['payment_gateway_name'] = record['payment_gateway']['name']
                record = self.add_object_to_id(record)
                self.remove_empty_date_times(record, schema)

                transformed_record = transformer.transform(
                    record, schema, stream_metadata
                )

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

            state = self.write_bookmark(state, value=current_max_bookmark_date)
            return counter.value