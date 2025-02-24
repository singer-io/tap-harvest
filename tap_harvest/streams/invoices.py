from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from tap_harvest.streams.invoice_messages import Invoice_messages
from tap_harvest.streams.invoice_payments import Invoice_payments
from tap_harvest.streams.invoice_line_items import Invoice_line_items

from tap_harvest.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Invoices(IncrementalStream):
    tap_stream_id = "invoices"
    key_properties = ["id"]
    replication_keys = ["updated_at"]
    data_key = "invoices"
    path = "invoices"
    object_to_id = ["client", "estimate", "retainer", "creator"]
    children = ["invoice_payments", "invoice_messages", "invoice_line_items"]

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
        self.update_filter_params(updated_since=bookmark_date)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.map_object(record)
                record = self.add_object_to_id(record)
                self.remove_empty_date_times(record, schema)

                transformed_record = transformer.transform(
                    record, schema, stream_metadata
                )
                self.append_times_to_dates(transformed_record)

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    write_record(self.tap_stream_id, transformed_record)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )
                    counter.increment()

                if "invoice_payments" in selected_streams:
                    invoice_payments = Invoice_payments(self.client)
                    invoice_payments.sync(
                        state=state,
                        schema=schema,
                        stream_metadata=stream_metadata,
                        transformer=transformer,
                        selected_streams=selected_streams,
                        parent_obj=record,
                    )

                if "invoice_messages" in selected_streams:
                    invoice_messages = Invoice_messages(self.client)
                    invoice_messages.sync(
                        state=state,
                        schema=schema,
                        stream_metadata=stream_metadata,
                        transformer=transformer,
                        selected_streams=selected_streams,
                        parent_obj=record,
                    )

                if "invoice_line_items" in selected_streams:
                    invoice_line_items = Invoice_line_items(self.client)
                    invoice_line_items.sync(
                        state=state,
                        schema=schema,
                        stream_metadata=stream_metadata,
                        transformer=transformer,
                        selected_streams=selected_streams,
                        parent_obj=record,
                    )

            state = self.write_bookmark(state, value=current_max_bookmark_date)
            return counter.value
