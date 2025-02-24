from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple, List

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)
from singer.utils import strftime, strptime_to_utc, strftime, strptime_with_tz

LOGGER = get_logger()


class BaseStream(ABC):
    """
    A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 100
    next_page_key = "next_page"
    params = {}
    headers = {"Accept": "application/json"}
    object_to_id = []
    date_fields = []
    children = []
    parent = ""

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def forced_replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    @property
    def selected_by_default(self) -> bool:
        """Indicates if a node in the schema should be replicated, if a user
        has not expressed any opinion on whether or not to replicate it."""
        return False

    @abstractmethod
    def sync(
        self,
        state: Dict,
        schema: Dict,
        stream_metadata: Dict,
        transformer: Transformer,
        selected_streams: List,
        parent_obj: Dict = None,
    ) -> Dict:
        """
        Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - schema (dict): Schema of the stream
         - transformer (object): A Object of the singer.transformer class.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def __init__(self, client=None) -> None:
        self.client = client

    def get_records(self) -> List:
        """Interacts with api client interaction and pagination."""
        extraction_url = self.url_endpoint
        page = 1

        while page:
            LOGGER.info("Calling Page %s", page)
            self.params["page"] = page
            response = self.client.get(
                extraction_url, self.params, self.headers, self.path
            )
            raw_records = response.get(self.data_key, [])

            page = response.get(self.next_page_key)
            yield from raw_records

    def write_schema(self, schema):
        """
        Write a schema message.
        """
        try:
            write_schema(self.tap_stream_id, schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_filter_params(self, **kwargs):
        """
        Update the filter key and value for the stream
        """
        self.params.update(kwargs)

    def add_object_to_id(self, record: Dict) -> Dict:
        """
        Add object_to_id to the stream
        """
        if self.object_to_id is not None:
            for key in self.object_to_id:
                if record[key] is not None:
                    record[key + "_id"] = record[key]["id"]
                else:
                    record[key + "_id"] = None

        return record

    def map_object(self, record: Dict) -> Dict:
        """
        Modify the record before writing to the stream
        """
        return record

    def remove_empty_date_times(self, record: Dict, schema: Dict):
        """
        Remove empty date-time fields from the item
        """
        fields = []

        for key in schema["properties"]:
            subschema = schema["properties"][key]
            if subschema.get("format") == "date-time":
                fields.append(key)

        for field in fields:
            if record.get(field) is None:
                del record[field]

    def append_times_to_dates(self, record: Dict):
        """
        Append times to date fields
        """
        for date_field in self.date_fields:
            if record.get(date_field):
                record[date_field] = strftime(strptime_with_tz(record[date_field]))


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"
    config_start_key = "start_date"

    def get_bookmark(self, state: dict, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            self.tap_stream_id,
            key or self.replication_keys[0],
            self.client.config.get(self.config_start_key, False),
        )

    def write_bookmark(self, state: dict, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return write_bookmark(
            state, self.tap_stream_id, key or self.replication_keys[0], value
        )

    def sync(
        self,
        state: Dict,
        schema: Dict,
        stream_metadata: Dict,
        transformer: Transformer,
        selected_streams: List,
        parent_obj: Dict = None,
    ) -> Dict:
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

            state = self.write_bookmark(state, value=current_max_bookmark_date)
            return counter.value
