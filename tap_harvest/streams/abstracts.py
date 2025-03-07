from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple

from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
)
from singer.utils import strftime, strptime_with_tz

LOGGER = get_logger()


class BaseStream(ABC):
    """A Base Class providing structure and boilerplate for generic streams and
    required attributes for any kind of stream ~~~

    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    page_size = 100
    next_page_key = "next_page"
    headers = {"Accept": "application/json"}
    object_to_id = []
    date_fields = []
    children = []
    support_filter = True
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    bookmark_value = None

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}

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
    def key_properties(self) -> Tuple[str, str]:
        """List of key properties for stream."""

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Performs a replication sync for the stream. ~~~

        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """

    def get_records(self) -> List:
        """Interacts with api client interaction and pagination."""
        page = 1
        while page:
            self.params["page"] = page
            response = self.client.get(
                self.url_endpoint, self.params, self.headers, self.path
            )
            raw_records = response.get(self.data_key, [])

            page = response.get(self.next_page_key)
            yield from raw_records

    def write_schema(self):
        """Write a schema message."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                "OS Error while writing schema for: {}".format(self.tap_stream_id)
            )
            raise err

    def update_params(self, **kwargs):
        """Update params for the stream."""
        if self.support_filter:
            self.params.update(kwargs)

    def add_object_to_id(self, record: Dict) -> Dict:
        """Add object_to_id to the stream."""
        if self.object_to_id is not None:
            for key in self.object_to_id:
                if record[key] is not None:
                    record[key + "_id"] = record[key]["id"]
                else:
                    record[key + "_id"] = None

        return record

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream."""
        record = self.add_object_to_id(record)
        self.remove_empty_date_times(record)
        return record

    def remove_empty_date_times(self, record: Dict):
        """Remove empty date-time fields from the item."""
        fields = []

        for key in self.schema["properties"]:
            sub_schema = self.schema["properties"][key]
            if sub_schema.get("format") == "date-time":
                fields.append(key)

        for field in fields:
            if record.get(field) is None:
                del record[field]

    def append_times_to_dates(self, record: Dict):
        """Append times to date fields."""
        for date_field in self.date_fields:
            if record.get(date_field):
                record[date_field] = strftime(strptime_with_tz(record[date_field]))

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Get the URL endpoint for the stream."""
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"


class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        return get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )

    def write_bookmark(
        self, state: dict, stream: str, key: Any = None, value: Any = None
    ) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        current_bookmark = get_bookmark(
            state,
            stream,
            key or self.replication_keys[0],
            self.client.config["start_date"],
        )
        value = max(current_bookmark, value)
        return write_bookmark(state, stream, key or self.replication_keys[0], value)

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        current_max_bookmark_date = bookmark_date = self.get_bookmark(
            state, self.tap_stream_id
        )
        self.update_params(updated_since=bookmark_date)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                self.append_times_to_dates(transformed_record)

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )

                    for child in self.child_to_sync:
                        child.sync(
                            state=state, transformer=transformer, parent_obj=record
                        )

            state = self.write_bookmark(
                state, self.tap_stream_id, value=current_max_bookmark_date
            )
            return counter.value
