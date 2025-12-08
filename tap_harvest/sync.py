import singer
from typing import Dict
from tap_harvest.streams import STREAMS
from tap_harvest.client import Client
from tap_harvest.schema import write_schema

LOGGER = singer.get_logger()


def update_currently_syncing(state: Dict, stream_name: str) -> None:
    """Update currently_syncing in state and write it."""
    if not stream_name and singer.get_currently_syncing(state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client: Client, config: Dict, catalog: singer.Catalog, state) -> None:
    """Sync selected streams from catalog."""

    streams_to_sync = []
    for stream in catalog.get_selected_streams(state):
        streams_to_sync.append(stream.stream)
    LOGGER.info(f"selected_streams: {streams_to_sync}")

    last_stream = singer.get_currently_syncing(state)
    LOGGER.info(f"last/currently syncing stream: {last_stream}")

    with singer.Transformer() as transformer:
        for stream_name in streams_to_sync:
            stream = STREAMS[stream_name](client, catalog.get_stream(stream_name))
            if stream.parent:
                if stream.parent not in streams_to_sync:
                    streams_to_sync.append(stream.parent)
                continue

            write_schema(stream, client, streams_to_sync, catalog)
            LOGGER.info(f"START Syncing: {stream_name}")
            update_currently_syncing(state, stream_name)
            total_records = stream.sync(state=state, transformer=transformer)

            update_currently_syncing(state, None)
            LOGGER.info(
                f"FINISHED Syncing: {stream_name}, total_records: {total_records}"
            )
