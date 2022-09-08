import singer
from tap_harvest.streams import STREAMS

LOGGER = singer.get_logger()

def get_streams_to_sync(selected_streams):
    """
    Get lists of streams to call the sync method.
    For children, ensure that dependent parent_stream is included even if it is not selected.
    """
    streams_to_sync = []

    # Loop thru all selected streams
    for stream_name in selected_streams:
        stream_obj = STREAMS[stream_name]
        # If the stream has a parent_stream, then it is a child stream
        parent_stream = hasattr(stream_obj, 'parent') and stream_obj.parent

        # Append selected parent streams
        if not parent_stream:
            streams_to_sync.append(stream_name)
        else:
            # Append un-selected parent streams of selected children
            if parent_stream not in selected_streams and parent_stream not in streams_to_sync:
                streams_to_sync.append(parent_stream)

    return streams_to_sync

def write_schemas_recursive(stream_id, catalog, selected_streams):
    """
    Write the schemas for each stream.
    """
    stream_obj = STREAMS[stream_id]()

    if stream_id in selected_streams:
        stream_obj.write_schema(catalog)

    for child in stream_obj.children:
        write_schemas_recursive(child, catalog, selected_streams)

def sync(client, config, catalog, state):
    """
    sync selected streams.
    """

    # Get ALL selected streams from catalog
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: %s', selected_streams)

    if not selected_streams:
        return

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)

    # state will preserve state passed in sync mode and
    # tap_state will be updated and written to output based on current sync
    tap_state = state.copy()

    # Get the list of streams(to sync stream itself or its child stream) for which
    # sync method needs to be called
    stream_to_sync = get_streams_to_sync(selected_streams)

    for stream_name in stream_to_sync:

        LOGGER.info('START Syncing: %s', stream_name)
        # Set currently syncing stream
        state = singer.set_currently_syncing(state, stream_name)
        write_schemas_recursive(stream_name, catalog, selected_streams)

        stream_obj = STREAMS[stream_name]()
        stream_obj.sync_endpoint(client, catalog, config, state, tap_state, selected_streams)
    
        LOGGER.info('FINISHED Syncing: %s', stream_name)

    # remove currently_syncing at the end of the sync
    state = singer.set_currently_syncing(tap_state, None)
    singer.write_state(tap_state)
