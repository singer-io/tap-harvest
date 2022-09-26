import singer
from tap_harvest.streams import STREAMS

LOGGER = singer.get_logger()


def is_any_child_selected(stream_obj, selected_streams):
    """
    Check if any of the child streams is selected for the parent.
    """
    if stream_obj.children:
        for child in stream_obj.children:
            if child in selected_streams:
                # Return true if child is selected
                return True

            if STREAMS[child].children:
                # Check for the nested child
                return is_any_child_selected(STREAMS[child], selected_streams)
    return False


def get_streams_to_sync(selected_streams, last_stream=None):
    """
    Get lists of streams to call the sync method.
    For children, ensure that dependent parent_stream is included even
    if it is not selected.
    """
    streams_to_sync = []
    # Loop through all the streams
    for stream_name, stream_obj in STREAMS.items():
        if stream_name in selected_streams or is_any_child_selected(stream_obj, selected_streams):
            # Append the selected stream or deselected parent stream into the list,
            # if its child or nested child is selected.
            streams_to_sync.append(stream_name)

    if last_stream:
        # If currently syncing stream is available
        # Set list in the order
        index = streams_to_sync.index(last_stream)
        return streams_to_sync[index:]+streams_to_sync[:index]
    return streams_to_sync

def write_schemas_recursive(stream_id, catalog, selected_streams):
    """
    Write the schemas for the selected parent and it's all child.
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

    # Get the list of streams(to sync stream itself or its child stream)
    # for which sync method needs to be called
    streams_to_sync = get_streams_to_sync(selected_streams, last_stream)

    # Loop through all `streams_to_sync` streams
    for stream_name in streams_to_sync:

        stream_obj = STREAMS[stream_name]()
        if stream_obj.parent:
            # Skip sync if stream is child of another stream
            continue

        LOGGER.info('START Syncing: %s', stream_name)
        # Set currently syncing stream
        tap_state = singer.set_currently_syncing(tap_state, stream_name)
        singer.write_state(tap_state)

        write_schemas_recursive(stream_name, catalog, selected_streams)

        stream_obj.sync_endpoint(client, catalog, config, state, tap_state,
                                 selected_streams, streams_to_sync)

        LOGGER.info('FINISHED Syncing: %s', stream_name)

        singer.write_state(tap_state)
    # remove currently_syncing at the end of the sync
    tap_state = singer.set_currently_syncing(tap_state, None)
    singer.write_state(tap_state)
