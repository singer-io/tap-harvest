import singer
from tap_harvest.streams import STREAMS

LOGGER = singer.get_logger()

def sync(client, config, catalog, state):
    """
    sync selected streams.
    """
    
    # Get ALL selected streams from catalog
    selected_streams = catalog.get_selected_streams(state)

    for stream_name in selected_streams:
        stream_obj = STREAMS[stream_name]()

        stream_obj.sync_endpoint(client, catalog, config, state, selected_streams)