import singer
from tap_harvest.streams import STREAMS

LOGGER = singer.get_logger()

SUB_STREAMS = {
    'roles': ['user_roles'],
    'users': ['user_projects', 'user_project_tasks'],
    'user_projects': ['user_project_tasks'],
    'invoices': ['invoice_messages', 'invoice_payments', 'invoice_line_items'],
    'estimates': ['estimate_messages', 'estimate_line_items'],
    'time_entries': ['external_reference', 'time_entry_external_reference']
}

SUB_STREAMS_LIST = ['user_roles', 'user_projects', 'user_project_tasks', 'invoice_messages',
                    'invoice_payments', 'invoice_line_items', 'estimate_messages',
                    'estimate_line_items', 'external_reference', 'time_entry_external_reference']

class DependencyException(Exception):
    pass

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract '{0}' data, "
                "to receive '{0}' data, you also need to select '{1}'.")

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog:
        if stream.tap_stream_id == stream_id:
            return stream
    return None

def sync(client, config, catalog, state):
    LOGGER.info("Starting sync")

    selected_streams = list(catalog.get_selected_streams(state))
    selected_streams_ids = [stream.tap_stream_id for stream in selected_streams]

    validate_dependencies(selected_streams_ids)

    # state will preserve state passed in sync mode and
    # tap_state will be updated and written to output based on current sync
    tap_state = state.copy()

    for stream in selected_streams:

        tap_stream_id = stream.tap_stream_id
        stream_schema = stream.schema.to_dict()

        # if it is a "sub_stream", it will be sync'd by its parent
        if tap_stream_id in SUB_STREAMS_LIST:
            continue

        stream_obj = STREAMS[tap_stream_id](client)
        sub_streams_ids = SUB_STREAMS.get(tap_stream_id, None)

        if not sub_streams_ids:
            stream_obj.sync(stream_schema, config, state, tap_state)
        else:
            stream_schemas = {tap_stream_id: stream_schema}

            for sub_stream in sub_streams_ids:
                if sub_stream in selected_streams_ids:
                    sub_stream_schema = get_stream_from_catalog(sub_stream, selected_streams)
                    stream_schemas[sub_stream] = sub_stream_schema.schema.to_dict()

            stream_obj.sync(stream_schemas, config, state, tap_state)
