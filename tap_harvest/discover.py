import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_harvest.schema import get_schemas
from tap_harvest.streams import STREAMS
from tap_harvest.exceptions import HarvestUnauthorizedError, HarvestForbiddenError, HarvestNotFoundError

LOGGER = singer.get_logger()


def check_stream_access(client, stream_name, stream_class) -> bool:
    """
    Probes a top-level stream endpoint with per_page=1 to verify the
    credentials have access.
    Returns True if accessible, False on 401/403/404. Any other exception is re-raised.
    Should only be called for top-level streams (those whose parent attribute is empty).
    """
    endpoint = f"{client.base_url}/{stream_class.path}"
    try:
        client.get(endpoint=endpoint, params={"per_page": 1})
        return True
    except (HarvestUnauthorizedError, HarvestForbiddenError, HarvestNotFoundError):
        return False


def discover(client) -> Catalog:
    """Run the discovery mode, prepare the catalog file and return the
    catalog. Probes each top-level stream endpoint to verify access; streams
    that return 401/403 are excluded from the catalog. Child streams are
    included only if their parent stream is accessible.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])
    accessible_streams = set()

    # Two-pass approach: first probe all top-level streams, then process
    # child streams — so parent accessibility is always known before children.
    # A stream is a child if its `parent` attribute is set (non-empty string),
    # regardless of whether '{}' appears in its path.
    top_level = {name: cls for name, cls in STREAMS.items() if not cls.parent}
    child = {name: cls for name, cls in STREAMS.items() if cls.parent}

    for stream_name, stream_class in {**top_level, **child}.items():
        if stream_name not in schemas:
            continue

        schema_dict = schemas[stream_name]

        if stream_class.parent:
            # Child stream: accessible only if its parent was accessible
            if stream_class.parent not in accessible_streams:
                LOGGER.warning(
                    "Stream '%s' will be excluded from the catalog because its "
                    "parent stream '%s' is not accessible.",
                    stream_name,
                    stream_class.parent,
                )
                continue
        elif not check_stream_access(client, stream_name, stream_class):
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )
            continue

        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error(f"stream_name: {stream_name}")
            LOGGER.error(f"type schema_dict: {type(schema_dict)}")
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")
        accessible_streams.add(stream_name)

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    if not catalog.streams:
        raise Exception(
            "No stream endpoints are accessible with the provided credentials. "
            "Verify that the API credentials have the required permissions."
        )

    return catalog
