import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_harvest.schema import get_schemas
from tap_harvest.streams import STREAMS
from tap_harvest.exceptions import HarvestUnauthorizedError, HarvestForbiddenError, HarvestNotFoundError, HarvestError

LOGGER = singer.get_logger()


def check_stream_access(client, stream_name, stream_class) -> bool:
    """Probe a stream endpoint (per_page=1) and return whether it is accessible.
    Returns False on 401/403/404; True on success or any other API error.
    """
    endpoint = f"{client.base_url}/{stream_class.path}"
    try:
        client.get(endpoint=endpoint, params={"per_page": 1})
        return True
    except (HarvestUnauthorizedError, HarvestForbiddenError, HarvestNotFoundError) as err:
        LOGGER.warning(
            "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
            stream_name,
            str(err),
        )
        return False


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Remove child streams from the catalog whose parent stream was excluded."""
    for stream_name, stream_class in list(STREAMS.items()):
        if stream_name in schemas and stream_class.parent and stream_class.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                stream_class.parent,
            )
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Remove inaccessible top-level streams and dependent children in place."""
    inaccessible_streams = [
        stream_name
        for stream_name, stream_class in STREAMS.items()
        if stream_name in schemas and not stream_class.parent and not check_stream_access(client, stream_name, stream_class)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if inaccessible_streams:
        accessible_parents = sum(
            1 for stream_name, stream_class in STREAMS.items()
            if stream_name in schemas and not stream_class.parent
        )
        if accessible_parents == 0:
            raise HarvestForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """Run discovery and exclude streams the credentials cannot read."""
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():

        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error(f"stream_name: {stream_name}")
            LOGGER.error(f"type schema_dict: {type(schema_dict)}")
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
