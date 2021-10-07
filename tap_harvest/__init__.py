#!/usr/bin/env python3

import singer
from singer import utils
from singer.catalog import write_catalog
from tap_harvest.discover import discover
from tap_harvest.sync import sync
from tap_harvest.client import HarvestClient

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "refresh_token",
    "client_id",
    "client_secret",
    "user_agent",
]

def do_discover(client):
    # Discover schemas for all streams and dump catalog
    LOGGER.info("Starting discover")

    catalog = discover(client)
    # dump catalog
    write_catalog(catalog)

    LOGGER.info("Discover completed")

@singer.utils.handle_top_exception(LOGGER)
def main():
    # Main starting poin of tap.
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config
    client = HarvestClient(config['client_id'],
                           config['client_secret'],
                           config['refresh_token'],
                           config['user_agent'])

    if args.discover: # When discover mode called
        do_discover(client)
    else: # When sync mode code called
        catalog = args.catalog if args.catalog else discover(client)
        sync(client, args.config, catalog, args.state)

if __name__ == "__main__":
    main()
