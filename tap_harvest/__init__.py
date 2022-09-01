#!/usr/bin/env python3

import sys
import json
import singer
from tap_harvest.discover import discover as _discover
from tap_harvest.client import HarvestClient
from tap_harvest.sync import sync as _sync


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "refresh_token",
    "client_id",
    "client_secret",
    "user_agent",
]

def do_discover():
    LOGGER.info('Starting discover')
    catalog = _discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = parsed_args.config
    
    with HarvestClient(config) as client:

            state = {}
            if parsed_args.state:
                state = parsed_args.state
            if parsed_args.discover:
                do_discover()
            elif parsed_args.catalog:
                _sync(client=client,
                        config=config,
                        catalog=parsed_args.catalog,
                        state=state)

if __name__ == '__main__':
    main()