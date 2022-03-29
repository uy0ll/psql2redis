#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# watch psql binlog event, update redis cache
#

from __future__ import print_function
import time
import random
import logging
import sys

from psql2redis import LogicRedisReader

import redis
import yaml

def load_config(file_path):
    f = open(file_path)
    try:
        conf = yaml.load(f, Loader=yaml.FullLoader)
        return conf
    except:
        exit()

def main():

    if len(sys.argv) == 1:
        exit()
    conf = sys.argv[1]

    config = load_config(conf)
    syncer = LogicRedisReader(config)
    syncer.connect_to_cloud()

    print("Starting streaming, press Control-C to end...", file=sys.stderr)

    try:
        syncer.redis_cloud2edge()
    except KeyboardInterrupt:
        syncer.edge_close()
        print("WARNING: Transaction logs will accumulate in pg_xlog "
        "until the slot is dropped.", file=sys.stderr)

if __name__ == "__main__":
    main()
