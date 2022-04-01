# -*- coding: utf-8 -*-
import ast
import logging
import redis
import psycopg2
import json
import time

from psycopg2 import extras
from psycopg2.extras import LogicalReplicationConnection

from select import select
from datetime import datetime

from .packet import EventWrapper
from .row_event import (
    UpdateRowEvent,
    WriteRowEvent,
    DeleteRowEvent
)

from . import util

logger = logging.getLogger()


class WalJsonError(Exception):
    def __init__(self, err_json):
        super().__init__()
        self.err_json = err_json

class LogicRedisReader(object):

    def __init__(self, config):

        self.config = config
        self.REDIS_SETTINGS = config["REDIS_SETTINGS"]
        self.SELF = config["SELF"]

#       REDIS_SETTINGS
        self.redis_client = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_cloud"],
            password=self.REDIS_SETTINGS["password"]
        )
        self.edge_host = self.REDIS_SETTINGS["edge_host"]
        self.db_replica = self.REDIS_SETTINGS["db_replica"]
        self.db_devlist = self.REDIS_SETTINGS["db_devlist"]

#       SELF_SETTINGS
        self.none_times = 0  # we compute the total sleep in logic_stream
        self.stream_connection = None
        self.replica_redis = None

    def connect_to_cloud(self):
        # init redis client
        self.replica_redis = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_replica"],
            password=self.REDIS_SETTINGS["password"]
        )

    def redis_cloud2edge(self):

        status_interval = 120.0
        while True:
            try:
                keys = self.replica_redis.hkeys(self.edge_host)
            except redis.RedisError as error:
                logger.exception('Load raw replication error')
                self.replica_redis.close()
                continue

            if not keys:
                now = datetime.now()
                timeout = status_interval - (datetime.now() - now).total_seconds()
                try:
                    sel = select([], [], [], max(0, timeout))
                except InterruptedError:
                    pass  # recalculate timeout and continue

            self.dev_redis = redis.StrictRedis(
                host=self.REDIS_SETTINGS["cloud_host"],
                port=self.REDIS_SETTINGS["port"],
                db=self.REDIS_SETTINGS["db_devlist"],
                password=self.REDIS_SETTINGS["password"]
            )
            self.edge_redis = redis.StrictRedis(
                host=self.REDIS_SETTINGS["edge_host"],
                port=self.REDIS_SETTINGS["port"],
                db=self.REDIS_SETTINGS["db_edge"],
                password=self.REDIS_SETTINGS["password"]
            )


            for key in keys:
                row = ast.literal_eval(self.replica_redis.hget(self.edge_host, key).decode("UTF-8"))
                if row['kind'] == "delete":
                    self.delete_redis(row)
                elif row['kind'] == "update":
                    self.update_redis(row)
                self.replica_redis.hdel( self.edge_host, ast.literal_eval(key.decode("UTF-8")) )

    def getList(self, dict):
        return list(dict.keys())


    def delete_redis(self, row):

        remote_host = self.edge_host
        dev_uuid = row['oldkeys']['keyvalues'][0]
        dev_eui = self.dev_redis.hget(remote_host, dev_uuid)
        dev_eui = '*' + dev_eui.decode("UTF-8")  + '*'
        prefixes = self.edge_redis.keys(dev_eui)
        for prefix in prefixes:
            self.edge_redis.delete(prefix.decode("UTF-8"))

        self.dev_redis.hdel(remote_host, dev_uuid)
#        dev_eui = self.dev_redis.hget(remote_host, dev_uuid[0]).decode("utf-8")

#        dev_list = self.dev_redis.hvals(remote_host)
#        dev_count = self.dev_redis.hlen(remote_host)
#        dict = self.getList(self.dev_redis.hgetall(remote_host))

#        for key in range(0,dev_count):
#            value = dev_list[key]
#            print ("VALUE: :", value.decode("utf-8"))
#            if value.decode("utf-8") == dev_eui:
#                uuid = dict[key]
#                print ("UUID: ", uuid.decode("utf-8"))
#                self.dev_redis.hdel(remote_host, uuid.decode("utf-8"))

        return

    def update_redis(self, row):

        dev_eui = '*' + row['columnvalues'][row['columnnames'].index('dev_eui')] + '*'
        prefixes = self.redis_client.keys(dev_eui)
        for prefix in prefixes:
            self.edge_redis.set(prefix.decode("UTF-8"), self.redis_client.get(prefix).decode("UTF-8") )

        return


class LogicStreamReader(object):

    def __init__(self, config,
                 use_add_table_option=True,
                 ):

        self.config = config
        self.DB_SETTINGS = config["DB_SETTINGS"]
        self.REDIS_SETTINGS = config["REDIS_SETTINGS"]
        self.SELF = config["SELF"]

#       DB_SETTINGS
        self.user = self.DB_SETTINGS["user"]
        self.passwd = self.DB_SETTINGS["passwd"]
        self.host = self.DB_SETTINGS["host"]
        self.port = self.DB_SETTINGS["port"]
        self.db = self.DB_SETTINGS["db"]
        self.URL = "postgres://" + self.user +":"+ self.passwd +"@"+ self.host +":"+ self.port +"/"+ self.db + "?sslmode=disable"

#       REDIS_SETTINGS
        self.redis_client = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_cloud"],
            password=self.REDIS_SETTINGS["password"]
        )
        self.edge_host = self.REDIS_SETTINGS["edge_host"]
        self.db_replica = self.REDIS_SETTINGS["db_replica"]
        self.db_devlist = self.REDIS_SETTINGS["db_devlist"]

#       SELF_SETTINGS
        self.slot_name = self.SELF["slot_name"]
        self.none_times = 0  # we compute the total sleep in logic_stream
        self.stream_connection = None
        self.cur = None
        self.start_lsn = self.SELF["start_lsn"]
        self.flush_lsn = self.SELF["start_lsn"]
        self.last_flush_lsn = self.SELF["start_lsn"]
        self.next_lsn = self.SELF["start_lsn"]
        self.connected_stream = False
        self.only_tables = self.SELF["only_tables"]
        self.ignored_tables = self.SELF["ignored_tables"]
        self.only_schemas = self.SELF["only_schemas"]
        self.ignored_schemas = self.SELF["ignored_schemas"]
        self.use_add_table_option = self.SELF["use_add_table_option"]
        self.allowed_events = self.allowed_event_list(
            self.SELF["only_events"], self.SELF["ignored_events"])

        only_schema_tables = []
        # AWS POSTGRES don't support the add-table
        for schema in self.only_schemas:
            for table in self.only_tables:
                only_schema_tables.append("{}.{}".format(schema, table))
        self.add_table_str = ",".join(only_schema_tables)
        self.use_add_table_option = use_add_table_option

    def close(self):
        if self.connected_stream:
            self.conn.close()
            self.connected_stream = False

    def edge_close(self):
        self.edge_redis.close()
        self.dev_redis.close()
        self.replica_redis.close()
        self.redis_client.close()

    def connect_to_stream(self):
        self.conn = psycopg2.connect(
            self.URL,
            connection_factory=psycopg2.extras.LogicalReplicationConnection
        )
        self.cur = self.conn.cursor()
        options = {
            "include-lsn": True,
        }
        if self.use_add_table_option:
            options["add-tables"] = self.add_table_str

        try:
            # test_decoding produces textual output
            self.cur.start_replication(
                slot_name=self.slot_name,
                decode=True,
                start_lsn=self.flush_lsn,   # first we debug don't flush,
                options=options
            )
        except psycopg2.ProgrammingError:
            self.cur.create_replication_slot(slot_name=self.slot_name,
                output_plugin='wal2json')

            self.cur.start_replication(
                slot_name=self.slot_name,
                decode=True,
                start_lsn=self.flush_lsn,   # first we debug don't flush
                options=options
            )
        self.connected_stream = True

        # init redis client
        self.redis_client = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_cloud"],
            password=self.REDIS_SETTINGS["password"]
        )

    def send_feedback(self, lsn=None, keep_live=False):
        if not self.connected_stream:
            self.connect_to_stream()

#        print ("Keep_live:")
#        print (keep_live)
        if keep_live:
            self.cur.send_feedback(reply=True)
            return

        if lsn is None:
            lsn = self.flush_lsn

        if lsn < self.last_flush_lsn:
            self.cur.send_feedback(reply=True)
            return

        self.cur.send_feedback(write_lsn=lsn, flush_lsn=lsn, reply=True)
        self.last_flush_lsn = lsn
        self.flush_lsn = lsn

    def consume(self, msg):
#        print('got replication message of size %d' % msg.data_size)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def fetchone(self):

        status_interval = 10.0
        while True:
            try:
                pkt = self.cur.read_message()
            except psycopg2.DatabaseError as error:
                self.cur.close()
                self.stream_connection.close()
                self.connected_stream = False
                continue
            if pkt:
                self.consume(pkt)
            else:
                now = datetime.now()
                timeout = status_interval - (now - self.cur.feedback_timestamp).total_seconds()
                try:
                    sel = select([self.cur], [], [], max(0, timeout))
                    if not any(sel):
                        self.cur.send_feedback() # timed out, send a keepalive message
                except InterruptedError:
                    pass  # recalculate timeout and continue
            if not pkt:
                # we don't have any data, first send some feedback
                # but when there always no data.
                # the client don't have chance to send_feedback
                # does we need to seed feedback?
                # If we got 30 None we send back the next_lsn
                self.none_times += 1
                # but why 30
                if self.none_times > 30:
                    # when there is no change the next_lsn still can increase
                    self.send_feedback(self.next_lsn)
                    self.none_times = 0
                continue

            else:
                if pkt.data_start > self.flush_lsn:
                    self.flush_lsn = pkt.data_start
                try:
                    payload_json = json.loads(pkt.payload)
                    self.next_lsn = util.str_lsn_to_int(payload_json["nextlsn"])
                    changes = payload_json["change"]

                except json.decoder.JSONDecodeError:
                    # raise it or handle it ?
                    # this may be too ugly may be can use regex
                    # to match '"nextlsn":"2F/804EE880"'
                    # but I think index may be fast here
                    logger.error(pkt.payload[0:2000])
                    next_lsn_index = pkt.payload.index('"nextlsn"')
                    next_lsn_len = len('"nextlsn"')
                    next_lsn = pkt.payload[
                        next_lsn_index + next_lsn_len + 2:  # 2 is :"
                        next_lsn_index + next_lsn_len + 2 + 11  # 11 is the lsn
                    ]
                    self.next_lsn = util.str_lsn_to_int(next_lsn)
                    changes = []

            if changes:
                wraper = EventWrapper(
                    changes,
                    self.allowed_events,
                    self.only_tables,
                    self.ignored_tables,
                    self.only_schemas,
                    self.ignored_schemas)

                for change in changes:
                    if change["kind"] == "delete":
                        self.delete_handler(change)
                    elif change["kind"] == "update":
                        self.update_handler(change)
                    elif (change["kind"] == "insert" and change["kind"] != self.SELF["ignored_events"][0]):
                        self.insert_handler(change)

                if not wraper.events:
                   continue
#                return wraper.events
            else:
                # seem like last wal have finished we send it
                self.send_feedback(self.flush_lsn)

    def allowed_event_list(self, only_events, ignored_events):

        if only_events != "None":
            events = set(only_events)
        else:
            events = {
                UpdateRowEvent,
                WriteRowEvent,
                DeleteRowEvent
            }
        if ignored_events != "None":
            for e in ignored_events:
                if e == "insert":
                    events.remove(WriteRowEvent)
                elif e == "update":
                    events.remove(UpdateRowEvent)
                elif e == "delete":
                    events.remove(DeleteRowEvent)
        return frozenset(events)

    def __iter__(self):
        return iter(self.fetchone, None)

    def delete_handler(self, row):

        self.replica_redis = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_replica"],
            password=self.REDIS_SETTINGS["password"]
        )

        self.dev_redis = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_devlist"],
            password=self.REDIS_SETTINGS["password"]
        )

        remote_host = self.edge_host
        dev_uuid = row["oldkeys"]["keyvalues"]
        dev_eui = self.dev_redis.hget(remote_host, dev_uuid[0]).decode("utf-8")
#        print ("DEV_EUI=", dev_eui)
#        for key in self.replica_redis.scan_iter():
#            print ("KEYS: ", key.decode("utf-8"))

        keys = self.replica_redis.hlen(remote_host)
        keys = keys + 1
        self.replica_redis.hset(remote_host, keys, str(row).encode() )

        self.dev_redis.close()
        self.replica_redis.close()
        return

    def update_handler(self, row):
        dev_uuid = ''
        dev_eui = row['columnvalues'][row['columnnames'].index('dev_eui')]

        try :
            connection = psycopg2.connect(
                user=self.user,
                password=self.passwd,
                host=self.host,
                port=self.port,
                database=self.db
            )
            cursor = connection.cursor()
            postgreSQL_select_Query = "SELECT id FROM end_devices WHERE dev_eui='"+ dev_eui + "'"
            cursor.execute(postgreSQL_select_Query)
            dev_uuid = ''.join(cursor.fetchone())

        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)

        finally:
             # closing database connection.
            if connection:
                cursor.close()
                connection.close()
        # init cloud redis client
        self.replica_redis = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_replica"],
            password=self.REDIS_SETTINGS["password"]
        )

        self.dev_redis = redis.StrictRedis(
            host=self.REDIS_SETTINGS["cloud_host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_devlist"],
            password=self.REDIS_SETTINGS["password"]
        )

        remote_host = row['columnvalues'][row['columnnames'].index('application_server_address')]
        keys = self.replica_redis.hlen(remote_host)
        keys = keys + 1
        self.dev_redis.hset(remote_host, dev_uuid, dev_eui )
        self.replica_redis.hset(remote_host, keys, str(row).encode() )

        self.dev_redis.close()
        self.replica_redis.close()
        return

    def insert_handler(self, row):
        return

