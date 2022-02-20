# -*- coding: utf-8 -*-

import psycopg2
import json
import logging
import redis
import psycopg2
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
            host=self.REDIS_SETTINGS["host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db"],
            password=self.REDIS_SETTINGS["password"]
        )
        self.log_pos_prefix=self.REDIS_SETTINGS["log_pos_prefix"],
        self.server_id=self.REDIS_SETTINGS["server_id"]

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
                start_lsn=flush_lsn,   # first we debug don't flush
                options=options
            )
        self.connected_stream = True

        # init redis client
        self.redis_client = redis.StrictRedis(
            host=self.REDIS_SETTINGS["host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db"],
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
                    prefix = "%s:%s:" % (change["schema"], change["table"])

                    if change["kind"] == "delete":
                        print ( change['oldkeys']['keyvalues'] )
                        self.delete_handler(change)

                    elif change["kind"] == "update":
                        self.update_handler(change)

                    elif change["kind"] == "insert":
                        self.insert_handler(change)

                if not wraper.events:
                    continue
                return wraper.events

            else:
                # seem like last wal have finished we send it
                self.send_feedback(self.flush_lsn)


    def allowed_event_list(self, only_events, ignored_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = {
                UpdateRowEvent,
                WriteRowEvent,
                DeleteRowEvent,
            }
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)

        return frozenset(events)

    def __iter__(self):
        return iter(self.fetchone, None)

    def delete_handler(self, row):
         print (row)
#        self.redis_client.delete(prefix + str(vals["keyvalues"]))

    def update_handler(self, row):
        self.redis_prefix(row)

    def insert_handler(self, row):
#        print ( row['columnvalues'][row['columnnames'].index('id')] )
#        adr = prefix + '@' + row['columnvalues'][val]
        self.redis_prefix(row)

    def set_log_pos(self, log_file, log_pos):
        key = "%s%s" % (self.log_pos_prefix, self.server_id)
        self.redis_client.hmset(key, {'log_pos': log_pos, 'log_file': log_file})

    def get_log_pos(self):
        key = "%s%s" % (self.log_pos_prefix, self.server_id)
        ret = self.redis_client.hgetall(key)
        return self.redis_client.get("log_file"), ret.get("log_pos")

    def redis_prefix(self, changes):

        # init remote redis client
        self.remote_redis = redis.StrictRedis(
            host=remote_host,
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db_remote"],
            password=self.REDIS_SETTINGS["password"]
        )

        dev_eui = changes['columnvalues'][changes['columnnames'].index('dev_eui')]
        join_eui = changes['columnvalues'][changes['columnnames'].index('join_eui')]
        application_id = changes['columnvalues'][changes['columnnames'].index('application_id')]
        device_id = changes['columnvalues'][changes['columnnames'].index('device_id')]
        remote_host = changes['columnvalues'][changes['columnnames'].index('application_server_address')]

        as_dev = "ttn:v3:as:devices:eui:" + dev_eui + ":"+ join_eui
        as_uid = "ttn:v3:as:devices:uid:" + application_id +':'+ device_id
        js_dev = "ttn:v3:js:devices:eui:" + join_eui +":"+ dev_eui
        js_uid = "ttn:v3:js:devices:uid:" + application_id +':'+ device_id
        ns_dev = "ttn:v3:ns:devices:eui:" + join_eui +":"+ dev_eui
        ns_uid = "ttn:v3:ns:devices:uid:" + application_id +":"+ device_id

        val = self.redis_client.get(as_dev)
        self.remote_redis.set(as_dev, val)
        val = self.redis_client.get(as_uid)
        self.remote_redis.set(as_uid, val)

        val = self.redis_client.get(js_dev)
        self.remote_redis.set(js_dev, val)
        val = self.redis_client.get(js_uid)
        self.remote_redis.set(js_uid, val)

        val = self.redis_client.get(ns_dev)
        self.remote_redis.set(ns_dev, val)
        val = self.redis_client.get(ns_uid)
        self.remote_redis.set(ns_uid, val)

        self.remote_redis.close()
        return
