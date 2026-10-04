# SPDX-FileCopyrightText: 2016 Dmitry Merkurev <didika914@gmail.com>
# SPDX-FileCopyrightText: 2017-2026 Michel Oosterhof <michel@oosterhof.net>
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime

from rethinkdb import r
from twisted.internet import defer
from twisted.logger import Logger

import cowrie.core.output
from cowrie.core.config import CowrieConfig


def iso8601_to_timestamp(value):
    """Unix timestamp for an event timestamp.

    The trailing Z marks the value as UTC, so it is read as UTC: the instant
    stored must not shift with the honeypot host's own timezone.
    """
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    return parsed.replace(tzinfo=UTC).timestamp()


RETHINK_DB_SEGMENT = "output_rethinkdblog"


class Output(cowrie.core.output.Output):
    _log = Logger()

    connection = None

    # noinspection PyAttributeOutsideInit
    def start(self):
        self.host = CowrieConfig.get(RETHINK_DB_SEGMENT, "host")
        self.port = CowrieConfig.getint(RETHINK_DB_SEGMENT, "port")
        self.db = CowrieConfig.get(RETHINK_DB_SEGMENT, "db")
        self.table = CowrieConfig.get(RETHINK_DB_SEGMENT, "table")
        self.password = CowrieConfig.get(RETHINK_DB_SEGMENT, "password", raw=True)
        r.set_loop_type("twisted")
        self.connect()

    @defer.inlineCallbacks
    def connect(self):
        try:
            connection = yield r.connect(
                host=self.host, port=self.port, db=self.db, password=self.password
            )
        except r.ReqlError as e:
            self._log.error("output_rethinkdblog: connect failed: {error}", error=e)
            return
        try:
            yield r.db_create(self.db).run(connection)
            yield r.db(self.db).table_create(self.table).run(connection)
        except r.RqlRuntimeError:
            pass
        self.connection = connection

    def stop(self):
        if self.connection is not None:
            self.connection.close()

    def write(self, event):
        if self.connection is None:
            return

        for i in list(event):
            # remove twisted 15 legacy keys
            if i.startswith("log_"):
                del event[i]

        if "timestamp" in event:
            event["timestamp"] = iso8601_to_timestamp(event["timestamp"])

        d = r.table(self.table).insert(event).run(self.connection)
        d.addErrback(self.insert_failed)

    def insert_failed(self, failure):
        self._log.error(
            "output_rethinkdblog: insert failed: {error}",
            error=failure.getErrorMessage(),
        )
