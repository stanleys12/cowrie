# SPDX-FileCopyrightText: 2026 Michel Oosterhof <michel@oosterhof.net>
#
# SPDX-License-Identifier: BSD-3-Clause

# ABOUTME: The rethinkdblog output plugin must shut down cleanly when start()
# ABOUTME: never connected to the database, and must not block the reactor.

from __future__ import annotations

import os
import sys
import tempfile
import types
import unittest
from typing import Any
from unittest.mock import Mock, patch

from twisted.internet import defer

os.environ["COWRIE_HONEYPOT_DATA_PATH"] = "data"
os.environ["COWRIE_HONEYPOT_DOWNLOAD_PATH"] = tempfile.gettempdir()
os.environ["COWRIE_SHELL_FILESYSTEM"] = "src/cowrie/data/fs.pickle"

try:
    import rethinkdb  # noqa: F401
except ImportError:
    # The driver is an optional dependency; stub it so the plugin imports.
    stub = types.ModuleType("rethinkdb")
    stub.r = Mock()  # type: ignore[attr-defined]
    sys.modules["rethinkdb"] = stub

from cowrie.output import rethinkdblog


def _make() -> Any:
    with patch.object(rethinkdblog.Output, "start", lambda self: None):
        return rethinkdblog.Output()


def _mock_r() -> Mock:
    return Mock(ReqlError=Exception, RqlRuntimeError=RuntimeError)


class OutputRethinkdblogHardeningTests(unittest.TestCase):
    def test_stop_without_successful_start(self) -> None:
        out = _make()
        out.stop()

    def test_write_before_connect_is_dropped(self) -> None:
        out = _make()
        out.host, out.port, out.db, out.password = "localhost", 28015, "cowrie", ""
        out.table = "events"
        r = _mock_r()
        pending: defer.Deferred = defer.Deferred()
        r.connect.return_value = pending
        with patch.object(rethinkdblog, "r", r):
            out.connect()
            out.write({"eventid": "t"})
        self.assertIsNone(out.connection)
        r.table.assert_not_called()

    def test_connect_failure_is_logged(self) -> None:
        out = _make()
        out.host, out.port, out.db, out.password = "localhost", 28015, "cowrie", ""
        r = _mock_r()
        r.connect.return_value = defer.fail(Exception("refused"))
        with (
            patch.object(rethinkdblog, "r", r),
            patch.object(rethinkdblog.Output, "_log") as log,
        ):
            d = out.connect()
        seen: list = []
        d.addBoth(seen.append)
        self.assertEqual(seen, [None])
        self.assertIsNone(out.connection)
        log.error.assert_called_once()

    def test_insert_failure_is_logged(self) -> None:
        out = _make()
        out.connection = Mock()
        out.table = "events"
        r = _mock_r()
        d_fail: defer.Deferred = defer.fail(Exception("timeout"))
        r.table.return_value.insert.return_value.run.return_value = d_fail
        with (
            patch.object(rethinkdblog, "r", r),
            patch.object(rethinkdblog.Output, "_log") as log,
        ):
            out.write({"eventid": "t", "timestamp": "2026-01-01T00:00:00.000000Z"})
        r.table.return_value.insert.assert_called_once_with(
            {"eventid": "t", "timestamp": 1767225600.0}
        )
        log.error.assert_called_once()
        seen: list = []
        d_fail.addBoth(seen.append)
        self.assertEqual(seen, [None])


if __name__ == "__main__":
    unittest.main()
