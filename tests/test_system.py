"""
Tests sin broker Kafka (MemoryBus). Ejecutar desde la raíz del repo:
    python -m unittest discover -s tests -v
"""
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
for p in (ROOT, os.path.join(ROOT, "central"), os.path.join(ROOT, "ws"), os.path.join(ROOT, "fo")):
    sys.path.insert(0, p)

from wm_common import protocol as proto          # noqa: E402
from wm_common.bus import MemoryBus              # noqa: E402
import WM_Central                                # noqa: E402
import WM_WS_M                                   # noqa: E402
import WM_WS_E                                   # noqa: E402
import WM_FO                                     # noqa: E402


def wait_for(cond, timeout=10.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(step)
    return False


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ProtocolTests(unittest.TestCase):
    def test_lrc(self):
        self.assertEqual(proto.lrc(b""), 0)
        self.assertEqual(proto.lrc(b"AB"), 0x41 ^ 0x42)

    def test_frame_roundtrip_and_bad_lrc(self):
        a, b = socket.socketpair()
        ca, cb = proto.Channel(a, 2), proto.Channel(b, 2)

        def server():
            proto.accept_handshake(cb)
            msg = proto.recv_request(cb)
            proto.send_answer(cb, "ECHO#" + msg)

        t = threading.Thread(target=server)
        t.start()
        proto.client_handshake(ca)
        self.assertEqual(proto.request(ca, "HELLO#ñ#1"), "ECHO#HELLO#ñ#1")
        t.join()

    def test_nack_on_corrupt_frame_then_retry(self):
        a, b = socket.socketpair()
        ca, cb = proto.Channel(a, 2), proto.Channel(b, 2)
        good = proto.build_frame("PING")
        bad = good[:-1] + bytes([good[-1] ^ 0xFF])
        a.sendall(bad)
        a.sendall(good)

        self.assertEqual(proto.recv_request(cb), "PING")
        self.assertEqual(ca.read_byte(), proto.NAK)
        self.assertEqual(ca.read_byte(), proto.ACK)


class SystemTests(unittest.TestCase):
    def setUp(self):
        self.bus = MemoryBus()
        self.tmp = tempfile.mkdtemp()
        self.central = WM_Central.Central(os.path.join(self.tmp, "c.db"), self.bus, hb_timeout=3,
                                          default_duration=3, operators=["FO-01", "FO-02"])
        self.central.start(0)
        cport = self.central.socket_port
        self.eport = free_port()
        self.monitor = WM_WS_M.Monitor("WS-01", "River Park", "pw", ("127.0.0.1", cport), self.eport)
        self.monitor.start()
        self.engine = WM_WS_E.Engine(self.bus, ("127.0.0.1", self.eport), base_flow=12, grace=0)
        self.engine.start()
        self.fo = WM_FO.FieldOperator(self.bus, "FO-01", default_duration=2, grace=0)
        self.fo.start()
        st = lambda: self.central.stations.get("WS-01")
        self.assertTrue(wait_for(lambda: st() and st().last_engine_status > 0 and
                                 self.central.state_of(st()) == WM_Central.AVAILABLE), "WS no quedó disponible")
        self.st = st()

    def tearDown(self):
        self.monitor.stop()
        self.engine.stop()

    def state(self):
        return self.central.state_of(self.st)

    def test_full_watering_cycle(self):
        res = self.fo.request_watering("WS-01", 2, timeout=15)
        self.assertEqual(res, "SUMMARY")
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.AVAILABLE))
        row = self.central.db.list_waterings(1)[0]
        self.assertEqual(row["ws_id"], "WS-01")
        self.assertEqual(row["operator_id"], "FO-01")
        self.assertEqual(row["end_reason"], "TIMEOUT")
        self.assertGreater(row["volume_l"], 0.1)

    def test_denied_unknown_operator_and_ws(self):
        bad = WM_FO.FieldOperator(self.bus, "FO-99", grace=0)
        bad.start()
        self.assertEqual(bad.request_watering("WS-01", 2, timeout=5), "DENIED")
        self.assertEqual(self.fo.request_watering("WS-77", 2, timeout=5), "DENIED")

    def test_concurrent_request_is_denied(self):
        fo2 = WM_FO.FieldOperator(self.bus, "FO-02", grace=0)
        fo2.start()
        out = {}
        t = threading.Thread(target=lambda: out.update(a=self.fo.request_watering("WS-01", 3, timeout=15)))
        t.start()
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.WATERING))
        self.assertEqual(fo2.request_watering("WS-01", 2, timeout=5), "DENIED")
        t.join()
        self.assertEqual(out["a"], "SUMMARY")

    def test_leak_during_watering_and_recovery(self):
        out = {}
        t = threading.Thread(target=lambda: out.update(a=self.fo.request_watering("WS-01", 20, timeout=20)))
        t.start()
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.WATERING))
        time.sleep(1.5)
        self.engine.toggle_ko()                       # simula fuga
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.LEAK, 8))
        t.join(15)
        self.assertEqual(out.get("a"), "SUMMARY")     # el riego termina inmediatamente
        self.assertEqual(self.central.db.list_waterings(1)[0]["end_reason"], "LEAK")
        # con fuga no se permite regar
        self.assertEqual(self.fo.request_watering("WS-01", 2, timeout=5), "DENIED")
        self.engine.toggle_ko()                       # reparada
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.AVAILABLE, 8))

    def test_block_and_activate_from_central(self):
        self.central.command("block", "WS-01")
        self.assertEqual(self.state(), WM_Central.OOS)
        self.assertTrue(wait_for(lambda: self.engine.blocked))
        self.assertEqual(self.fo.request_watering("WS-01", 2, timeout=5), "DENIED")
        self.central.command("activate", "WS-01")
        self.assertTrue(wait_for(lambda: not self.engine.blocked))
        self.assertEqual(self.fo.request_watering("WS-01", 2, timeout=15), "SUMMARY")

    def test_central_start_and_manual_engine_start(self):
        self.central.command("start", "WS-01", 2)
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.WATERING))
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.AVAILABLE, 10))
        self.engine.manual_start(30)
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.WATERING))
        time.sleep(1.5)
        self.engine.manual_end()
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.AVAILABLE, 8))
        self.assertEqual(self.central.db.list_waterings(1)[0]["end_reason"], "MANUAL")

    def test_disconnection_and_reconnection(self):
        self.monitor.stop()
        self.assertTrue(wait_for(lambda: self.state() == WM_Central.DISCONNECTED, 8))

    def test_bad_credentials_rejected(self):
        s = socket.create_connection(("127.0.0.1", self.central.socket_port), timeout=3)
        ch = proto.Channel(s, 3)
        proto.client_handshake(ch)
        self.assertEqual(proto.request(ch, "REG#WS-01#Hack#wrong"), "REG_KO#BAD_CREDENTIALS")
        ch.close()


if __name__ == "__main__":
    unittest.main()
