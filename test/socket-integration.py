#!/usr/bin/env python3
"""Exercise the existing fabricd event-loop seam, without changing core code.

Usage: socket-integration.py CORE_TEST_MODULE FABRICD PLATFORM_JSON
The core native test module supplies only its client and process fixture.
"""
import array
import importlib.util
import json
import os
import pathlib
import signal
import socket
import struct
import sys
import threading
import time
import unittest


module_path = pathlib.Path(sys.argv.pop(1)).resolve()
spec = importlib.util.spec_from_file_location("pci_fixture", module_path)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


def frame(command, payload=b"", length=None, flags=0):
    return struct.pack("<HHIII", 1, command,
                       len(payload) + 16 if length is None else length,
                       flags, 0) + payload


VERSION = frame(1, struct.pack("<HH", 0, 1) +
                b'{"capabilities":{"max_msg_fds":0}}\0')
WRITE = frame(10, struct.pack("<QII", 4, 0, 4) + b"abcd")


class SocketBounds(unittest.TestCase):
    setUp = base.PciTests.setUp
    tearDown = base.PciTests.tearDown
    connect = base.PciTests.connect
    control = base.PciTests.control

    def raw(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect(str(self.path))
        self.clients.append(sock)
        return sock

    def responsive(self):
        start = time.monotonic()
        self.assertTrue(self.control("get_status")["ok"])
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 2)
        print(f"control latency {elapsed:.4f}s", flush=True)

    def reconnect(self):
        for _ in range(100):
            if not self.control("get_status")["result"]["pci"]["connected"]:
                break
            time.sleep(.01)
        else:
            self.fail("expired connection was not detached")
        client = self.connect()
        self.assertEqual(client.word(0), 0x5646494f)
        client.close()
        for _ in range(100):
            if not self.control("get_status")["result"]["pci"]["connected"]:
                break
            time.sleep(.01)
        else:
            self.fail("valid client close was not detached")

    def stop_open(self, sig):
        start = time.monotonic()
        self.proc.send_signal(sig)
        self.assertEqual(self.proc.wait(timeout=2), 0)
        elapsed = time.monotonic() - start
        self.assertFalse(self.path.exists())
        self.assertFalse((self.root / "control.sock").exists())
        print(f"{signal.Signals(sig).name} teardown {elapsed:.4f}s (peer open)", flush=True)

    def stage(self, name):
        if name.startswith("neg_"):
            sock = self.raw()
            data = {
                "neg_silent": b"", "neg_header": VERSION[:1],
                "neg_body": VERSION[:16], "neg_partial_body": VERSION[:21],
            }[name]
        else:
            sock = self.connect().sock
            data = {"request_header": WRITE[:1],
                    "request_body": WRITE[:16],
                    "request_partial_body": WRITE[:-1]}[name]
        if data:
            sock.sendall(data)
        time.sleep(.03)  # Enter the library call, not merely the listen backlog.
        return sock

    def test_fragmented_valid_negotiation_and_requests(self):
        sock = self.raw()

        def send_fragmented(data):
            sock.sendall(data[:1])

            def rest():
                for byte in data[1:]:
                    time.sleep(.001)
                    sock.sendall(bytes([byte]))

            sender = threading.Thread(target=rest)
            sender.start()
            return sender

        def receive_reply():
            def receive(count):
                data = b""
                while len(data) < count:
                    chunk = sock.recv(count - len(data))
                    self.assertTrue(chunk)
                    data += chunk
                return data

            header = struct.unpack("<HHIII", receive(16))
            self.assertEqual(header[3:], (1, 0))
            return receive(header[2] - 16)

        for data in (VERSION, WRITE, frame(9, struct.pack("<QII", 4, 0, 4))):
            sender = send_fragmented(data)
            response = receive_reply()
            sender.join(timeout=1)
            self.assertFalse(sender.is_alive())
        self.assertEqual(response[-4:], b"abcd")
        # A second complete request still decodes correctly after fragmentation.
        sock.sendall(frame(9, struct.pack("<QII", 4, 0, 4)))
        self.assertEqual(receive_reply()[-4:], b"abcd")
        self.responsive()

    def test_slow_trickle_absolute_deadline(self):
        sock = self.raw()
        start = time.monotonic()
        sock.sendall(VERSION[:1])
        stopped = threading.Event()

        def trickle():
            for byte in VERSION[1:]:
                if stopped.wait(.06):
                    break
                try:
                    sock.sendall(bytes([byte]))
                except OSError:
                    break

        sender = threading.Thread(target=trickle)
        sender.start()
        try:
            self.responsive()
            self.assertLess(time.monotonic() - start, .9)
            self.assertEqual(sock.recv(1), b"")
        finally:
            stopped.set()
            sender.join(timeout=1)
        self.reconnect()

    def stalled_responses(self):
        client = self.connect()
        sock = client.sock
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        sock.settimeout(.1)
        request = frame(9, struct.pack("<QII", 0, 7, 256))

        def flood():
            try:
                # Far more response bytes than the Unix socket send buffer.
                sock.sendall(request * 30000)
            except OSError:
                pass

        sender = threading.Thread(target=flood)
        sender.start()
        time.sleep(.12)
        return sender

    def test_peer_stops_reading_responses(self):
        sender = self.stalled_responses()
        try:
            self.responsive()
            self.reconnect()
        finally:
            sender.join(timeout=1)
            self.assertFalse(sender.is_alive())

    def test_descriptor_hygiene_and_repeated_disconnect(self):
        self.responsive()
        baseline = len(list(pathlib.Path(f"/proc/{self.proc.pid}/fd").iterdir()))
        for cycle in range(40):
            sock = self.raw()
            if cycle % 2:
                sock.sendall(VERSION[:1])
            else:
                # Deliver SCM_RIGHTS before EOF in a partial header.
                sock.sendmsg([VERSION[:1]], [(socket.SOL_SOCKET,
                    socket.SCM_RIGHTS, array.array("i", [sock.fileno()]))])
            time.sleep(.003)
            self.assert_cloexec()
            sock.close()
            self.reconnect()
        self.assertEqual(len(list(pathlib.Path(f"/proc/{self.proc.pid}/fd").iterdir())), baseline)

    def assert_cloexec(self, minimum=2):
        directory = pathlib.Path(f"/proc/{self.proc.pid}/fdinfo")
        checked = 0
        for info in directory.iterdir():
            if int(info.name) < 3:
                continue
            try:
                target = os.readlink(f"/proc/{self.proc.pid}/fd/{info.name}")
                if not target.startswith("socket:"):
                    continue  # Audit transport sockets, not unrelated core files.
                flags = next(line.split()[1] for line in info.read_text().splitlines()
                             if line.startswith("flags:"))
            except FileNotFoundError:
                continue  # Descriptor closed during inspection.
            self.assertTrue(int(flags, 8) & os.O_CLOEXEC, (info.name, target))
            checked += 1
        self.assertGreaterEqual(checked, minimum)

    def test_twin_socket_close_on_exec(self):
        sock = self.raw()
        sock.sendall(frame(1, struct.pack("<HH", 0, 1) + json.dumps({
            "capabilities": {"max_msg_fds": 8,
                             "twin_socket": {"supported": True}}
        }).encode() + b"\0"))
        data, ancillary, _, _ = sock.recvmsg(8192, socket.CMSG_SPACE(32), socket.MSG_CMSG_CLOEXEC)
        received = []
        try:
            for level, kind, body in ancillary:
                if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                    values = array.array("i")
                    values.frombytes(body)
                    received.extend(values)
            self.assertEqual(len(received), 1)
            self.assertGreaterEqual(len(data), 16)
            self.assert_cloexec(minimum=4)
            self.responsive()
        finally:
            for fd in received:
                os.close(fd)


def bounded_case(stage, sig=None):
    def run(self):
        self.stage(stage)
        self.assert_cloexec(minimum=3)
        if sig is None:
            self.responsive()
            self.reconnect()
        else:
            self.stop_open(sig)
    return run


for stage in ("neg_silent", "neg_header", "neg_body", "neg_partial_body",
              "request_header", "request_body", "request_partial_body"):
    setattr(SocketBounds, "test_control_" + stage, bounded_case(stage))
    for sig in (signal.SIGTERM, signal.SIGINT):
        setattr(SocketBounds, f"test_{sig.name}_{stage}", bounded_case(stage, sig))


def eof_case(prefix, negotiation=False):
    def run(self):
        sock = self.raw() if negotiation else self.connect().sock
        sock.sendall((VERSION if negotiation else WRITE)[:prefix])
        sock.close()
        self.responsive()
        self.reconnect()
    return run


for stage, prefixes in (("negotiation", (0, 1, 15, 16, 17, len(VERSION) - 1)),
                        ("request", (0, 1, 15, 16, 17, len(WRITE) - 1))):
    for prefix in prefixes:
        setattr(SocketBounds, f"test_eof_{stage}_{prefix}",
                eof_case(prefix, stage == "negotiation"))


def invalid_length_case(length):
    def run(self):
        for flags in (0, 0x10):  # Include NO_REPLY: closure cannot rely on send.
            with self.subTest(flags=flags):
                client = self.connect()
                # Trailing bytes must never be interpreted as a new frame.
                client.sock.sendall(frame(10, WRITE if flags == 0 else b"",
                                          length=length, flags=flags))
                self.responsive()
                self.reconnect()
    return run


for length in (0, 15, 0xffffffff):
    setattr(SocketBounds, f"test_invalid_length_{length}", invalid_length_case(length))


def stop_sending_case(sig):
    def run(self):
        sender = self.stalled_responses()
        try:
            self.stop_open(sig)
        finally:
            sender.join(timeout=1)
            self.assertFalse(sender.is_alive())
    return run


for sig in (signal.SIGTERM, signal.SIGINT):
    setattr(SocketBounds, f"test_{sig.name}_blocked_send", stop_sending_case(sig))


if __name__ == "__main__":
    unittest.main(verbosity=2)
