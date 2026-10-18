#
# Copyright (c) 2021 Nutanix Inc. All rights reserved.
#
# Authors: John Levon <john.levon@nutanix.com>
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#      * Redistributions of source code must retain the above copyright
#        notice, this list of conditions and the following disclaimer.
#      * Redistributions in binary form must reproduce the above copyright
#        notice, this list of conditions and the following disclaimer in the
#        documentation and/or other materials provided with the distribution.
#      * Neither the name of Nutanix nor the names of its contributors may be
#        used to endorse or promote products derived from this software without
#        specific prior written permission.
#
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#  ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
#  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
#  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
#  DAMAGE.
#

from libvfio_user import *
import errno
import json as json_module
import os
import pytest
import socket
import struct

ctx = None


def client_version_json(expect=0, json=''):
    sock = connect_sock()

    payload = struct.pack("HH%dsc" % len(json),
                          LIBVFIO_USER_MAJOR, LIBVFIO_USER_MINOR, json, b'\0')

    hdr = vfio_user_header(VFIO_USER_VERSION, size=len(payload))
    sock.send(hdr + payload)
    vfu_attach_ctx(ctx, expect=expect)
    payload = get_reply(sock, expect=expect)
    sock.close()

    return payload


def test_server_setup():
    global ctx

    ctx = vfu_create_ctx()
    assert ctx is not None

    ret = vfu_setup_device_migration_callbacks(ctx)
    assert ret == 0

    ret = vfu_realize_ctx(ctx)
    assert ret == 0


def test_short_write():
    sock = connect_sock()
    hdr = vfio_user_header(VFIO_USER_VERSION, size=0)
    sock.send(hdr)

    vfu_attach_ctx(ctx, expect=errno.EINVAL)
    get_reply(sock, expect=errno.EINVAL)


def test_long_write():
    sock = connect_sock()
    hdr = vfio_user_header(VFIO_USER_VERSION, size=SERVER_MAX_MSG_SIZE + 1)
    sock.send(hdr)

    ret = vfu_attach_ctx(ctx, expect=errno.EINVAL)
    assert ret == -1
    assert c.get_errno() == errno.EINVAL


def test_bad_command():
    sock = connect_sock()

    payload = struct.pack("HHs", LIBVFIO_USER_MAJOR, LIBVFIO_USER_MINOR, b"")
    hdr = vfio_user_header(999, size=len(payload))
    sock.send(hdr + payload)

    vfu_attach_ctx(ctx, expect=errno.EINVAL)
    get_reply(sock, expect=errno.EINVAL)


def test_invalid_major():
    sock = connect_sock()

    payload = struct.pack("HHs", 999, LIBVFIO_USER_MINOR, b"")
    hdr = vfio_user_header(VFIO_USER_VERSION, size=len(payload))
    sock.send(hdr + payload)

    vfu_attach_ctx(ctx, expect=errno.EINVAL)
    get_reply(sock, expect=errno.EINVAL)


def test_invalid_json_missing_NUL():
    sock = connect_sock()

    payload = struct.pack("HHcc", LIBVFIO_USER_MAJOR, LIBVFIO_USER_MINOR,
                          b"{", b"}")
    hdr = vfio_user_header(VFIO_USER_VERSION, size=len(payload))
    sock.send(hdr + payload)

    vfu_attach_ctx(ctx, expect=errno.EINVAL)
    get_reply(sock, expect=errno.EINVAL)


def test_invalid_json_missing_closing_brace():
    client_version_json(errno.EINVAL,  b"{")


def test_invalid_json_missing_closing_quote():
    client_version_json(errno.EINVAL, b'"')


def test_invalid_json_truncated_irq_clear_capability():
    client_version_json(errno.EINVAL,
                        b'{"capabilities":{"x-msix-clear-pending":true')


def test_invalid_json_bad_capabilities_object():
    client_version_json(errno.EINVAL, b'{ "capabilities": "23" }')


def test_invalid_json_bad_max_fds():
    client_version_json(errno.EINVAL,
                        b'{ "capabilities": { "max_msg_fds": "foo" } }')


def test_invalid_json_bad_max_fds2():
    client_version_json(errno.EINVAL,
                        b'{ "capabilities": { "max_msg_fds": -1 } }')


def test_invalid_json_bad_max_fds3():
    client_version_json(errno.EINVAL,
                        b'{ "capabilities": { "max_msg_fds": %d } }' %
                        (VFIO_USER_CLIENT_MAX_FDS_LIMIT + 1))


def test_invalid_json_bad_migration_object():
    client_version_json(errno.EINVAL,
                        b'{ "capabilities": { "migration": "23" } }')


def test_invalid_json_bad_pgsize():
    client_version_json(errno.EINVAL, b'{ "capabilities": ' +
                        b'{ "migration": { "pgsize": "foo" } } }')


def test_invalid_json_bad_pgsize2():
    client_version_json(errno.EINVAL,
        b'{ "capabilities": { "migration": { "pgsize": 4095 } } }')


def test_valid_negotiate_no_json():
    client = Client(sock=connect_sock())

    payload = struct.pack("HH", LIBVFIO_USER_MAJOR, LIBVFIO_USER_MINOR)
    hdr = vfio_user_header(VFIO_USER_VERSION, size=len(payload))
    client.sock.send(hdr + payload)

    vfu_attach_ctx(ctx)

    payload = get_reply(client.sock)
    (major, minor, json_str, _) = struct.unpack("HH%dsc" % (len(payload) - 5),
                                                payload)
    assert major == LIBVFIO_USER_MAJOR
    assert minor == LIBVFIO_USER_MINOR
    json = parse_json(json_str)
    assert json.capabilities.max_msg_fds == SERVER_MAX_FDS
    assert json.capabilities.max_data_xfer_size == SERVER_MAX_DATA_XFER_SIZE
    assert json.capabilities.migration.pgsize == PAGE_SIZE

    client.disconnect(ctx)


def test_valid_negotiate_empty_json():
    client_version_json(json=b'{}')

    # notice client closed connection
    vfu_run_ctx(ctx, expect=errno.ENOTCONN)


def test_valid_negotiate_json():
    client_version_json(json=bytes(
        '{ "capabilities": { "max_msg_fds": %s, "max_data_xfer_size": %u } }' %
        (VFIO_USER_CLIENT_MAX_FDS_LIMIT, VFIO_USER_DEFAULT_MAX_DATA_XFER_SIZE),
         "utf-8"))

    # notice client closed connection
    vfu_run_ctx(ctx, expect=errno.ENOTCONN)


def test_destroying():
    vfu_destroy_ctx(ctx)


@pytest.mark.parametrize("twin", [False, True])
@pytest.mark.parametrize("clear", [None, False, True],
                         ids=["absent", "false", "true"])
@pytest.mark.parametrize("max_fds", [None, 0, 1, 2],
                         ids=["default-fds", "zero-fds", "one-fd", "two-fds"])
def test_irq_clear_negotiated_fds(twin, clear, max_fds):
    local_ctx = vfu_create_ctx()
    sock = None
    received_fds = []
    try:
        assert vfu_realize_ctx(local_ctx) == 0
        sock = connect_sock()
        caps = {"twin_socket": {"supported": twin}}
        if clear is not None:
            caps["x-msix-clear-pending"] = clear
        if max_fds is not None:
            caps["max_msg_fds"] = max_fds
        encoded = json_module.dumps({"capabilities": caps}).encode() + b'\0'
        payload = struct.pack("HH", LIBVFIO_USER_MAJOR,
                              LIBVFIO_USER_MINOR) + encoded
        sock.sendall(vfio_user_header(VFIO_USER_VERSION,
                                     size=len(payload)) + payload)
        vfu_attach_ctx(local_ctx)
        received_fds, payload = get_reply_fds(sock)
        reply = json_module.loads(payload[4:-1])["capabilities"]
        limit = 1 if max_fds is None else max_fds
        expect_twin = twin and limit > 0
        expect_clear = clear is True and limit > int(expect_twin)
        assert len(received_fds) == int(expect_twin) + int(expect_clear)
        assert ("twin_socket" in reply) == expect_twin
        assert ("x-msix-clear-pending" in reply) == expect_clear
        for name, enabled, index, sock_type in [
            ("twin_socket", expect_twin, 0, socket.SOCK_STREAM),
            ("x-msix-clear-pending", expect_clear, int(expect_twin),
             socket.SOCK_SEQPACKET),
        ]:
            if enabled:
                expected = {"fd_index": index}
                if name == "twin_socket":
                    expected["supported"] = True
                assert reply[name] == expected
                with socket.fromfd(received_fds[index], socket.AF_UNIX,
                                   sock_type) as channel:
                    assert channel.getsockopt(socket.SOL_SOCKET,
                                              socket.SO_TYPE) == sock_type
    finally:
        for fd in received_fds:
            os.close(fd)
        if sock is not None:
            sock.close()
        vfu_destroy_ctx(local_ctx)


@pytest.mark.parametrize("twin", [False, True])
@pytest.mark.parametrize("value", [1, "true", None, {}, []])
def test_invalid_irq_clear_capability(twin, value):
    local_ctx = vfu_create_ctx()
    sock = None
    try:
        assert vfu_realize_ctx(local_ctx) == 0
        sock = connect_sock()
        encoded = json_module.dumps({"capabilities": {
            "twin_socket": {"supported": twin},
            "x-msix-clear-pending": value,
        }}).encode() + b'\0'
        payload = struct.pack("HH", LIBVFIO_USER_MAJOR,
                              LIBVFIO_USER_MINOR) + encoded
        sock.sendall(vfio_user_header(VFIO_USER_VERSION,
                                     size=len(payload)) + payload)
        vfu_attach_ctx(local_ctx, expect=errno.EINVAL)
        received_fds, _ = get_reply_fds(sock, expect=errno.EINVAL)
        try:
            assert received_fds == []
        finally:
            for fd in received_fds:
                os.close(fd)
    finally:
        if sock is not None:
            sock.close()
        vfu_destroy_ctx(local_ctx)

# ex: set tabstop=4 shiftwidth=4 softtabstop=4 expandtab:
