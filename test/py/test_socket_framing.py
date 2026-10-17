"""Fragmented writes dispatch exactly once, incomplete writes never dispatch."""
import errno
import mmap
import struct
import threading
import time
from unittest.mock import patch

import pytest
from libvfio_user import *


@pytest.mark.parametrize("prefix", [1, 15, 16, 17, 32, 35, None])
def test_fragmented_write_callback(prefix):
    ctx = vfu_create_ctx(flags=LIBVFIO_USER_FLAG_ATTACH_NB)
    assert ctx is not None
    client = None
    sender = None
    try:
        assert vfu_pci_init(ctx) == 0
        assert vfu_setup_region(ctx, index=VFU_PCI_DEV_BAR0_REGION_IDX,
                                size=4096, flags=VFU_REGION_FLAG_RW) == 0
        assert vfu_realize_ctx(ctx) == 0
        client = connect_client(ctx)
        payload = struct.pack("QII", 0, 0, 4) + b"abcd"
        frame = vfio_user_header(VFIO_USER_REGION_WRITE, size=len(payload)) + payload
        with patch("libvfio_user.pci_region_cb", return_value=4) as callback:
            if prefix is None:
                client.sock.sendall(frame[:1])

                def send_rest():
                    for byte in frame[1:]:
                        time.sleep(.001)
                        client.sock.sendall(bytes([byte]))

                sender = threading.Thread(target=send_rest)
                sender.start()
                assert vfu_run_ctx(ctx) == 1
                sender.join(timeout=1)
                assert not sender.is_alive()
                get_reply(client.sock)
                callback.assert_called_once()
                assert vfu_run_ctx(ctx) == 0  # No duplicate dispatch on idle poll.
                callback.assert_called_once()
            else:
                client.sock.sendall(frame[:prefix])
                start = time.monotonic()
                vfu_run_ctx(ctx, expect=errno.ENOTCONN)
                assert time.monotonic() - start < .8
                callback.assert_not_called()
    finally:
        if client is not None:
            client.sock.close()
        if sender is not None:
            sender.join(timeout=1)
        vfu_destroy_ctx(ctx)


@pytest.fixture(params=[False, True], ids=["primary", "twin"])
def dma_connection(request):
    ctx = prepare_ctx_for_dma()
    client = None
    try:
        caps = {"capabilities": {"twin_socket": {"supported": request.param}}}
        client = connect_client(ctx, caps)
        sock = client.client_cmd_socket if request.param else client.sock
        assert sock is not None
        client.sock.settimeout(1)
        sock.settimeout(1)
        mapping = vfio_user_dma_map(argsz=len(vfio_user_dma_map()),
                                    flags=VFIO_USER_F_DMA_REGION_READ |
                                          VFIO_USER_F_DMA_REGION_WRITE,
                                    addr=0x10000, size=PAGE_SIZE)
        msg(ctx, client.sock, VFIO_USER_DMA_MAP, mapping)
        ret, sg = vfu_addr_to_sgl(ctx, dma_addr=mapping.addr, length=4,
                                 max_nr_sgs=1,
                                 prot=mmap.PROT_READ | mmap.PROT_WRITE)
        assert ret == 1
        yield ctx, client, sock, sg
    finally:
        if client is not None:
            client.sock.close()
            if client.client_cmd_socket is not None:
                client.client_cmd_socket.close()
        vfu_destroy_ctx(ctx)


def dma_exchange(connection, direction, reply_kind):
    """One bounded public DMA operation, with an independently malformed peer."""
    ctx, _, sock, sg = connection
    errors = []

    def respond():
        try:
            _, msg_id, cmd, payload = get_msg_fds(sock, VFIO_USER_F_TYPE_COMMAND)
            expected_cmd = (VFIO_USER_DMA_READ if direction == "read"
                            else VFIO_USER_DMA_WRITE)
            assert cmd == expected_cmd
            body = payload[:c.sizeof(vfio_user_dma_region_access)]
            if direction == "read":
                body += b"abcd"
            flags = VFIO_USER_F_TYPE_REPLY
            error = 0
            if reply_kind == "short":
                body = body[:-1]
            elif reply_kind == "long":
                body += b"x"
            elif reply_kind == "empty":
                body = b""
            elif reply_kind == "wrong_id":
                msg_id += 1
            elif reply_kind == "wrong_type":
                flags = VFIO_USER_F_TYPE_COMMAND
            elif reply_kind == "error_body":
                error = errno.EINVAL
            elif isinstance(reply_kind, int):
                error = reply_kind
                body = b""
            hdr = vfio_user_header(cmd, size=len(body), msg_type=flags,
                                   msg_id=msg_id, error=error != 0,
                                   error_no=error)
            if reply_kind == "invalid_size":
                hdr = hdr[:4] + struct.pack("I", 15) + hdr[8:]
            elif reply_kind == "oversized_frame":
                hdr = hdr[:4] + struct.pack("I", 0xffffffff) + hdr[8:]
            sock.sendall(hdr + body)
        except Exception as exc:
            errors.append(exc)

    responder = threading.Thread(target=respond)
    responder.start()
    try:
        if direction == "read":
            ret, data = vfu_sgl_read(ctx, sg, 1, 0)
        else:
            ret = vfu_sgl_write(ctx, sg, 1, bytearray(b"abcd"), 0)
            data = None
        error = c.get_errno()
    finally:
        responder.join(timeout=2)
    assert not responder.is_alive()
    assert not errors, errors
    return ret, error, data


def assert_socket_disconnected(sock):
    # Unix streams can report reset rather than EOF when unread bytes remain.
    try:
        assert sock.recv(1) == b""
    except ConnectionResetError:
        pass


@pytest.mark.parametrize("direction", ["read", "write"])
@pytest.mark.parametrize("reply_kind", ["short", "long", "empty", "wrong_id",
                                       "wrong_type", "error_body", "invalid_size",
                                       "oversized_frame"])
def test_malformed_dma_reply_resets_connection(dma_connection, direction, reply_kind):
    ctx, client, sock, _ = dma_connection
    with patch("libvfio_user.reset_cb", return_value=0) as reset, \
         patch("libvfio_user.quiesce_cb", return_value=0) as quiesce:
        ret, error, _ = dma_exchange(dma_connection, direction, reply_kind)
        assert (ret, error) == (-1, errno.ENOTCONN)
        quiesce.assert_called_once_with(ctx)
        reset.assert_called_once_with(ctx, VFU_RESET_LOST_CONN)
        assert_socket_disconnected(client.sock)
        if sock is not client.sock:
            assert_socket_disconnected(sock)
        # The listener is reusable immediately, not after a second DMA attempt.
        replacement = connect_client(ctx)
        try:
            assert vfu_run_ctx(ctx) == 0
        finally:
            replacement.sock.close()


@pytest.mark.parametrize("direction", ["read", "write"])
@pytest.mark.parametrize("error_no", [errno.EINVAL, errno.EIO])
def test_complete_dma_error_keeps_connection(dma_connection, direction, error_no):
    with patch("libvfio_user.reset_cb", return_value=0) as reset, \
         patch("libvfio_user.quiesce_cb", return_value=0) as quiesce:
        ret, error, _ = dma_exchange(dma_connection, direction, error_no)
        assert (ret, error) == (-1, error_no)
        reset.assert_not_called()
        quiesce.assert_not_called()
        ret, _, data = dma_exchange(dma_connection, direction, "valid")
        assert ret == 0
        if direction == "read":
            assert data == b"abcd"
        reset.assert_not_called()


def test_malformed_dma_reply_defers_reset_until_quiesced(dma_connection):
    ctx, client, _, _ = dma_connection

    def busy(_):
        c.set_errno(errno.EBUSY)
        return -1

    with patch("libvfio_user.reset_cb", return_value=0) as reset, \
         patch("libvfio_user.quiesce_cb", side_effect=busy) as quiesce:
        ret, error, _ = dma_exchange(dma_connection, "read", "short")
        assert (ret, error) == (-1, errno.ENOTCONN)
        quiesce.assert_called_once_with(ctx)
        reset.assert_not_called()
        assert vfu_device_quiesced(ctx, 0) == 0
        reset.assert_called_once_with(ctx, VFU_RESET_LOST_CONN)
        assert_socket_disconnected(client.sock)
        if client.client_cmd_socket is not None:
            assert_socket_disconnected(client.client_cmd_socket)


@pytest.mark.parametrize("ending", ["partial_header", "partial_body", "eof"])
def test_blocking_idle_preserves_session(ending):
    """Idle blocking sessions survive; only an in-progress frame is timed out."""
    ctx = vfu_create_ctx(flags=0)
    assert ctx is not None
    client = None
    worker = None
    result = []
    try:
        assert vfu_pci_init(ctx) == 0
        assert vfu_setup_region(ctx, index=VFU_PCI_DEV_BAR0_REGION_IDX,
                                size=4096, flags=VFU_REGION_FLAG_RW) == 0
        assert vfu_realize_ctx(ctx) == 0
        client = connect_client(ctx)
        client.sock.settimeout(1)
        payload = struct.pack("QII", 0, 0, 4) + b"abcd"
        frame = vfio_user_header(VFIO_USER_REGION_WRITE, size=len(payload)) + payload

        def serve():
            ret = lib.vfu_run_ctx(ctx)
            result.append((ret, c.get_errno()))

        with patch("libvfio_user.pci_region_cb", return_value=4) as callback:
            worker = threading.Thread(target=serve)
            worker.start()
            time.sleep(.4)  # Longer than the frame deadline, without a frame.
            assert worker.is_alive()
            callback.assert_not_called()
            client.sock.sendall(frame)
            get_reply(client.sock)
            callback.assert_called_once()
            time.sleep(.4)  # The same session must also survive its next idle wait.
            assert worker.is_alive()
            start = time.monotonic()
            if ending == "eof":
                client.sock.close()
            else:
                prefix = 1 if ending == "partial_header" else 17
                client.sock.sendall(frame[:prefix])
            worker.join(timeout=.8)
            assert not worker.is_alive()
            assert time.monotonic() - start < .8
            assert result == [(-1, errno.ENOTCONN)]
            callback.assert_called_once()
    finally:
        if client is not None:
            client.sock.close()
        if worker is not None:
            worker.join(timeout=1)
            assert not worker.is_alive()
        vfu_destroy_ctx(ctx)
