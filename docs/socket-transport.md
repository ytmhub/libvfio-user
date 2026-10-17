# Bounded socket framing

The socket transport uses finite-deadline synchronous framing, not a new
resumable API. This compatibility policy bounds an untrusted local peer's
ability to stall a server event loop. It does not bound application callbacks,
quiesce handlers, or an application's own scheduling latency.

`LIBVFIO_USER_FLAG_ATTACH_NB` preserves immediate idle attach/request polling.
After accept, negotiation receives its complete header and body against one
absolute **250 ms CLOCK_MONOTONIC deadline**. An in-progress request's header
and body share the same 250 ms deadline, starting when input becomes ready.
Blocking-mode servers wait indefinitely between requests; healthy idle sessions
do not time out or reset. Each complete response send has a separate 250 ms
deadline. A synchronous server-to-client exchange similarly has separate send
and receive deadlines, including the optional twin socket. Progress, EINTR,
short writes and slow trickles do not renew a deadline. Negotiation or one
request can therefore occupy the transport for at most 500 ms, plus local
processing/scheduling. These are local robustness bounds, not wire values or
device-platform constants. Blocking-mode partial-frame receives are bounded,
but idle waits and successive valid requests can continue indefinitely by
design. Applications requiring fairness should use `ATTACH_NB`.

Valid fragmented input within the deadline is reassembled. No callback runs
before the entire request arrives. On EOF, expiry, failed writes or malformed
framing the connection is shut down; residual body bytes cannot become another
header. Impossible lengths or unsolicited reply headers may receive the
existing error reply before closure. Header-only remote error responses remain
ordinary protocol errors, not connection failures. SCM_RIGHTS is sent only
with the first successfully written bytes, never again on a partial-write
retry. Received descriptors have atomic close-on-exec, are collected across
fragments, and are closed on truncation or failure. Listeners, accepted sockets
and twin socketpairs use Linux's atomic close-on-exec creation APIs.

An unsuccessful attach closes its accepted socket and permits a fresh attach.
A synchronous `vfu_run_ctx()` whose reply failure reset/detached the socket
reports the documented `ENOTCONN`; the caller must attach again and refresh its
poll descriptor. Successful asynchronous `vfu_device_quiesced()` completion
retains its existing return semantics. There is no partial-frame retry contract
and no new public function, flag or wire extension. A failed send must not be
retried as a new side-effecting request: its callback may already have run.
Normal reset/quiesce callbacks still own device cleanup.

Malformed synchronous replies report `ECONNRESET` after shutting down the
stream. Public DMA operations therefore return `ENOTCONN` and follow the
existing context-reset/quiesce path, detaching both primary and twin sockets.
If quiesce is asynchronous, detach remains deferred until
`vfu_device_quiesced()`. Complete header-only error replies instead preserve
the peer's errno and leave the connection usable; they do not trigger reset.
Successful replies must have the complete expected body length, even when the
expected or supplied body is empty.

The internal `tran_sock_recv()` helper retains its prefix-read convention used
by the sample DMA-write client: it can read a fixed-size body prefix and leave
the remaining command payload for the caller. That caller must consume the tail
before reading another header and owns the tail's receive policy. This helper is not
the server request path: server callbacks still require the entire frame.

## Regressions

`meson test -C BUILD socket_transport_tests --print-errorlogs` runs independent
watchdog-protected receive/send/SCM_RIGHTS cases. It works in ASan/UBSan builds,
including allocation and descriptor cleanup. Python `test_socket_framing.py`
checks zero dispatch of incomplete writes and exactly one dispatch of a valid
fragmented write, and idle blocking sessions followed by valid requests,
partial headers/bodies or EOF. C cases check prefix DMA writes leave the exact
tail and next frame intact, and reject an undersized fixed prefix. Existing
negotiation and request-error tests remain relevant.

Public DMA framing cases exercise both primary and twin sockets, reads and
writes, malformed lengths/IDs/types/error bodies, valid remote errors followed
by a successful exchange, synchronous reset and asynchronous quiesce completion.
They verify disconnect of both channels and immediate reconnect after reset.

The normal-build `sample_migration` functional test runs the shipped client and
servers through migration, including destination CRC validation, the restored
interrupt and destination DMA. It checks completion by the actual client child,
not just the original PID's exit status. The internal migration-write caller
expects a header-only success; DMA replies retain strict expected body lengths.
The full sample has pre-existing migration/region-info allocation leaks, so this
functional test is not registered in sanitizer builds. It can be run separately
with sample leak detection disabled; transport/unit sanitizer leak checks remain
enabled.

An optional integration regression uses an existing fabricd native test fixture
without editing the adapter or proxying traffic:

```sh
python3 libvfio-user/test/socket-integration.py \
  ocp-emulator-core/fabricd/tests/test_pci_function.py \
  out/native/fabricd-pci/fabricd platform/platform-v1.json
```

It independently exercises control response (<2 seconds), SIGINT/SIGTERM
shutdown (<2 seconds while peers stay open), reconnect, split frames, slow
negotiation, EOF, invalid lengths, stalled response readers, socket CLOEXEC and
repeated malformed/descriptor cycles. The executable must load the rebuilt
library; record `ldd`, `/proc/PID/maps`, library hash and source dirty state.
