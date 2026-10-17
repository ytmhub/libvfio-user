#!/usr/bin/env python3
# SPDX-License-Identifier: BSD-3-Clause

"""Exercise migration through the shipped samples, including the client child."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


def main():
    client, server = (Path(arg).resolve() for arg in sys.argv[1:])
    # Build-local output and short relative socket names avoid sockaddr_un limits.
    with tempfile.TemporaryDirectory(prefix="sample-migration-",
                                     dir=client.parent.parent) as runtime, \
            tempfile.TemporaryFile(mode="w+") as server_log:
        processes = []
        client_proc = None
        output = ""
        try:
            source = subprocess.Popen([str(server), "-v", "device.sock"],
                                      cwd=runtime, stdout=server_log,
                                      stderr=subprocess.STDOUT,
                                      start_new_session=True)
            processes.append(source)
            deadline = time.monotonic() + 5
            while not (Path(runtime) / "device.sock").is_socket():
                assert source.poll() is None, "source server exited during startup"
                assert time.monotonic() < deadline, "source server startup timeout"
                time.sleep(0.01)

            client_proc = subprocess.Popen([str(client), "device.sock"],
                                           cwd=runtime, stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, text=True,
                                           start_new_session=True)
            processes.append(client_proc)
            # The sample forks: its original PID becomes the destination server.
            # communicate waits for EOF from the actual client child as well;
            # the original PID's zero exit status alone does not prove success.
            output, _ = client_proc.communicate(timeout=40)
            assert client_proc.returncode == 0, "destination server failed"
            assert source.wait(timeout=5) == 0, "source server failed"
        finally:
            # Also kill descendants if the original process already exited.
            for proc in processes:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if client_proc is not None:
                output, _ = client_proc.communicate(timeout=5)
            for proc in processes:
                proc.wait(timeout=5)
            server_log.seek(0)
            server_output = server_log.read()
            print(output, end="")
            print(server_output, end="")

        # This is emitted only after destination CRC validation, RUNNING state,
        # its restored interrupt, DMA operations and final unmap all succeed.
        assert output.count("client: migration and destination DMA completed\n") == 1, \
            "client child did not complete migration and destination DMA"
        for diagnostic in ("ERROR: AddressSanitizer", "LeakSanitizer:",
                           "runtime error:"):
            assert diagnostic not in output + server_output, diagnostic
    print("sample migration: PASS (client child completed)")


if __name__ == "__main__":
    main()
