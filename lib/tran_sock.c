/*
 * Copyright (c) 2020 Nutanix Inc. All rights reserved.
 *
 * Authors: Thanos Makatos <thanos@nutanix.com>
 *          Swapnil Ingle <swapnil.ingle@nutanix.com>
 *          Felipe Franciosi <felipe@nutanix.com>
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *      * Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *      * Redistributions in binary form must reproduce the above copyright
 *        notice, this list of conditions and the following disclaimer in the
 *        documentation and/or other materials provided with the distribution.
 *      * Neither the name of Nutanix nor the names of its contributors may be
 *        used to endorse or promote products derived from this software without
 *        specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 *  SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *  CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *  LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 *  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 *  DAMAGE.
 *
 */

#include <sys/param.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "tran_sock.h"

typedef struct {
    int listen_fd;
    int conn_fd;
    int client_cmd_socket_fd;
    int64_t request_deadline;
    bool poisoned;
} tran_sock_t;

/* A compatibility bound, not a wire-protocol or device-platform constant. */
#define FRAME_TIMEOUT_MS 250

static int64_t
monotonic_ms(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        abort();
    }
    return (int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

/* Poison a possibly partial frame. Never let its remainder become a header. */
static int
disconnect_socket(int fd)
{
    (void)shutdown(fd, SHUT_RDWR);
    return ERROR_INT(ECONNRESET);
}

static int
wait_socket(int fd, short events, int64_t deadline)
{
    struct pollfd pfd = { .fd = fd, .events = events };
    int ret;

    for (;;) {
        int64_t remaining = deadline - monotonic_ms();

        if (remaining <= 0) {
            return disconnect_socket(fd);
        }
        ret = poll(&pfd, 1, remaining);
        if (ret > 0) {
            return 0; /* recv/send reports EOF or the precise socket error. */
        }
        if (ret < 0 && errno == EINTR) {
            continue;
        }
        return disconnect_socket(fd);
    }
}

int
tran_sock_send_iovec(int sock, uint16_t msg_id, bool is_reply,
                     enum vfio_user_command cmd,
                     struct iovec *iovecs, size_t nr_iovecs,
                     int *fds, int count, int err)
{
    int ret;
    struct vfio_user_header hdr = { .msg_id = msg_id };
    struct msghdr msg;
    size_t i;
    size_t size = count * sizeof(*fds);
    char *buf;
    struct iovec *pending;
    int64_t deadline = monotonic_ms() + FRAME_TIMEOUT_MS;

    if (nr_iovecs == 0) {
        iovecs = alloca(sizeof(*iovecs));
        nr_iovecs = 1;
    }

    memset(&msg, 0, sizeof(msg));

    if (is_reply) {
        hdr.flags |= VFIO_USER_F_TYPE_REPLY;
        hdr.cmd = cmd;
        if (err != 0) {
            hdr.flags |= VFIO_USER_F_ERROR;
            hdr.error_no = err;
        }
    } else {
        hdr.cmd = cmd;
        hdr.flags |= VFIO_USER_F_TYPE_COMMAND;
    }

    iovecs[0].iov_base = &hdr;
    iovecs[0].iov_len = sizeof(hdr);

    for (i = 0; i < nr_iovecs; i++) {
        hdr.msg_size += iovecs[i].iov_len;
    }

    msg.msg_iovlen = nr_iovecs;
    msg.msg_iov = iovecs;

    if (fds != NULL) {
        size_t cmsg_space_aligned = MAX(CMSG_SPACE(size), sizeof(struct cmsghdr));

        buf = alloca(cmsg_space_aligned);
        memset(buf, 0, cmsg_space_aligned);

        msg.msg_control = buf;
        msg.msg_controllen = CMSG_SPACE(size);

        struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
        /* Quieten clang-analyzer. */
        assert(cmsg != NULL);
        cmsg->cmsg_level = SOL_SOCKET;
        cmsg->cmsg_type = SCM_RIGHTS;
        cmsg->cmsg_len = CMSG_LEN(size);
        memcpy(CMSG_DATA(cmsg), fds, size);
    }

    /* Work on a copy: advancing a partial write must not mutate caller iovecs. */
    pending = calloc(nr_iovecs, sizeof(*pending));
    if (pending == NULL) {
        return -1;
    }
    memcpy(pending, iovecs, nr_iovecs * sizeof(*pending));
    msg.msg_iov = pending;

    while (msg.msg_iovlen != 0) {
        if (monotonic_ms() >= deadline) {
            goto disconnected;
        }
        ret = sendmsg(sock, &msg, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            if ((errno == EAGAIN || errno == EWOULDBLOCK) &&
                wait_socket(sock, POLLOUT, deadline) == 0) {
                continue;
            }
            goto disconnected;
        }
        if (ret == 0) {
            goto disconnected;
        }
        /* SCM_RIGHTS belongs to the first successfully sent byte only. */
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
        while (msg.msg_iovlen != 0 &&
               (size_t)ret >= msg.msg_iov[0].iov_len) {
            ret -= msg.msg_iov[0].iov_len;
            msg.msg_iov++;
            msg.msg_iovlen--;
        }
        if (msg.msg_iovlen != 0) {
            msg.msg_iov[0].iov_base = (char *)msg.msg_iov[0].iov_base + ret;
            msg.msg_iov[0].iov_len -= ret;
        }
    }
    free(pending);
    return 0;

disconnected:
    free(pending);
    return disconnect_socket(sock);
}

int
tran_sock_send(int sock, uint16_t msg_id, bool is_reply,
               enum vfio_user_command cmd,
               void *data, size_t data_len)
{
    /* [0] is for the header. */
    struct iovec iovecs[2] = {
        [1] = {
            .iov_base = data,
            .iov_len = data_len
        }
    };
    return tran_sock_send_iovec(sock, msg_id, is_reply, cmd, iovecs,
                                ARRAY_SIZE(iovecs), NULL, 0, 0);
}

static int
get_msg(void *data, size_t len, int *fds, size_t *nr_fds, int sock_fd,
        int sock_flags, int64_t deadline)
{
    int ret;
    size_t capacity = nr_fds == NULL ? 0 : *nr_fds;
    size_t received = 0, done = 0;
    /* Fixed protocol limit also bounds stack use for untrusted ancillary data. */
    union {
        struct cmsghdr align;
        char bytes[CMSG_SPACE(sizeof(int) * VFIO_USER_CLIENT_MAX_MSG_FDS_LIMIT)];
    } control;

    if (nr_fds != NULL) {
        *nr_fds = 0;
    }

    while (done < len) {
        struct iovec iov = { .iov_base = (char *)data + done,
                             .iov_len = len - done };
        struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1,
                             .msg_control = &control,
                             .msg_controllen = sizeof(control) };
        struct cmsghdr *cmsg;
        bool invalid = false;

        if (monotonic_ms() >= deadline) {
            goto disconnected;
        }
        ret = recvmsg(sock_fd, &msg, MSG_DONTWAIT | MSG_CMSG_CLOEXEC);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                goto disconnected;
            }
            /* Preserve the public idle nonblocking poll contract. */
            if (done == 0 && (sock_flags & MSG_DONTWAIT)) {
                return -1;
            }
            if (wait_socket(sock_fd, POLLIN, deadline) < 0) {
                goto disconnected;
            }
            continue;
        }
        if (ret == 0) {
            goto disconnected;
        }
        /* Parse even truncated/short messages: already delivered FDs are ours. */
        for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
            if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
                continue;
            }
            size_t size = cmsg->cmsg_len - CMSG_LEN(0);
            size_t i;

            if (size % sizeof(int) != 0 || size == 0) {
                invalid = true;
            }
            for (i = 0; i < size / sizeof(int); i++) {
                int fd;

                memcpy(&fd, (char *)CMSG_DATA(cmsg) + i * sizeof(fd), sizeof(fd));
                if (received < capacity) {
                    fds[received++] = fd;
                } else {
                    close(fd);
                    invalid = true;
                }
            }
        }
        if (invalid || (msg.msg_flags & (MSG_CTRUNC | MSG_TRUNC))) {
            goto disconnected;
        }
        done += ret;
    }
    if (nr_fds != NULL) {
        *nr_fds = received;
    }
    return done;

disconnected:
    while (received != 0) {
        close(fds[--received]);
    }
    return disconnect_socket(sock_fd);
}

/*
 * Receive a vfio-user message.  If "len" is set to non-zero, the message should
 * include data of that length, which is stored in the pre-allocated "data"
 * pointer.
 */
static int
tran_sock_recv_fds(int sock, struct vfio_user_header *hdr, bool is_reply,
                   uint16_t *msg_id, void *data, size_t *len, int *fds,
                   size_t *nr_fds, int64_t deadline)
{
    int ret, error;

    /* FIXME if ret == -1 then fcntl can overwrite recv's errno */

    ret = get_msg(hdr, sizeof(*hdr), fds, nr_fds, sock, 0, deadline);
    if (ret < 0) {
        return ret;
    }

    if (is_reply) {
        if (msg_id != NULL && hdr->msg_id != *msg_id) {
            error = EPROTO;
            goto malformed;
        }

        if ((hdr->flags & VFIO_USER_F_TYPE_MASK) != VFIO_USER_F_TYPE_REPLY) {
            error = EINVAL;
            goto malformed;
        }

        if (hdr->flags & VFIO_USER_F_ERROR) {
            if (hdr->error_no <= 0 || hdr->error_no > SERVER_MAX_ERROR_NO) {
                hdr->error_no = EINVAL;
            }
            /* A complete header-only error reply does not desynchronize I/O. */
            if (hdr->msg_size == sizeof(*hdr)) {
                if (nr_fds != NULL) {
                    while (*nr_fds != 0) {
                        close(fds[--*nr_fds]);
                    }
                }
                return ERROR_INT(hdr->error_no);
            }
            error = hdr->error_no;
            goto malformed;
        }
    } else {
        if ((hdr->flags & VFIO_USER_F_TYPE_MASK) != VFIO_USER_F_TYPE_COMMAND) {
            error = EINVAL;
            goto malformed;
        }
        if (msg_id != NULL) {
            *msg_id = hdr->msg_id;
        }
    }

    if (hdr->msg_size < sizeof(*hdr) || hdr->msg_size > SERVER_MAX_MSG_SIZE) {
        error = EINVAL;
        goto malformed;
    }

    /* Replies are complete exchanges, including zero-length bodies. A header
     * claiming success cannot omit the body expected by the DMA caller. */
    if (is_reply && len != NULL && hdr->msg_size - sizeof(*hdr) != *len) {
        error = EINVAL;
        goto malformed;
    }

    if (len != NULL && *len > 0 && hdr->msg_size > sizeof(*hdr)) {
        /* Internal callers may deliberately receive a fixed body prefix, then
         * consume the remaining payload themselves (e.g. sample DMA writes). */
        if (hdr->msg_size - sizeof(*hdr) < *len) {
            error = EINVAL;
            goto malformed;
        }
        ret = get_msg(data, MIN(hdr->msg_size - sizeof(*hdr), *len),
                      NULL, NULL, sock, 0, deadline);
        if (ret < 0) {
            error = errno;
            goto malformed;
        } else if (ret == 0) {
            error = ENOMSG;
            goto malformed;
        } else if (*len != (size_t)ret) {
            error = ECONNRESET;
            goto malformed;
        }
        *len = ret;
    }

    return 0;

malformed:
    if (nr_fds != NULL) {
        while (*nr_fds != 0) {
            close(fds[--*nr_fds]);
        }
    }
    (void)shutdown(sock, SHUT_RDWR);
    /* A poisoned reply stream is a lost connection, not a peer operation
     * error. Public DMA callers use this errno to quiesce/reset and detach
     * both primary and twin sockets. Complete header-only errors return above. */
    return ERROR_INT(is_reply ? ECONNRESET : error);
}

int
tran_sock_recv(int sock, struct vfio_user_header *hdr, bool is_reply,
         uint16_t *msg_id, void *data, size_t *len)
{
    return tran_sock_recv_fds(sock, hdr, is_reply, msg_id,
                              data, len, NULL, NULL,
                              monotonic_ms() + FRAME_TIMEOUT_MS);
}

/*
 * Like tran_sock_recv(), but will automatically allocate reply data.
 */
int
tran_sock_recv_alloc(int sock, struct vfio_user_header *hdr, bool is_reply,
                     uint16_t *msg_id, void **datap, size_t *lenp)
{
    void *data;
    size_t len;
    int ret;
    int64_t deadline = monotonic_ms() + FRAME_TIMEOUT_MS;

    ret = tran_sock_recv_fds(sock, hdr, is_reply, msg_id, NULL, NULL,
                             NULL, NULL, deadline);

    if (ret != 0) {
        return ret;
    }

    assert(hdr->msg_size >= sizeof(*hdr));
    assert(hdr->msg_size <= SERVER_MAX_MSG_SIZE);

    len = hdr->msg_size - sizeof(*hdr);

    if (len == 0) {
        *datap = NULL;
        *lenp = 0;
        return 0;
    }

    data = calloc(1, len);

    if (data == NULL) {
        (void)shutdown(sock, SHUT_RDWR);
        return ERROR_INT(ENOMEM);
    }

    ret = get_msg(data, len, NULL, NULL, sock, 0, deadline);
    if (ret < 0) {
        ret = errno;
        free(data);
        return ERROR_INT(ret);
    } else if (ret == 0) {
        free(data);
        return ERROR_INT(ENOMSG);
    } else if (len != (size_t)ret) {
        free(data);
        return ERROR_INT(ECONNRESET);
    }

    *datap = data;
    *lenp = len;
    return 0;
}

/*
 * FIXME: all these send/recv handlers need to be made robust against async
 * messages.
 */
int
tran_sock_msg_iovec(int sock, uint16_t msg_id, enum vfio_user_command cmd,
                    struct iovec *iovecs, size_t nr_iovecs,
                    int *send_fds, size_t send_fd_count,
                    struct vfio_user_header *hdr,
                    void *recv_data, size_t recv_len,
                    int *recv_fds, size_t *recv_fd_count)
{
    int ret = tran_sock_send_iovec(sock, msg_id, false, cmd, iovecs, nr_iovecs,
                                   send_fds, send_fd_count, 0);
    if (ret < 0) {
        return ret;
    }
    if (hdr == NULL) {
        hdr = alloca(sizeof(*hdr));
    }
    return tran_sock_recv_fds(sock, hdr, true, &msg_id, recv_data, &recv_len,
                              recv_fds, recv_fd_count,
                              monotonic_ms() + FRAME_TIMEOUT_MS);
}

int
tran_sock_msg_fds(int sock, uint16_t msg_id, enum vfio_user_command cmd,
                  void *send_data, size_t send_len,
                  struct vfio_user_header *hdr,
                  void *recv_data, size_t recv_len, int *recv_fds,
                  size_t *recv_fd_count)
{
    /* [0] is for the header. */
    struct iovec iovecs[2] = {
        [1] = {
            .iov_base = send_data,
            .iov_len = send_len
        }
    };
    return tran_sock_msg_iovec(sock, msg_id, cmd, iovecs, ARRAY_SIZE(iovecs),
                               NULL, 0, hdr, recv_data, recv_len, recv_fds,
                               recv_fd_count);
}

int
tran_sock_msg(int sock, uint16_t msg_id, enum vfio_user_command cmd,
              void *send_data, size_t send_len,
              struct vfio_user_header *hdr,
              void *recv_data, size_t recv_len)
{
    return tran_sock_msg_fds(sock, msg_id, cmd, send_data, send_len, hdr,
                             recv_data, recv_len, NULL, NULL);
}

static int
tran_sock_init(vfu_ctx_t *vfu_ctx)
{
    struct sockaddr_un addr = { .sun_family = AF_UNIX };
    tran_sock_t *ts = NULL;
    int ret;

    assert(vfu_ctx != NULL);

    ts = calloc(1, sizeof(tran_sock_t));

    if (ts == NULL) {
        return -1;
    }

    ts->listen_fd = -1;
    ts->conn_fd = -1;
    ts->client_cmd_socket_fd = -1;

    if ((ts->listen_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)) == -1) {
        ret = errno;
        goto out;
    }

    if (vfu_ctx->flags & LIBVFIO_USER_FLAG_ATTACH_NB) {
        ret = fcntl(ts->listen_fd, F_SETFL,
                    fcntl(ts->listen_fd, F_GETFL, 0) | O_NONBLOCK);
        if (ret < 0) {
            ret = errno;
            goto out;
        }
    }

    ret = snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", vfu_ctx->uuid);
    if (ret >= (int)sizeof(addr.sun_path)) {
        ret = ENAMETOOLONG;
        goto out;
    } else if (ret < 0) {
        ret = EINVAL;
        goto out;
    }

    /* start listening for business */
    ret = bind(ts->listen_fd, (struct sockaddr *)&addr, sizeof(addr));
    if (ret < 0) {
        ret = errno;
        goto out;
    }

    ret = listen(ts->listen_fd, 0);
    if (ret < 0) {
        ret = errno;
        (void) unlink(vfu_ctx->uuid);
    }

out:
    if (ret != 0) {
        if (ts != NULL) {
            close_safely(&ts->listen_fd);
        }
        free(ts);
        return ERROR_INT(ret);
    }

    vfu_ctx->tran_data = ts;
    return 0;
}

static int
tran_sock_get_poll_fd(vfu_ctx_t *vfu_ctx)
{
    tran_sock_t *ts = vfu_ctx->tran_data;

    if (ts->conn_fd != -1) {
        return ts->conn_fd;
    }

    return ts->listen_fd;
}

static int
tran_sock_attach(vfu_ctx_t *vfu_ctx)
{
    tran_sock_t *ts;
    int ret;

    assert(vfu_ctx != NULL);
    assert(vfu_ctx->tran_data != NULL);

    ts = vfu_ctx->tran_data;

    if (ts->conn_fd != -1) {
        vfu_log(vfu_ctx, LOG_ERR, "%s: already attached with fd=%d",
                __func__, ts->conn_fd);
        return ERROR_INT(EINVAL);
    }

    ts->conn_fd = accept4(ts->listen_fd, NULL, NULL, SOCK_CLOEXEC);
    if (ts->conn_fd == -1) {
        return -1;
    }
    ts->poisoned = false;

    ret = tran_negotiate(vfu_ctx, &ts->client_cmd_socket_fd);
    if (ret < 0) {
        int saved = errno;

        close_safely(&ts->conn_fd);
        close_safely(&ts->client_cmd_socket_fd);
        return ERROR_INT(saved);
    }

    return 0;
}

static int
tran_sock_get_request_header(vfu_ctx_t *vfu_ctx, struct vfio_user_header *hdr,
                             int *fds, size_t *nr_fds)
{
    tran_sock_t *ts;
    int sock_flags = 0;

    assert(vfu_ctx != NULL);
    assert(vfu_ctx->tran_data != NULL);

    ts = vfu_ctx->tran_data;

    if (ts->conn_fd == -1) {
        vfu_log(vfu_ctx, LOG_ERR, "%s: not connected", __func__);
        return ERROR_INT(ENOTCONN);
    }
    if (ts->poisoned) {
        return disconnect_socket(ts->conn_fd);
    }

    /* Idle polls return immediately; partial frames finish against one bound. */
    if (vfu_ctx->flags & LIBVFIO_USER_FLAG_ATTACH_NB) {
        sock_flags = MSG_DONTWAIT;
    } else {
        /* Blocking mode waits indefinitely between requests, as before. Start
         * the shared frame deadline only when input (or EOF) becomes ready. */
        struct pollfd pfd = { .fd = ts->conn_fd, .events = POLLIN };

        if (poll(&pfd, 1, -1) < 0) {
            return -1;
        }
    }
    ts->request_deadline = monotonic_ms() + FRAME_TIMEOUT_MS;
    int ret = get_msg(hdr, sizeof(*hdr), fds, nr_fds, ts->conn_fd,
                      sock_flags, ts->request_deadline);

    /* Preserve the error reply, but never parse unread malformed-frame bytes. */
    if (ret >= 0 && (hdr->msg_size < sizeof(*hdr) ||
                    hdr->msg_size > SERVER_MAX_MSG_SIZE ||
                    (hdr->flags & VFIO_USER_F_TYPE_MASK) !=
                        VFIO_USER_F_TYPE_COMMAND)) {
        ts->poisoned = true;
        /* Also wake idle polling when NO_REPLY suppresses the error reply. */
        (void)shutdown(ts->conn_fd, SHUT_RD);
    }
    return ret;
}

static int
tran_sock_recv_body(vfu_ctx_t *vfu_ctx, vfu_msg_t *msg)
{
    tran_sock_t *ts;
    int ret;

    assert(vfu_ctx != NULL);
    assert(vfu_ctx->tran_data != NULL);
    assert(msg != NULL);

    ts = vfu_ctx->tran_data;

    if (ts->conn_fd == -1) {
        vfu_log(vfu_ctx, LOG_ERR, "%s: not connected", __func__);
        return ERROR_INT(ENOTCONN);
    }

    assert(msg->in.iov.iov_len <= SERVER_MAX_MSG_SIZE);

    msg->in.iov.iov_base = malloc(msg->in.iov.iov_len);

    if (msg->in.iov.iov_base == NULL) {
        return disconnect_socket(ts->conn_fd);
    }

    ret = get_msg(msg->in.iov.iov_base, msg->in.iov.iov_len, NULL, NULL,
                  ts->conn_fd, 0, ts->request_deadline);

    if (ret < 0) {
        ret = errno;
        free(msg->in.iov.iov_base);
        msg->in.iov.iov_base = NULL;
        return ERROR_INT(ret);
    } else if (ret == 0) {
        free(msg->in.iov.iov_base);
        msg->in.iov.iov_base = NULL;
        return ERROR_INT(ENOMSG);
    } else if (ret != (int)msg->in.iov.iov_len)  {
        vfu_log(vfu_ctx, LOG_ERR, "msg%#hx: short read: expected=%zu, actual=%d",
                msg->hdr.msg_id, msg->in.iov.iov_len, ret);
        free(msg->in.iov.iov_base);
        msg->in.iov.iov_base = NULL;
        return ERROR_INT(EINVAL);
    }

    return 0;
}

static int
tran_sock_recv_msg(vfu_ctx_t *vfu_ctx, vfu_msg_t *msg)
{
    tran_sock_t *ts;

    assert(vfu_ctx != NULL);
    assert(vfu_ctx->tran_data != NULL);
    assert(msg != NULL);

    ts = vfu_ctx->tran_data;

    if (ts->conn_fd == -1) {
        vfu_log(vfu_ctx, LOG_ERR, "%s: not connected", __func__);
        return ERROR_INT(ENOTCONN);
    }

    return tran_sock_recv_alloc(ts->conn_fd, &msg->hdr, false, NULL,
                                &msg->in.iov.iov_base, &msg->in.iov.iov_len);
}

static int
tran_sock_reply(vfu_ctx_t *vfu_ctx, vfu_msg_t *msg, int err)
{
    struct iovec *iovecs;
    size_t nr_iovecs;
    tran_sock_t *ts;
    int ret;

    assert(vfu_ctx != NULL);
    assert(vfu_ctx->tran_data != NULL);
    assert(msg != NULL);

    ts = vfu_ctx->tran_data;

    /* First iovec entry is for msg header. */
    nr_iovecs = (msg->nr_out_iovecs != 0) ? (msg->nr_out_iovecs + 1) : 2;
    iovecs = calloc(nr_iovecs, sizeof(*iovecs));

    if (iovecs == NULL) {
        return -1;
    }

    if (msg->out_iovecs != NULL) {
        memcpy(iovecs + 1, msg->out_iovecs,
               msg->nr_out_iovecs * sizeof(*iovecs));
    } else {
        iovecs[1].iov_base = msg->out.iov.iov_base;
        iovecs[1].iov_len = msg->out.iov.iov_len;
    }

    ret = tran_sock_send_iovec(ts->conn_fd, msg->hdr.msg_id, true, msg->hdr.cmd,
                               iovecs, nr_iovecs,
                               msg->out.fds, msg->out.nr_fds, err);
    if (ts->poisoned) {
        (void)shutdown(ts->conn_fd, SHUT_RDWR);
    }

    free(iovecs);

    return ret;
}

static void maybe_print_cmd_collision_warning(vfu_ctx_t *vfu_ctx) {
    static bool warning_printed = false;
    static const char *warning_msg =
        "You are using libvfio-user in a configuration that issues "
        "client-to-server commands, but without the twin_socket feature "
        "enabled. This is known to break when client and server send a command "
        "at the same time. See "
        "https://github.com/nutanix/libvfio-user/issues/279 for details.";

    if (!warning_printed) {
        vfu_log(vfu_ctx, LOG_WARNING, "%s", warning_msg);
        warning_printed = true;
    }
}

static int
tran_sock_send_msg(vfu_ctx_t *vfu_ctx, uint16_t msg_id,
              enum vfio_user_command cmd,
              void *send_data, size_t send_len,
              struct vfio_user_header *hdr,
              void *recv_data, size_t recv_len)
{
    tran_sock_t *ts;
    int fd;

    assert(vfu_ctx != NULL);
    assert(vfu_ctx->tran_data != NULL);

    ts = vfu_ctx->tran_data;

    fd = ts->client_cmd_socket_fd;
    if (fd == -1) {
        maybe_print_cmd_collision_warning(vfu_ctx);
        fd = ts->conn_fd;
    }

    return tran_sock_msg(fd, msg_id, cmd, send_data, send_len, hdr, recv_data,
                         recv_len);
}

static void
tran_sock_detach(vfu_ctx_t *vfu_ctx)
{
    tran_sock_t *ts;

    assert(vfu_ctx != NULL);

    ts = vfu_ctx->tran_data;

    if (ts != NULL) {
        close_safely(&ts->conn_fd);
        close_safely(&ts->client_cmd_socket_fd);
    }
}

static void
tran_sock_fini(vfu_ctx_t *vfu_ctx)
{
    tran_sock_t *ts;

    assert(vfu_ctx != NULL);

    ts = vfu_ctx->tran_data;

    if (ts != NULL) {
        (void) unlink(vfu_ctx->uuid);
        close_safely(&ts->listen_fd);
    }

    free(vfu_ctx->tran_data);
    vfu_ctx->tran_data = NULL;
}

struct transport_ops tran_sock_ops = {
    .init = tran_sock_init,
    .get_poll_fd = tran_sock_get_poll_fd,
    .attach = tran_sock_attach,
    .get_request_header = tran_sock_get_request_header,
    .recv_body = tran_sock_recv_body,
    .reply = tran_sock_reply,
    .recv_msg = tran_sock_recv_msg,
    .send_msg = tran_sock_send_msg,
    .detach = tran_sock_detach,
    .fini = tran_sock_fini
};

/* ex: set tabstop=4 shiftwidth=4 softtabstop=4 expandtab: */
