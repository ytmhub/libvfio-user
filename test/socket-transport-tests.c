/* Socket framing regressions. Each case runs independently, with a watchdog. */
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "tran_sock.h"

static double
seconds(void)
{
    struct timespec t;
    assert(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static void
pair(int fds[2])
{
    assert(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds) == 0);
}

static size_t
fd_count(void)
{
    DIR *dir = opendir("/proc/self/fd");
    struct dirent *entry;
    size_t count = 0;

    assert(dir != NULL);
    while ((entry = readdir(dir)) != NULL) {
        count++;
    }
    closedir(dir);
    return count;
}

static void
receive_expired(size_t prefix, bool trickle)
{
    int fds[2];
    struct vfio_user_header hdr = { .msg_size = sizeof(hdr) + 4,
                                     .cmd = VFIO_USER_VERSION };
    char frame[sizeof(hdr) + 4] = { 0 };
    void *body = NULL;
    size_t len = 0;
    double start;
    pid_t child = -1;

    memcpy(frame, &hdr, sizeof(hdr));
    pair(fds);
    if (prefix != 0) {
        assert(write(fds[1], frame, prefix) == (ssize_t)prefix);
    }
    if (trickle) {
        child = fork();
        assert(child >= 0);
        if (child == 0) {
            close(fds[0]);
            for (size_t i = prefix; i < sizeof(frame); i++) {
                usleep(100000);
                if (send(fds[1], frame + i, 1, MSG_NOSIGNAL) != 1) {
                    break;
                }
            }
            close(fds[1]);
            _exit(0);
        }
    }
    start = seconds();
    assert(tran_sock_recv_alloc(fds[0], &hdr, false, NULL, &body, &len) == -1);
    assert(errno == ECONNRESET);
    assert(seconds() - start < 0.8);
    assert(body == NULL);
    /* The remainder is not reusable as another frame. */
    assert(recv(fds[0], frame, sizeof(frame), MSG_DONTWAIT) <= 0);
    close(fds[0]);
    close(fds[1]);
    if (child != -1) {
        assert(waitpid(child, NULL, 0) == child);
    }
}

static void silent(void) { receive_expired(0, false); }
static void partial_header(void) { receive_expired(1, false); }
static void missing_body(void) { receive_expired(16, false); }
static void partial_body(void) { receive_expired(17, false); }
static void trickle_header(void) { receive_expired(1, true); }
static void trickle_body(void) { receive_expired(16, true); }

static void
fragmented(void)
{
    int fds[2], status;
    struct vfio_user_header hdr = { .msg_size = sizeof(hdr) + 4,
                                     .cmd = VFIO_USER_VERSION };
    char frame[sizeof(hdr) + 4] = { 0 };
    void *body = NULL;
    size_t len = 0;
    pid_t child;

    memcpy(frame, &hdr, sizeof(hdr));
    memcpy(frame + sizeof(hdr), "abcd", 4);
    pair(fds);
    child = fork();
    assert(child >= 0);
    if (child == 0) {
        close(fds[0]);
        for (size_t i = 0; i < sizeof(frame); i++) {
            assert(write(fds[1], frame + i, 1) == 1);
            usleep(2000);
        }
        close(fds[1]);
        _exit(0);
    }
    close(fds[1]);
    assert(tran_sock_recv_alloc(fds[0], &hdr, false, NULL, &body, &len) == 0);
    assert(len == 4 && memcmp(body, "abcd", 4) == 0);
    free(body);
    close(fds[0]);
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void
eof_stages(void)
{
    for (size_t prefix = 0; prefix < 20; prefix++) {
        int fds[2];
        struct vfio_user_header hdr = { .msg_size = sizeof(hdr) + 4 };
        char frame[20] = { 0 };
        void *body = NULL;
        size_t len = 0;

        memcpy(frame, &hdr, sizeof(hdr));
        pair(fds);
        assert(write(fds[1], frame, prefix) == (ssize_t)prefix);
        close(fds[1]);
        assert(tran_sock_recv_alloc(fds[0], &hdr, false, NULL, &body, &len) < 0);
        assert(body == NULL);
        close(fds[0]);
    }
}

static void
dma_write_prefix(void)
{
    int fds[2];
    struct vfio_user_header hdr;
    struct vfio_user_dma_region_access access = { .addr = 0x1000, .count = 4 };
    struct vfio_user_dma_region_access received;
    char body[sizeof(access) + 4], payload[4];
    size_t len = sizeof(received);
    uint16_t msg_id = 0;
    void *next = NULL;

    memcpy(body, &access, sizeof(access));
    memcpy(body + sizeof(access), "abcd", 4);
    pair(fds);
    assert(tran_sock_send(fds[1], 7, false, VFIO_USER_DMA_WRITE,
                          body, sizeof(body)) == 0);
    /* The shipped sample client receives the fixed prefix and data separately. */
    assert(tran_sock_recv(fds[0], &hdr, false, &msg_id, &received, &len) == 0);
    assert(msg_id == 7 && len == sizeof(access));
    assert(hdr.msg_size == sizeof(hdr) + sizeof(body));
    assert(received.addr == access.addr && received.count == access.count);
    assert(recv(fds[0], payload, sizeof(payload), MSG_WAITALL) == sizeof(payload));
    assert(memcmp(payload, "abcd", sizeof(payload)) == 0);
    /* Consuming the tail leaves the stream exactly at the next header. */
    assert(tran_sock_send(fds[1], 8, false, VFIO_USER_DMA_WRITE,
                          body, sizeof(body)) == 0);
    assert(tran_sock_recv_alloc(fds[0], &hdr, false, &msg_id, &next, &len) == 0);
    assert(msg_id == 8 && len == sizeof(body));
    assert(memcmp(next, body, len) == 0);
    free(next);
    close(fds[0]);
    close(fds[1]);
}

static void
short_dma_prefix(void)
{
    int fds[2];
    struct vfio_user_header hdr;
    struct vfio_user_dma_region_access access;
    size_t len = sizeof(access);
    char body[] = "abcd";

    pair(fds);
    assert(tran_sock_send(fds[1], 7, false, VFIO_USER_DMA_WRITE, body, 4) == 0);
    assert(tran_sock_recv(fds[0], &hdr, false, NULL, &access, &len) == -1);
    assert(errno == EINVAL);
    assert(send(fds[0], "x", 1, MSG_NOSIGNAL) == -1);
    close(fds[0]);
    close(fds[1]);
}

static void
blocked_send(void)
{
    int fds[2], small = 4096;
    size_t size = 4 * 1024 * 1024;
    char *body = calloc(1, size);
    double start;

    assert(body != NULL);
    pair(fds);
    assert(setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)) == 0);
    start = seconds();
    assert(tran_sock_send(fds[0], 1, true, VFIO_USER_REGION_READ, body, size) < 0);
    assert(errno == ECONNRESET);
    assert(seconds() - start < 0.8);
    assert(send(fds[0], body, 1, MSG_NOSIGNAL) < 0);
    close(fds[0]);
    close(fds[1]);
    free(body);
}

static void
oversized_reply(void)
{
    int fds[2];
    struct vfio_user_header hdr;
    char body[20] = { 0 }, prefix[16];
    size_t len = sizeof(prefix);

    pair(fds);
    assert(tran_sock_send(fds[1], 7, true, VFIO_USER_DMA_READ,
                          body, sizeof(body)) == 0);
    /* A server expecting a complete reply must not leave an unread tail. */
    assert(tran_sock_recv(fds[0], &hdr, true, NULL, prefix, &len) == -1);
    assert(errno == ECONNRESET);
    assert(send(fds[0], "x", 1, MSG_NOSIGNAL) == -1);
    close(fds[0]);
    close(fds[1]);
}

static void
partial_send(void)
{
    int fds[2], small = 4096, status;
    size_t size = 1024 * 1024;
    char *body = malloc(size);
    pid_t child;

    assert(body != NULL);
    memset(body, 'x', size);
    pair(fds);
    assert(setsockopt(fds[0], SOL_SOCKET, SO_SNDBUF, &small, sizeof(small)) == 0);
    child = fork();
    assert(child >= 0);
    if (child == 0) {
        struct vfio_user_header hdr;
        char *received = malloc(size + sizeof(hdr));
        size_t len = 0, fd_count = 0;

        assert(received != NULL);
        close(fds[0]);
        usleep(10000); /* Force the first sendmsg to be a partial write. */
        while (len < size + sizeof(hdr)) {
            union {
                struct cmsghdr align;
                char data[CMSG_SPACE(sizeof(int) * 4)];
            } control;
            struct iovec iov = { .iov_base = received + len,
                                 .iov_len = MIN(4096, size + sizeof(hdr) - len) };
            struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1,
                .msg_control = &control, .msg_controllen = sizeof(control) };
            ssize_t ret = recvmsg(fds[1], &msg, MSG_CMSG_CLOEXEC);

            assert(ret > 0 && !(msg.msg_flags & MSG_CTRUNC));
            for (struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL;
                 cmsg = CMSG_NXTHDR(&msg, cmsg)) {
                assert(cmsg->cmsg_type == SCM_RIGHTS);
                assert(cmsg->cmsg_len == CMSG_LEN(sizeof(int)));
                int fd;

                memcpy(&fd, CMSG_DATA(cmsg), sizeof(fd));
                assert(fcntl(fd, F_GETFD) & FD_CLOEXEC);
                close(fd);
                fd_count++;
            }
            len += ret;
        }
        memcpy(&hdr, received, sizeof(hdr));
        assert(hdr.msg_size == size + sizeof(hdr) && fd_count == 1);
        assert(memcmp(received + sizeof(hdr), body, size) == 0);
        free(received);
        free(body);
        close(fds[1]);
        _exit(0);
    }
    close(fds[1]);
    struct iovec iovecs[2] = { [1] = { .iov_base = body, .iov_len = size } };
    assert(tran_sock_send_iovec(fds[0], 1, true, VFIO_USER_REGION_READ,
                                iovecs, 2, &fds[0], 1, 0) == 0);
    close(fds[0]);
    free(body);
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void
rights_reply(int fd, int passed, size_t bytes)
{
    struct vfio_user_header hdr = { .msg_id = 1, .cmd = VFIO_USER_REGION_READ,
        .msg_size = sizeof(hdr), .flags = VFIO_USER_F_TYPE_REPLY };
    union {
        struct cmsghdr align;
        char data[CMSG_SPACE(sizeof(int))];
    } control = { 0 };
    struct iovec iov = { .iov_base = &hdr, .iov_len = bytes };
    struct msghdr msg = { .msg_iov = &iov, .msg_iovlen = 1,
        .msg_control = &control, .msg_controllen = sizeof(control) };
    struct cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);

    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(cmsg), &passed, sizeof(passed));
    assert(sendmsg(fd, &msg, MSG_NOSIGNAL) == (ssize_t)bytes);
}

static void
rights_hygiene(void)
{
    size_t baseline = fd_count();

    for (int cycle = 0; cycle < 40; cycle++) {
        int fds[2], received = -1;
        size_t count = 1;
        struct vfio_user_header hdr;
        bool partial = cycle % 2 == 0;

        pair(fds);
        rights_reply(fds[1], fds[1], partial ? 1 : 16);
        if (partial) {
            assert(shutdown(fds[1], SHUT_WR) == 0);
        }
        int ret = tran_sock_msg_fds(fds[0], 1, VFIO_USER_REGION_READ,
                                    NULL, 0, &hdr, NULL, 0, &received, &count);
        if (partial) {
            assert(ret < 0 && count == 0);
        } else {
            assert(ret == 0 && count == 1);
            assert(fcntl(received, F_GETFD) & FD_CLOEXEC);
            close(received);
        }
        close(fds[0]);
        close(fds[1]);
        assert(fd_count() == baseline);
    }
}

int
main(void)
{
    const struct {
        const char *name;
        void (*run)(void);
    } cases[] = {
        { "silent", silent },
        { "partial header", partial_header },
        { "missing body", missing_body },
        { "partial body", partial_body },
        { "trickle header", trickle_header },
        { "trickle body", trickle_body },
        { "fragmented valid frame", fragmented },
        { "EOF at every byte", eof_stages },
        { "DMA write prefix and separate payload", dma_write_prefix },
        { "undersized DMA prefix is rejected", short_dma_prefix },
        { "oversized reply is rejected", oversized_reply },
        { "blocked send", blocked_send },
        { "partial send with exactly one SCM_RIGHTS transfer", partial_send },
        { "SCM_RIGHTS CLOEXEC and failure cleanup", rights_hygiene },
    };
    int failed = 0;

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        int status;
        pid_t child = fork();

        assert(child >= 0);
        if (child == 0) {
            alarm(3);
            cases[i].run();
            exit(0); /* Let LeakSanitizer inspect the owned case process. */
        }
        assert(waitpid(child, &status, 0) == child);
        bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
        printf("%s: %s\n", cases[i].name, ok ? "PASS" : "FAIL");
        fflush(stdout);
        failed += !ok;
    }
    return failed != 0;
}
