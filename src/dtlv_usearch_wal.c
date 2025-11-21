#include "dtlv_usearch_wal.h"

#include "dtlv_bytes.h"
#include "dtlv_crc32c.h"

#ifdef _WIN32
#ifndef _CRT_RAND_S
#define _CRT_RAND_S
#endif
#endif

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <time.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <windows.h>
#else
#include <sys/types.h>
#include <unistd.h>
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

struct dtlv_usearch_wal_ctx {
  dtlv_uuid128 token;
  uint64_t snapshot_seq_base;
  uint64_t log_seq_hint;
  uint32_t next_ordinal;
  uint32_t frame_count;
  dtlv_ulog_state state;
  char domain_root[PATH_MAX];
  char pending_dir[PATH_MAX];
  char path_open[PATH_MAX];
  char path_sealed[PATH_MAX];
  char path_ready[PATH_MAX];
#ifndef _WIN32
  int fd;
#else
  HANDLE handle;
#endif
};

static void dtlv_format_token(const dtlv_uuid128 *token, char *out, size_t len) {
  snprintf(out, len, "%016llx%016llx",
           (unsigned long long)token->hi,
           (unsigned long long)token->lo);
}

static int dtlv_trace_enabled(void) {
  const char *flag = getenv("DTLV_TRACE_TESTS");
  return flag && *flag;
}

static int dtlv_fault_flag_enabled(const char *flag) {
  if (!flag) return 0;
  const char *value = getenv(flag);
  return value && *value && strcmp(value, "0") != 0;
}

static void dtlv_tracef(const char *fmt, ...) {
  if (!dtlv_trace_enabled()) return;
  va_list args;
  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
}

static int dtlv_random_bytes(void *dst, size_t len) {
  uint8_t *bytes = (uint8_t *)dst;
  size_t produced = 0;
#ifdef _WIN32
  while (produced < len) {
    unsigned int value = 0;
    if (rand_s(&value) != 0) return EIO;
    size_t chunk = len - produced;
    if (chunk > sizeof(value)) chunk = sizeof(value);
    memcpy(bytes + produced, &value, chunk);
    produced += chunk;
  }
  return 0;
#else
  int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
  if (fd >= 0) {
    while (produced < len) {
      ssize_t rc = read(fd, bytes + produced, len - produced);
      if (rc < 0) {
        if (errno == EINTR) continue;
        close(fd);
        return errno;
      }
      produced += (size_t)rc;
    }
    close(fd);
    return 0;
  }
  srand((unsigned int)time(NULL));
  while (produced < len) {
    int value = rand();
    size_t chunk = len - produced;
    if (chunk > sizeof(value)) chunk = sizeof(value);
    memcpy(bytes + produced, &value, chunk);
    produced += chunk;
  }
  return 0;
#endif
}

#ifdef _WIN32
static int dtlv_utf8_to_wide(const char *src, wchar_t **out) {
  if (!src || !out) return EINVAL;
  int needed = MultiByteToWideChar(CP_UTF8, 0, src, -1, NULL, 0);
  if (needed <= 0) return EINVAL;
  wchar_t *buf = (wchar_t *)malloc((size_t)needed * sizeof(wchar_t));
  if (!buf) return ENOMEM;
  if (!MultiByteToWideChar(CP_UTF8, 0, src, -1, buf, needed)) {
    free(buf);
    return EINVAL;
  }
  *out = buf;
  return 0;
}

static int dtlv_win32_last_error(void) {
  DWORD err = GetLastError();
  return err ? (int)err : EIO;
}

static int dtlv_win32_ensure_directory(const char *path) {
  wchar_t *utf16 = NULL;
  int rc = dtlv_utf8_to_wide(path, &utf16);
  if (rc != 0) return rc;
  if (_wmkdir(utf16) == 0) {
    free(utf16);
    return 0;
  }
  int err = errno;
  free(utf16);
  if (err == EEXIST) return 0;
  return err ? err : EIO;
}

static int dtlv_win32_open_file(const char *path, HANDLE *handle) {
  wchar_t *utf16 = NULL;
  int rc = dtlv_utf8_to_wide(path, &utf16);
  if (rc != 0) return rc;
  HANDLE h = CreateFileW(
      utf16, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ,
      NULL, CREATE_NEW,
      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN | FILE_FLAG_WRITE_THROUGH,
      NULL);
  free(utf16);
  if (h == INVALID_HANDLE_VALUE) return dtlv_win32_last_error();
  *handle = h;
  return 0;
}

static int dtlv_win32_rename_file(const char *src, const char *dst) {
  wchar_t *wsrc = NULL;
  wchar_t *wdst = NULL;
  int rc = dtlv_utf8_to_wide(src, &wsrc);
  if (rc != 0) return rc;
  rc = dtlv_utf8_to_wide(dst, &wdst);
  if (rc != 0) {
    free(wsrc);
    return rc;
  }
  BOOL ok = MoveFileExW(wsrc, wdst, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH);
  free(wsrc);
  free(wdst);
  if (!ok) return dtlv_win32_last_error();
  return 0;
}

static int dtlv_win32_delete_file(const char *path) {
  wchar_t *utf16 = NULL;
  int rc = dtlv_utf8_to_wide(path, &utf16);
  if (rc != 0) return rc;
  BOOL ok = DeleteFileW(utf16);
  int err = ok ? 0 : dtlv_win32_last_error();
  free(utf16);
  if (!ok && err == ERROR_FILE_NOT_FOUND) return 0;
  return err;
}
#endif

static int dtlv_make_directories(const char *path) {
  if (!path || !*path) return EINVAL;
  char buffer[PATH_MAX];
  size_t len = strlen(path);
  if (len >= sizeof(buffer)) return ENAMETOOLONG;
  memcpy(buffer, path, len + 1);
  for (size_t i = 1; i < len; ++i) {
    if (buffer[i] == '/' || buffer[i] == '\\') {
      if (i == 2 && buffer[1] == ':') continue;
      char saved = buffer[i];
      buffer[i] = '\0';
#ifdef _WIN32
      int rc = dtlv_win32_ensure_directory(buffer);
      if (rc != 0 && rc != EEXIST) return rc;
#else
      if (mkdir(buffer, 0775) != 0 && errno != EEXIST) return errno;
#endif
      buffer[i] = saved;
    }
  }
#ifdef _WIN32
  int rc = dtlv_win32_ensure_directory(buffer);
  return (rc == EEXIST) ? 0 : rc;
#else
  if (mkdir(buffer, 0775) == 0) return 0;
  if (errno == EEXIST) return 0;
  return errno;
#endif
}

#ifndef _WIN32
static int dtlv_write_full_fd(int fd, const void *data, size_t len) {
  const uint8_t *ptr = (const uint8_t *)data;
  size_t written = 0;
  while (written < len) {
    ssize_t rc = write(fd, ptr + written, len - written);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return errno;
    }
    if (dtlv_trace_enabled()) {
      dtlv_tracef("[wal_write_fd] chunk=%zu wrote=%zd\n", len - written, rc);
    }
    written += (size_t)rc;
  }
  return 0;
}

static int dtlv_write_at_fd(int fd, const void *data, size_t len, off_t offset) {
  const uint8_t *ptr = (const uint8_t *)data;
  size_t written = 0;
  while (written < len) {
    ssize_t rc = pwrite(fd, ptr + written, len - written, offset + (off_t)written);
    if (rc < 0) {
      if (errno == EINTR) continue;
      return errno;
    }
    written += (size_t)rc;
  }
  return 0;
}

static int dtlv_fdatasync_fd(int fd) {
  if (fsync(fd) != 0) return errno;
  return 0;
}
#else
static int dtlv_write_full_handle(HANDLE handle, const void *data, size_t len) {
  const uint8_t *ptr = (const uint8_t *)data;
  size_t written = 0;
  while (written < len) {
    DWORD chunk = (DWORD)((len - written) > (size_t)DWORD_MAX ? (size_t)DWORD_MAX : (len - written));
    DWORD out = 0;
    if (!WriteFile(handle, ptr + written, chunk, &out, NULL)) return dtlv_win32_last_error();
    if (out == 0) return EIO;
    written += (size_t)out;
  }
  return 0;
}

static int dtlv_write_at_handle(HANDLE handle, const void *data, size_t len, int64_t offset) {
  LARGE_INTEGER pos;
  pos.QuadPart = offset;
  if (!SetFilePointerEx(handle, pos, NULL, FILE_BEGIN)) return dtlv_win32_last_error();
  return dtlv_write_full_handle(handle, data, len);
}

static int dtlv_fdatasync_handle(HANDLE handle) {
  if (!FlushFileBuffers(handle)) return dtlv_win32_last_error();
  return 0;
}
#endif

static int dtlv_encode_header(const dtlv_usearch_wal_ctx *ctx, dtlv_ulog_state state, dtlv_ulog_header_v1 *out) {
  memset(out, 0, sizeof(*out));
  memcpy(out->magic, DTLV_ULOG_MAGIC, sizeof(out->magic));
  out->version = DTLV_ULOG_VERSION;
  out->state = (uint8_t)state;
  out->header_len = dtlv_to_be16((uint16_t)sizeof(*out));
  out->snapshot_seq_base = dtlv_to_be64(ctx->snapshot_seq_base);
  out->log_seq_hint = dtlv_to_be64(ctx->log_seq_hint);
  out->txn_token_hi = dtlv_to_be64(ctx->token.hi);
  out->txn_token_lo = dtlv_to_be64(ctx->token.lo);
  out->frame_count = dtlv_to_be32(ctx->frame_count);
  size_t checksum_offset = offsetof(dtlv_ulog_header_v1, header_len);
  size_t checksum_len = sizeof(*out) - checksum_offset - sizeof(uint32_t);
  uint32_t crc = dtlv_crc32c((const uint8_t *)out + checksum_offset, checksum_len);
  out->checksum = dtlv_to_be32(crc);
  return 0;
}

static int dtlv_write_header(dtlv_usearch_wal_ctx *ctx, dtlv_ulog_state state) {
  dtlv_ulog_header_v1 header;
  dtlv_encode_header(ctx, state, &header);
#ifndef _WIN32
  return dtlv_write_at_fd(ctx->fd, &header, sizeof(header), 0);
#else
  return dtlv_write_at_handle(ctx->handle, &header, sizeof(header), 0);
#endif
}

static int dtlv_write_frame(dtlv_usearch_wal_ctx *ctx, const void *payload, size_t payload_len) {
  if (!payload || payload_len == 0 || payload_len > UINT32_MAX) return EINVAL;
  dtlv_ulog_frame_prefix_v1 prefix;
  uint32_t crc = dtlv_crc32c(payload, payload_len);
  if (dtlv_fault_flag_enabled("DTLV_FAULT_WAL_CRC")) crc ^= 0xa5a5a5a5u;
  prefix.ordinal = dtlv_to_be32(ctx->next_ordinal);
  prefix.delta_bytes = dtlv_to_be32((uint32_t)payload_len);
  prefix.checksum = dtlv_to_be32(crc);
  if (dtlv_trace_enabled()) {
    dtlv_tracef("[wal_write] token=%016llx%016llx ordinal=%u bytes=%zu crc=0x%08x\n",
                (unsigned long long)ctx->token.hi,
                (unsigned long long)ctx->token.lo,
                ctx->next_ordinal,
                payload_len,
                crc);
  }
#ifndef _WIN32
  int rc = dtlv_write_full_fd(ctx->fd, &prefix, sizeof(prefix));
  if (rc != 0) return rc;
  rc = dtlv_write_full_fd(ctx->fd, payload, payload_len);
#else
  int rc = dtlv_write_full_handle(ctx->handle, &prefix, sizeof(prefix));
  if (rc != 0) return rc;
  rc = dtlv_write_full_handle(ctx->handle, payload, payload_len);
#endif
  if (rc != 0) return rc;
  ctx->next_ordinal += 1;
  ctx->frame_count += 1;
#ifndef _WIN32
  if (dtlv_trace_enabled()) {
    struct stat st;
    if (fstat(ctx->fd, &st) == 0) {
      dtlv_tracef("[wal_write] size_now=%lld\n", (long long)st.st_size);
    }
  }
#endif
  return 0;
}

int dtlv_usearch_wal_open(const char *domain_root,
                          uint64_t snapshot_seq_base,
                          uint64_t log_seq_hint,
                          dtlv_usearch_wal_ctx **ctx_out) {
  if (!domain_root || !ctx_out) return EINVAL;
  size_t len = strlen(domain_root);
  if (len == 0 || len >= PATH_MAX) return ENAMETOOLONG;
  dtlv_usearch_wal_ctx *ctx = (dtlv_usearch_wal_ctx *)calloc(1, sizeof(*ctx));
  if (!ctx) return ENOMEM;
  ctx->next_ordinal = 1;
  ctx->frame_count = 0;
  ctx->state = DTLV_ULOG_STATE_WRITING;
#if 1
  if (dtlv_trace_enabled()) {
    dtlv_tracef("[wal_open] header_size=%zu\n", sizeof(dtlv_ulog_header_v1));
  }
#endif
#ifndef _WIN32
  ctx->fd = -1;
#else
  ctx->handle = INVALID_HANDLE_VALUE;
#endif
  memcpy(ctx->domain_root, domain_root, len + 1);
  snprintf(ctx->pending_dir, sizeof(ctx->pending_dir), "%s", ctx->domain_root);
  int rc = dtlv_make_directories(ctx->pending_dir);
  if (rc != 0) {
    free(ctx);
    return rc;
  }
  rc = dtlv_random_bytes(&ctx->token, sizeof(ctx->token));
  if (rc != 0) {
    free(ctx);
    return rc;
  }
  char token_hex[DTLV_ULOG_TOKEN_HEX + 1];
  dtlv_format_token(&ctx->token, token_hex, sizeof(token_hex));
  snprintf(ctx->path_open, sizeof(ctx->path_open), "%s/%s.ulog.open", ctx->pending_dir, token_hex);
  snprintf(ctx->path_sealed, sizeof(ctx->path_sealed), "%s/%s.ulog", ctx->pending_dir, token_hex);
  snprintf(ctx->path_ready, sizeof(ctx->path_ready), "%s/%s.ulog.sealed", ctx->pending_dir, token_hex);
  ctx->snapshot_seq_base = snapshot_seq_base;
  ctx->log_seq_hint = log_seq_hint;
#ifndef _WIN32
  int flags = O_RDWR | O_CREAT | O_EXCL;
#ifdef O_CLOEXEC
  flags |= O_CLOEXEC;
#endif
#ifdef O_DSYNC
  flags |= O_DSYNC;
#endif
  ctx->fd = open(ctx->path_open, flags, 0640);
  if (ctx->fd < 0) {
    rc = errno;
    free(ctx);
    return rc;
  }
#else
  rc = dtlv_win32_open_file(ctx->path_open, &ctx->handle);
  if (rc != 0) {
    free(ctx);
    return rc;
  }
#endif
  rc = dtlv_write_header(ctx, ctx->state);
  if (rc != 0) {
    dtlv_usearch_wal_close(ctx, 1);
    return rc;
  }
#ifndef _WIN32
  if (lseek(ctx->fd, (off_t)sizeof(dtlv_ulog_header_v1), SEEK_SET) < 0) {
    rc = errno ? errno : EIO;
    dtlv_usearch_wal_close(ctx, 1);
    return rc;
  }
#else
  LARGE_INTEGER cursor;
  cursor.QuadPart = (LONGLONG)sizeof(dtlv_ulog_header_v1);
  if (!SetFilePointerEx(ctx->handle, cursor, NULL, FILE_BEGIN)) {
    rc = dtlv_win32_last_error();
    dtlv_usearch_wal_close(ctx, 1);
    return rc;
  }
#endif
  *ctx_out = ctx;
  return 0;
}

int dtlv_usearch_wal_append(dtlv_usearch_wal_ctx *ctx,
                            const void *delta_payload,
                            size_t delta_bytes) {
  if (!ctx || !delta_payload || delta_bytes == 0) return EINVAL;
  if (ctx->state != DTLV_ULOG_STATE_WRITING) return EBUSY;
  return dtlv_write_frame(ctx, delta_payload, delta_bytes);
}

int dtlv_usearch_wal_seal(dtlv_usearch_wal_ctx *ctx) {
  if (!ctx) return EINVAL;
  if (ctx->state != DTLV_ULOG_STATE_WRITING) return EBUSY;
#ifndef _WIN32
  int rc = dtlv_fdatasync_fd(ctx->fd);
#else
  int rc = dtlv_fdatasync_handle(ctx->handle);
#endif
  if (rc != 0) return rc;
  rc = dtlv_write_header(ctx, DTLV_ULOG_STATE_SEALED);
  if (rc != 0) return rc;
#ifndef _WIN32
  rc = dtlv_fdatasync_fd(ctx->fd);
#else
  rc = dtlv_fdatasync_handle(ctx->handle);
#endif
  if (rc != 0) return rc;
#ifndef _WIN32
  if (dtlv_trace_enabled()) {
    struct stat st;
    if (fstat(ctx->fd, &st) == 0) {
      dtlv_tracef("[wal_seal] size=%lld\n", (long long)st.st_size);
    }
  }
#endif
#ifdef _WIN32
  rc = dtlv_win32_rename_file(ctx->path_open, ctx->path_sealed);
#else
  if (rename(ctx->path_open, ctx->path_sealed) != 0) rc = errno;
#endif
  if (rc != 0) return rc;
  ctx->state = DTLV_ULOG_STATE_SEALED;
  return 0;
}

int dtlv_usearch_wal_mark_ready(dtlv_usearch_wal_ctx *ctx) {
  if (!ctx) return EINVAL;
  if (ctx->state != DTLV_ULOG_STATE_SEALED && ctx->state != DTLV_ULOG_STATE_READY_FOR_PUBLISH) {
    return EBUSY;
  }
  int rc = dtlv_write_header(ctx, DTLV_ULOG_STATE_READY_FOR_PUBLISH);
  if (rc != 0) return rc;
#ifndef _WIN32
  rc = dtlv_fdatasync_fd(ctx->fd);
#else
  rc = dtlv_fdatasync_handle(ctx->handle);
#endif
  if (rc != 0) return rc;
  if (ctx->state == DTLV_ULOG_STATE_SEALED) {
#ifdef _WIN32
    rc = dtlv_win32_rename_file(ctx->path_sealed, ctx->path_ready);
#else
    if (rename(ctx->path_sealed, ctx->path_ready) != 0) rc = errno;
#endif
    if (rc != 0) return rc;
  }
  ctx->state = DTLV_ULOG_STATE_READY_FOR_PUBLISH;
  return 0;
}

void dtlv_usearch_wal_close(dtlv_usearch_wal_ctx *ctx, int best_effort_delete) {
  if (!ctx) return;
#ifndef _WIN32
  if (ctx->fd >= 0) close(ctx->fd);
#else
  if (ctx->handle && ctx->handle != INVALID_HANDLE_VALUE) CloseHandle(ctx->handle);
#endif
  if (best_effort_delete) {
#ifdef _WIN32
    if (ctx->state == DTLV_ULOG_STATE_WRITING) {
      dtlv_win32_delete_file(ctx->path_open);
    } else if (ctx->state == DTLV_ULOG_STATE_SEALED) {
      dtlv_win32_delete_file(ctx->path_sealed);
    } else if (ctx->state == DTLV_ULOG_STATE_READY_FOR_PUBLISH) {
      dtlv_win32_delete_file(ctx->path_ready);
    }
#else
    const char *path = NULL;
    if (ctx->state == DTLV_ULOG_STATE_WRITING) path = ctx->path_open;
    else if (ctx->state == DTLV_ULOG_STATE_SEALED) path = ctx->path_sealed;
    else if (ctx->state == DTLV_ULOG_STATE_READY_FOR_PUBLISH) path = ctx->path_ready;
    if (path) unlink(path);
#endif
  }
  free(ctx);
}

const dtlv_uuid128 *dtlv_usearch_wal_token(const dtlv_usearch_wal_ctx *ctx) {
  return ctx ? &ctx->token : NULL;
}

uint32_t dtlv_usearch_wal_frame_count(const dtlv_usearch_wal_ctx *ctx) {
  return ctx ? ctx->frame_count : 0;
}

const char *dtlv_usearch_wal_open_path(const dtlv_usearch_wal_ctx *ctx) {
  return ctx ? ctx->path_open : NULL;
}

const char *dtlv_usearch_wal_sealed_path(const dtlv_usearch_wal_ctx *ctx) {
  return ctx ? ctx->path_sealed : NULL;
}

const char *dtlv_usearch_wal_ready_path(const dtlv_usearch_wal_ctx *ctx) {
  return ctx ? ctx->path_ready : NULL;
}
