#include "dtlv_usearch.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#ifdef _WIN32
#include <direct.h>
#include <windows.h>
#include <sys/stat.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <unistd.h>
#endif

#include <time.h>

#include "dtlv_bytes.h"
#include "dtlv_crc32c.h"
#include "usearch/c/usearch.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DTLV_USEARCH_SCHEMA_VERSION 1u
#define DTLV_USEARCH_DEFAULT_CHUNK_BYTES (1u << 20)
#define DTLV_USEARCH_SNAPSHOT_CHUNK_VERSION 1u
#define DTLV_USEARCH_CHECKPOINT_VERSION 1u
#define DTLV_META_KEY_SCHEMA_VERSION "schema_version"
#define DTLV_META_KEY_INIT "init"
#define DTLV_META_KEY_CHUNK_BYTES "chunk_bytes"
#define DTLV_META_KEY_CHECKPOINT_PENDING "checkpoint_pending"
#define DTLV_META_KEY_LOG_TAIL_SEQ "log_tail_seq"
#define DTLV_META_KEY_LOG_SEQ "log_seq"
#define DTLV_META_KEY_SNAPSHOT_SEQ "snapshot_seq"
#define DTLV_META_KEY_PUBLISHED_LOG_TAIL "published_log_tail"
#define DTLV_META_KEY_SEALED_LOG_SEQ "sealed_log_seq"
#define DTLV_META_KEY_SNAPSHOT_RETENTION_COUNT "snapshot_retention_count"
#define DTLV_META_KEY_SNAPSHOT_RETAINED_FLOOR "snapshot_retained_floor"
#define DTLV_META_KEY_CHECKPOINT_CHUNK_BATCH "checkpoint_chunk_batch"
#define DTLV_INIT_RECORD_VERSION 1u
#define DTLV_INIT_RECORD_SIZE 44u
#define DTLV_CHECKPOINT_RECORD_SIZE 32u
#define DTLV_SNAPSHOT_CHUNK_HEADER_SIZE 12u
#define DTLV_USEARCH_DEFAULT_SNAPSHOT_RETENTION 2u
#define DTLV_READER_PIN_VERSION 1u
#define DTLV_READER_PIN_DEFAULT_TTL_MS 60000u
#define DTLV_READER_PIN_DEFAULT_HEARTBEAT_MS 5000u
#define DTLV_READER_PIN_SLOT_COUNT 64u
#define DTLV_READER_PIN_RECORD_SIZE 48u
#define DTLV_USEARCH_MAGIC "usearch"
#define DTLV_USEARCH_MAGIC_LEN 7u
#define DTLV_USEARCH_HEADER_BYTES 64u
#define DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH 8u

#ifdef _WIN32
static void dtlv_fix_windows_path(char *path);
#endif

typedef enum {
  DTLV_CHECKPOINT_STAGE_NONE = 0,
  DTLV_CHECKPOINT_STAGE_INIT = 1,
  DTLV_CHECKPOINT_STAGE_WRITING = 2,
  DTLV_CHECKPOINT_STAGE_FINALIZING = 3
} dtlv_checkpoint_stage;

typedef struct {
  uint8_t version;
  uint8_t stage;
  uint16_t reserved;
  uint32_t chunk_cursor;
  uint64_t snapshot_seq;
  dtlv_uuid128 writer_uuid;
} dtlv_checkpoint_pending_record;

typedef struct {
  uint8_t version;
  uint8_t reserved;
  uint16_t header_len;
  uint32_t chunk_len;
  uint32_t checksum;
} dtlv_snapshot_chunk_header;

typedef struct {
  uint8_t version;
  uint8_t multi;
  uint16_t reserved;
  uint32_t metric_kind;
  uint32_t quantization;
  uint64_t dimensions;
  uint64_t connectivity;
  uint64_t expansion_add;
  uint64_t expansion_search;
} dtlv_usearch_init_record;

static int dtlv_usearch_error_status(usearch_error_t err) {
  if (!err) return 0;
  return EIO;
}

static int dtlv_trace_enabled(void) {
  const char *flag = getenv("DTLV_TRACE_TESTS");
  return flag && *flag;
}

static int dtlv_env_flag_enabled(const char *flag) {
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

struct dtlv_usearch_domain {
  MDB_env *env;
  MDB_dbi meta_dbi;
  MDB_dbi delta_dbi;
  MDB_dbi snapshot_dbi;
  char domain_name[256];
  char meta_name[256];
  char delta_name[256];
  char snapshot_name[256];
  char filesystem_root[PATH_MAX];
  char pending_root[PATH_MAX];
  char pins_path[PATH_MAX];
#ifndef _WIN32
  int pins_fd;
#else
  HANDLE pins_file;
  HANDLE pins_mapping;
#endif
  uint8_t *pins_map;
  size_t pins_map_size;
  uint32_t pins_slot_count;
  uint32_t pin_ttl_ms;
  uint32_t pin_heartbeat_ms;
  uint32_t chunk_bytes;
  uint32_t checkpoint_chunk_batch;
  uint32_t snapshot_retention_count;
  struct dtlv_usearch_handle *handles_head;
};

struct dtlv_usearch_txn_ctx {
  dtlv_usearch_domain *domain;
  MDB_txn *txn;
  dtlv_usearch_wal_ctx *wal;
  uint64_t snapshot_seq;
  uint64_t log_seq_head;
  uint64_t last_log_seq;
  uint32_t frames_appended;
};

struct dtlv_usearch_handle {
  dtlv_usearch_domain *domain;
  usearch_index_t index;
  usearch_scalar_kind_t scalar_kind;
  uint64_t snapshot_seq;
  uint64_t log_seq;
  struct dtlv_usearch_handle *next;
  struct dtlv_usearch_handle *prev;
};

static void dtlv_domain_register_handle(dtlv_usearch_domain *domain, dtlv_usearch_handle *handle) {
  if (!domain || !handle) return;
  handle->next = domain->handles_head;
  handle->prev = NULL;
  if (domain->handles_head) domain->handles_head->prev = handle;
  domain->handles_head = handle;
}

static void dtlv_domain_unregister_handle(dtlv_usearch_domain *domain, dtlv_usearch_handle *handle) {
  if (!domain || !handle) return;
  if (handle->prev) handle->prev->next = handle->next;
  if (handle->next) handle->next->prev = handle->prev;
  if (domain->handles_head == handle) domain->handles_head = handle->next;
  handle->next = NULL;
  handle->prev = NULL;
}

typedef struct {
  uint8_t version;
  uint8_t op;
  uint8_t key_len;
  uint8_t reserved;
  uint32_t ordinal;
  dtlv_uuid128 txn_token;
  uint32_t payload_len;
  uint32_t checksum;
  const uint8_t *key_bytes;
  const uint8_t *payload_bytes;
} dtlv_delta_entry_view;

typedef struct {
  uint8_t version;
  uint8_t reserved[7];
  dtlv_uuid128 reader_uuid;
  uint64_t snapshot_seq;
  uint64_t log_seq;
  int64_t expires_at_ms;
} dtlv_reader_pin_record;

#define DTLV_DELTA_HEADER_LEN 32u

static int dtlv_decode_delta(const MDB_val *val, dtlv_delta_entry_view *entry);
int dtlv_usearch_checkpoint_recover(dtlv_usearch_domain *domain);
static int dtlv_usearch_wal_recover(dtlv_usearch_domain *domain);
static int dtlv_meta_put_u32(MDB_txn *txn, MDB_dbi dbi, const char *key, uint32_t value);

static int64_t dtlv_now_ms(void) {
#ifdef _WIN32
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  ULARGE_INTEGER li;
  li.LowPart = ft.dwLowDateTime;
  li.HighPart = ft.dwHighDateTime;
  return (int64_t)(li.QuadPart / 10000);
#else
#ifdef CLOCK_REALTIME
  struct timespec ts;
  if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
  }
#endif
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

static int dtlv_ensure_directory(const char *path) {
  if (!path || !*path) return EINVAL;
#ifdef _WIN32
  if (_mkdir(path) == 0) return 0;
  if (errno == EEXIST) return 0;
  return errno ? errno : EIO;
#else
  if (mkdir(path, 0775) == 0) return 0;
  if (errno == EEXIST) return 0;
  return errno ? errno : EIO;
#endif
}

static void dtlv_pin_pack_key(const dtlv_uuid128 *uuid, uint8_t out[16]) {
  if (!uuid || !out) return;
  uint64_t hi = dtlv_to_be64(uuid->hi);
  uint64_t lo = dtlv_to_be64(uuid->lo);
  memcpy(out, &hi, sizeof(hi));
  memcpy(out + sizeof(hi), &lo, sizeof(lo));
}

static void dtlv_pin_pack_record(uint8_t *dst,
                                 const dtlv_uuid128 *reader_uuid,
                                 uint64_t snapshot_seq,
                                 uint64_t log_seq,
                                 int64_t expires_at_ms) {
  if (!dst) return;
  memset(dst, 0, DTLV_READER_PIN_RECORD_SIZE);
  dst[0] = (uint8_t)DTLV_READER_PIN_VERSION;
  uint8_t uuid_bytes[16];
  dtlv_pin_pack_key(reader_uuid, uuid_bytes);
  memcpy(dst + 8, uuid_bytes, sizeof(uuid_bytes));
  uint64_t snap_be = dtlv_to_be64(snapshot_seq);
  memcpy(dst + 24, &snap_be, sizeof(snap_be));
  uint64_t log_be = dtlv_to_be64(log_seq);
  memcpy(dst + 32, &log_be, sizeof(log_be));
  uint64_t expires_be = dtlv_to_be64((uint64_t)expires_at_ms);
  memcpy(dst + 40, &expires_be, sizeof(expires_be));
}

static int dtlv_pin_unpack_record(const uint8_t *src, dtlv_reader_pin_record *record) {
  if (!src) return EINVAL;
  uint8_t version = src[0];
  if (version == 0) return ENOENT;
  if (version != DTLV_READER_PIN_VERSION) return EINVAL;
  if (record) {
    memset(record, 0, sizeof(*record));
    record->version = version;
    uint64_t hi_be = 0;
    uint64_t lo_be = 0;
    uint64_t snap_be = 0;
    uint64_t log_be = 0;
    uint64_t expires_be = 0;
    memcpy(&hi_be, src + 8, sizeof(hi_be));
    memcpy(&lo_be, src + 16, sizeof(lo_be));
    memcpy(&snap_be, src + 24, sizeof(snap_be));
    memcpy(&log_be, src + 32, sizeof(log_be));
    memcpy(&expires_be, src + 40, sizeof(expires_be));
    record->reader_uuid.hi = dtlv_from_be64(hi_be);
    record->reader_uuid.lo = dtlv_from_be64(lo_be);
    record->snapshot_seq = dtlv_from_be64(snap_be);
    record->log_seq = dtlv_from_be64(log_be);
    record->expires_at_ms = (int64_t)dtlv_from_be64(expires_be);
  }
  return 0;
}

static uint8_t *dtlv_pin_slot_ptr(dtlv_usearch_domain *domain, size_t slot_idx) {
  if (!domain || !domain->pins_map) return NULL;
  size_t offset = slot_idx * (size_t)DTLV_READER_PIN_RECORD_SIZE;
  if (offset + DTLV_READER_PIN_RECORD_SIZE > domain->pins_map_size) return NULL;
  return domain->pins_map + offset;
}

static void dtlv_pin_write_slot(uint8_t *slot,
                                const dtlv_uuid128 *reader_uuid,
                                uint64_t snapshot_seq,
                                uint64_t log_seq,
                                int64_t expires_at_ms) {
  if (!slot) return;
  uint8_t buf[DTLV_READER_PIN_RECORD_SIZE];
  dtlv_pin_pack_record(buf, reader_uuid, snapshot_seq, log_seq, expires_at_ms);
  slot[0] = 0;
  memcpy(slot + 1, buf + 1, DTLV_READER_PIN_RECORD_SIZE - 1);
  slot[0] = buf[0];
}

static int dtlv_reader_pins_lock(dtlv_usearch_domain *domain) {
  if (!domain) return EINVAL;
#ifdef _WIN32
  OVERLAPPED ov = {0};
  DWORD len_low = (DWORD)(domain->pins_map_size & 0xFFFFFFFFu);
  DWORD len_high = (DWORD)(domain->pins_map_size >> 32);
  if (!LockFileEx(domain->pins_file, LOCKFILE_EXCLUSIVE_LOCK, 0, len_low, len_high, &ov)) {
    DWORD err = GetLastError();
    return err ? (int)err : EIO;
  }
  return 0;
#else
  struct flock lock;
  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(domain->pins_fd, F_SETLKW, &lock) != 0) return errno ? errno : EIO;
  return 0;
#endif
}

static void dtlv_reader_pins_unlock(dtlv_usearch_domain *domain) {
  if (!domain) return;
#ifdef _WIN32
  OVERLAPPED ov = {0};
  DWORD len_low = (DWORD)(domain->pins_map_size & 0xFFFFFFFFu);
  DWORD len_high = (DWORD)(domain->pins_map_size >> 32);
  UnlockFileEx(domain->pins_file, 0, len_low, len_high, &ov);
#else
  struct flock lock;
  memset(&lock, 0, sizeof(lock));
  lock.l_type = F_UNLCK;
  lock.l_whence = SEEK_SET;
  fcntl(domain->pins_fd, F_SETLK, &lock);
#endif
}

static int dtlv_reader_pins_open(dtlv_usearch_domain *domain) {
  if (!domain) return EINVAL;
  if (snprintf(domain->pins_path, sizeof(domain->pins_path), "%s/reader-pins.lock", domain->filesystem_root) >=
      (int)sizeof(domain->pins_path)) {
    return ENAMETOOLONG;
  }
#ifdef _WIN32
  char root_path[PATH_MAX];
  snprintf(root_path, sizeof(root_path), "%s", domain->filesystem_root);
  dtlv_fix_windows_path(root_path);
  int rc = dtlv_ensure_directory(root_path);
#else
  int rc = dtlv_ensure_directory(domain->filesystem_root);
#endif
  if (rc != 0 && rc != EEXIST) return rc;
  size_t map_size = (size_t)DTLV_READER_PIN_RECORD_SIZE * (size_t)DTLV_READER_PIN_SLOT_COUNT;
#ifdef _WIN32
  char win_path[PATH_MAX];
  snprintf(win_path, sizeof(win_path), "%s", domain->pins_path);
  dtlv_fix_windows_path(win_path);
  HANDLE file = CreateFileA(win_path,
                            GENERIC_READ | GENERIC_WRITE,
                            FILE_SHARE_READ | FILE_SHARE_WRITE,
                            NULL,
                            OPEN_ALWAYS,
                            FILE_ATTRIBUTE_NORMAL,
                            NULL);
  if (file == INVALID_HANDLE_VALUE) return (int)GetLastError();
  LARGE_INTEGER size;
  size.QuadPart = (LONGLONG)map_size;
  if (!SetFilePointerEx(file, size, NULL, FILE_BEGIN) || !SetEndOfFile(file)) {
    DWORD err = GetLastError();
    CloseHandle(file);
    return err ? (int)err : EIO;
  }
  HANDLE mapping = CreateFileMappingA(file, NULL, PAGE_READWRITE, size.HighPart, size.LowPart, NULL);
  if (!mapping) {
    DWORD err = GetLastError();
    CloseHandle(file);
    return err ? (int)err : EIO;
  }
  uint8_t *base = (uint8_t *)MapViewOfFile(mapping, FILE_MAP_WRITE, 0, 0, map_size);
  if (!base) {
    DWORD err = GetLastError();
    CloseHandle(mapping);
    CloseHandle(file);
    return err ? (int)err : EIO;
  }
  domain->pins_file = file;
  domain->pins_mapping = mapping;
  domain->pins_map = base;
#else
  int fd = open(domain->pins_path, O_RDWR | O_CREAT, 0664);
  if (fd < 0) return errno ? errno : EIO;
  struct stat st;
  if (fstat(fd, &st) != 0) {
    int err = errno ? errno : EIO;
    close(fd);
    return err;
  }
  if ((size_t)st.st_size != map_size) {
    if (ftruncate(fd, (off_t)map_size) != 0) {
      int err = errno ? errno : EIO;
      close(fd);
      return err;
    }
  }
  void *base = mmap(NULL, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  if (base == MAP_FAILED) {
    int err = errno ? errno : EIO;
    close(fd);
    return err;
  }
  domain->pins_fd = fd;
  domain->pins_map = (uint8_t *)base;
#endif
  domain->pins_map_size = map_size;
  domain->pins_slot_count = DTLV_READER_PIN_SLOT_COUNT;
  return 0;
}

static void dtlv_reader_pins_close(dtlv_usearch_domain *domain) {
  if (!domain) return;
#ifdef _WIN32
  if (domain->pins_map) UnmapViewOfFile(domain->pins_map);
  if (domain->pins_mapping) CloseHandle(domain->pins_mapping);
  if (domain->pins_file && domain->pins_file != INVALID_HANDLE_VALUE) CloseHandle(domain->pins_file);
  domain->pins_map = NULL;
  domain->pins_mapping = NULL;
  domain->pins_file = NULL;
#else
  if (domain->pins_map && domain->pins_map_size) munmap(domain->pins_map, domain->pins_map_size);
  if (domain->pins_fd >= 0) close(domain->pins_fd);
  domain->pins_fd = -1;
  domain->pins_map = NULL;
#endif
  domain->pins_map_size = 0;
  domain->pins_slot_count = 0;
}

static MDB_val dtlv_meta_key(const char *key) {
  MDB_val k;
  k.mv_size = strlen(key) + 1;
  k.mv_data = (void *)key;
  return k;
}

static size_t dtlv_u64_to_size(uint64_t value) {
  if (value > (uint64_t)SIZE_MAX) return SIZE_MAX;
  return (size_t)value;
}

static uint32_t dtlv_size_to_u32(size_t value) {
  if (value > (size_t)UINT32_MAX) return UINT32_MAX;
  return (uint32_t)value;
}

static uint64_t dtlv_usearch_reserve_hint(uint64_t snapshot_seq, uint64_t log_seq) {
  uint64_t hint = log_seq;
  if (hint < snapshot_seq) hint = snapshot_seq;
  if (hint < 16u) hint = 16u;
  return hint;
}

static int dtlv_meta_put_u64(MDB_txn *txn, MDB_dbi dbi, const char *key, uint64_t value) {
  uint64_t be = dtlv_to_be64(value);
  MDB_val k = dtlv_meta_key(key);
  MDB_val v = {.mv_size = sizeof(be), .mv_data = &be};
  return mdb_put(txn, dbi, &k, &v, 0);
}

static int dtlv_meta_put_u32(MDB_txn *txn, MDB_dbi dbi, const char *key, uint32_t value) {
  uint32_t be = dtlv_to_be32(value);
  MDB_val k = dtlv_meta_key(key);
  MDB_val v = {.mv_size = sizeof(be), .mv_data = &be};
  return mdb_put(txn, dbi, &k, &v, 0);
}

static int dtlv_meta_get_u64(MDB_txn *txn,
                             MDB_dbi dbi,
                             const char *key,
                             uint64_t default_value,
                             uint64_t *out) {
  MDB_val k = dtlv_meta_key(key);
  MDB_val v;
  int rc = mdb_get(txn, dbi, &k, &v);
  if (rc == MDB_NOTFOUND) {
    *out = default_value;
    return 0;
  }
  if (rc != 0) return rc;
  if (v.mv_size != sizeof(uint64_t)) return MDB_CORRUPTED;
  uint64_t be;
  memcpy(&be, v.mv_data, sizeof(be));
  *out = dtlv_from_be64(be);
  return 0;
}

static int dtlv_meta_get_u32(MDB_txn *txn,
                             MDB_dbi dbi,
                             const char *key,
                             uint32_t default_value,
                             uint32_t *out) {
  MDB_val k = dtlv_meta_key(key);
  MDB_val v;
  int rc = mdb_get(txn, dbi, &k, &v);
  if (rc == MDB_NOTFOUND) {
    *out = default_value;
    return 0;
  }
  if (rc != 0) return rc;
  if (v.mv_size != sizeof(uint32_t)) return MDB_CORRUPTED;
  uint32_t be;
  memcpy(&be, v.mv_data, sizeof(be));
  *out = dtlv_from_be32(be);
  return 0;
}

static int dtlv_meta_put_bytes(MDB_txn *txn,
                               MDB_dbi dbi,
                               const char *key,
                               const void *buf,
                               size_t len) {
  MDB_val k = dtlv_meta_key(key);
  MDB_val v = {.mv_size = len, .mv_data = (void *)buf};
  return mdb_put(txn, dbi, &k, &v, 0);
}

static int dtlv_meta_delete(MDB_txn *txn, MDB_dbi dbi, const char *key) {
  MDB_val k = dtlv_meta_key(key);
  int rc = mdb_del(txn, dbi, &k, NULL);
  if (rc == MDB_NOTFOUND) return 0;
  return rc;
}

static int dtlv_meta_get_sealed_log_seq(MDB_txn *txn,
                                        MDB_dbi dbi,
                                        dtlv_uuid128 *token_out,
                                        uint64_t *log_seq_out,
                                        int *found) {
  if (found) *found = 0;
  if (!txn) return EINVAL;
  MDB_val key = dtlv_meta_key(DTLV_META_KEY_SEALED_LOG_SEQ);
  MDB_val val;
  int rc = mdb_get(txn, dbi, &key, &val);
  if (rc == MDB_NOTFOUND) return 0;
  if (rc != 0) return rc;
  if (val.mv_size != sizeof(uint64_t) * 3) return MDB_CORRUPTED;
  const uint8_t *ptr = (const uint8_t *)val.mv_data;
  uint64_t hi_be;
  uint64_t lo_be;
  uint64_t seq_be;
  memcpy(&hi_be, ptr, sizeof(hi_be));
  memcpy(&lo_be, ptr + sizeof(hi_be), sizeof(lo_be));
  memcpy(&seq_be, ptr + sizeof(hi_be) + sizeof(lo_be), sizeof(seq_be));
  if (token_out) {
    token_out->hi = dtlv_from_be64(hi_be);
    token_out->lo = dtlv_from_be64(lo_be);
  }
  if (log_seq_out) *log_seq_out = dtlv_from_be64(seq_be);
  if (found) *found = 1;
  return 0;
}

static int dtlv_meta_get_published_log_tail(MDB_txn *txn,
                                            MDB_dbi dbi,
                                            dtlv_uuid128 *token_out,
                                            uint32_t *ordinal_out,
                                            int *found) {
  if (found) *found = 0;
  if (!txn) return EINVAL;
  MDB_val key = dtlv_meta_key(DTLV_META_KEY_PUBLISHED_LOG_TAIL);
  MDB_val val;
  int rc = mdb_get(txn, dbi, &key, &val);
  if (rc == MDB_NOTFOUND) return 0;
  if (rc != 0) return rc;
  if (val.mv_size != sizeof(uint64_t) * 2 + sizeof(uint32_t) * 2) return MDB_CORRUPTED;
  const uint8_t *ptr = (const uint8_t *)val.mv_data;
  uint64_t hi_be;
  uint64_t lo_be;
  uint32_t ordinal_be;
  memcpy(&hi_be, ptr, sizeof(hi_be));
  memcpy(&lo_be, ptr + sizeof(hi_be), sizeof(lo_be));
  memcpy(&ordinal_be, ptr + sizeof(hi_be) + sizeof(lo_be), sizeof(ordinal_be));
  if (token_out) {
    token_out->hi = dtlv_from_be64(hi_be);
    token_out->lo = dtlv_from_be64(lo_be);
  }
  if (ordinal_out) *ordinal_out = dtlv_from_be32(ordinal_be);
  if (found) *found = 1;
  return 0;
}

static int dtlv_meta_put_published_log_tail(MDB_txn *txn,
                                            MDB_dbi dbi,
                                            const dtlv_uuid128 *token,
                                            uint32_t ordinal) {
  if (!txn || !token) return EINVAL;
  struct {
    uint64_t token_hi;
    uint64_t token_lo;
    uint32_t ordinal;
    uint32_t reserved;
  } record;
  record.token_hi = dtlv_to_be64(token->hi);
  record.token_lo = dtlv_to_be64(token->lo);
  record.ordinal = dtlv_to_be32(ordinal);
  record.reserved = 0;
  MDB_val key = dtlv_meta_key(DTLV_META_KEY_PUBLISHED_LOG_TAIL);
  MDB_val val = {.mv_size = sizeof(record), .mv_data = &record};
  return mdb_put(txn, dbi, &key, &val, 0);
}

static int dtlv_usearch_update_published_tail(dtlv_usearch_domain *domain,
                                              const dtlv_uuid128 *token,
                                              uint32_t ordinal) {
  if (!domain || !domain->env || !token) return EINVAL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, 0, &txn);
  if (rc != 0) return rc;
  rc = dtlv_meta_put_published_log_tail(txn, domain->meta_dbi, token, ordinal);
  if (rc == 0) rc = mdb_txn_commit(txn);
  else if (txn) mdb_txn_abort(txn);
  return rc;
}

static int dtlv_usearch_read_published_tail(dtlv_usearch_domain *domain,
                                            dtlv_uuid128 *token_out,
                                            uint32_t *ordinal_out,
                                            int *found) {
  if (found) *found = 0;
  if (!domain || !domain->env) return EINVAL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  rc = dtlv_meta_get_published_log_tail(txn, domain->meta_dbi, token_out, ordinal_out, found);
  mdb_txn_abort(txn);
  return rc;
}

static void dtlv_snapshot_pack_key(uint64_t snapshot_seq, uint32_t chunk, uint8_t out[12]) {
  uint64_t seq_be = dtlv_to_be64(snapshot_seq);
  uint32_t chunk_be = dtlv_to_be32(chunk);
  memcpy(out, &seq_be, sizeof(seq_be));
  memcpy(out + sizeof(seq_be), &chunk_be, sizeof(chunk_be));
}

static int dtlv_snapshot_load(dtlv_usearch_domain *domain,
                              MDB_txn *txn,
                              uint64_t snapshot_seq,
                              uint8_t **buffer_out,
                              size_t *length_out) {
  if (!domain || !txn || !buffer_out || !length_out) return EINVAL;
  *buffer_out = NULL;
  *length_out = 0;
  if (snapshot_seq == 0) return 0;
  MDB_cursor *cursor = NULL;
  int rc = mdb_cursor_open(txn, domain->snapshot_dbi, &cursor);
  if (rc != 0) return rc;
  uint8_t key_buf[12];
  dtlv_snapshot_pack_key(snapshot_seq, 0, key_buf);
  MDB_val key = {.mv_data = key_buf, .mv_size = sizeof(key_buf)};
  MDB_val val;
  rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
  size_t expected_chunk = 0;
  uint8_t *buffer = NULL;
  size_t buffer_len = 0;
  while (rc == 0) {
    if (key.mv_size != sizeof(key_buf)) {
      rc = MDB_CORRUPTED;
      break;
    }
    uint64_t seq_be;
    memcpy(&seq_be, key.mv_data, sizeof(seq_be));
    uint64_t seq = dtlv_from_be64(seq_be);
    if (seq != snapshot_seq) break;
    uint32_t chunk_be;
    memcpy(&chunk_be, (uint8_t *)key.mv_data + sizeof(seq_be), sizeof(chunk_be));
    uint32_t chunk = dtlv_from_be32(chunk_be);
    if (chunk != expected_chunk) {
      rc = MDB_CORRUPTED;
      break;
    }
    if (val.mv_size < DTLV_SNAPSHOT_CHUNK_HEADER_SIZE) {
      rc = MDB_CORRUPTED;
      break;
    }
    const uint8_t *ptr = (const uint8_t *)val.mv_data;
    if (ptr[0] != (uint8_t)DTLV_USEARCH_SNAPSHOT_CHUNK_VERSION) {
      rc = EINVAL;
      break;
    }
    uint16_t header_len_be;
    memcpy(&header_len_be, ptr + 2, sizeof(header_len_be));
    uint16_t header_len = dtlv_from_be16(header_len_be);
    if (header_len != DTLV_SNAPSHOT_CHUNK_HEADER_SIZE) {
      rc = MDB_CORRUPTED;
      break;
    }
    uint32_t chunk_len_be;
    memcpy(&chunk_len_be, ptr + 4, sizeof(chunk_len_be));
    uint32_t chunk_len = dtlv_from_be32(chunk_len_be);
    uint32_t checksum_be;
    memcpy(&checksum_be, ptr + 8, sizeof(checksum_be));
    uint32_t checksum = dtlv_from_be32(checksum_be);
    if (val.mv_size != (size_t)DTLV_SNAPSHOT_CHUNK_HEADER_SIZE + chunk_len) {
      rc = MDB_CORRUPTED;
      break;
    }
    const uint8_t *payload = ptr + DTLV_SNAPSHOT_CHUNK_HEADER_SIZE;
    uint32_t computed = dtlv_crc32c(payload, chunk_len);
    if (computed != checksum) {
      rc = EIO;
      break;
    }
    size_t needed = buffer_len + chunk_len;
    uint8_t *tmp = realloc(buffer, needed ? needed : 1);
    if (!tmp) {
      rc = ENOMEM;
      break;
    }
    buffer = tmp;
    if (chunk_len) memcpy(buffer + buffer_len, payload, chunk_len);
    buffer_len += chunk_len;
    expected_chunk++;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  if (rc == MDB_NOTFOUND) rc = 0;
  mdb_cursor_close(cursor);
  if (rc != 0) {
    free(buffer);
    return rc;
  }
  if (expected_chunk == 0) {
    free(buffer);
    *buffer_out = NULL;
    *length_out = 0;
    return 0;
  }
  *buffer_out = buffer;
  *length_out = buffer_len;
  return 0;
}

static int dtlv_snapshot_delete_chunks_from(dtlv_usearch_domain *domain,
                                            MDB_txn *txn,
                                            uint64_t snapshot_seq,
                                            uint32_t chunk_start) {
  MDB_cursor *cursor = NULL;
  int rc = mdb_cursor_open(txn, domain->snapshot_dbi, &cursor);
  if (rc != 0) return rc;
  uint8_t key_buf[12];
  dtlv_snapshot_pack_key(snapshot_seq, chunk_start, key_buf);
  MDB_val key = {.mv_data = key_buf, .mv_size = sizeof(key_buf)};
  MDB_val val;
  rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
  while (rc == 0) {
    if (key.mv_size != sizeof(key_buf)) {
      rc = MDB_CORRUPTED;
      break;
    }
    uint64_t seq_be;
    memcpy(&seq_be, key.mv_data, sizeof(seq_be));
    uint64_t seq = dtlv_from_be64(seq_be);
    if (seq > snapshot_seq) break;
    if (seq < snapshot_seq) {
      rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
      continue;
    }
    rc = mdb_cursor_del(cursor, 0);
    if (rc != 0) break;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  if (rc == MDB_NOTFOUND) rc = 0;
  mdb_cursor_close(cursor);
  return rc;
}

static int dtlv_snapshot_delete_before(dtlv_usearch_domain *domain,
                                       MDB_txn *txn,
                                       uint64_t floor_seq) {
  if (!floor_seq) return 0;
  MDB_cursor *cursor = NULL;
  int rc = mdb_cursor_open(txn, domain->snapshot_dbi, &cursor);
  if (rc != 0) return rc;
  MDB_val key;
  MDB_val val;
  rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
  while (rc == 0) {
    if (key.mv_size != sizeof(uint64_t) + sizeof(uint32_t)) {
      rc = MDB_CORRUPTED;
      break;
    }
    uint64_t seq_be;
    memcpy(&seq_be, key.mv_data, sizeof(seq_be));
    uint64_t seq = dtlv_from_be64(seq_be);
    if (seq >= floor_seq) break;
    rc = mdb_cursor_del(cursor, 0);
    if (rc != 0) break;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  if (rc == MDB_NOTFOUND) rc = 0;
  mdb_cursor_close(cursor);
  return rc;
}

static int dtlv_delta_delete_upto(dtlv_usearch_domain *domain,
                                  MDB_txn *txn,
                                  uint64_t upto_seq) {
  MDB_cursor *cursor = NULL;
  int rc = mdb_cursor_open(txn, domain->delta_dbi, &cursor);
  if (rc != 0) return rc;
  MDB_val key;
  MDB_val val;
  rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
  while (rc == 0) {
    if (key.mv_size != sizeof(uint64_t)) {
      rc = MDB_CORRUPTED;
      break;
    }
    uint64_t seq_be;
    memcpy(&seq_be, key.mv_data, sizeof(seq_be));
    uint64_t seq = dtlv_from_be64(seq_be);
    if (seq > upto_seq) break;
    dtlv_delta_entry_view entry;
    rc = dtlv_decode_delta(&val, &entry);
    if (rc != 0) break;
    rc = mdb_cursor_del(cursor, 0);
    if (rc != 0) break;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  if (rc == MDB_NOTFOUND) rc = 0;
  mdb_cursor_close(cursor);
  return rc;
}

static int dtlv_decode_delta(const MDB_val *val, dtlv_delta_entry_view *entry) {
  if (!val || !entry) return EINVAL;
  if (val->mv_size < DTLV_DELTA_HEADER_LEN) return MDB_CORRUPTED;
  const uint8_t *src = (const uint8_t *)val->mv_data;
  memset(entry, 0, sizeof(*entry));
  entry->version = src[0];
  if (entry->version != 1) return EINVAL;
  entry->op = src[1];
  entry->key_len = src[2];
  entry->reserved = src[3];
  uint32_t ordinal_be;
  memcpy(&ordinal_be, src + 4, sizeof(ordinal_be));
  entry->ordinal = dtlv_from_be32(ordinal_be);
  uint64_t token_hi;
  memcpy(&token_hi, src + 8, sizeof(token_hi));
  entry->txn_token.hi = dtlv_from_be64(token_hi);
  uint64_t token_lo;
  memcpy(&token_lo, src + 16, sizeof(token_lo));
  entry->txn_token.lo = dtlv_from_be64(token_lo);
  uint32_t payload_len_be;
  memcpy(&payload_len_be, src + 24, sizeof(payload_len_be));
  entry->payload_len = dtlv_from_be32(payload_len_be);
  uint32_t checksum_be;
  memcpy(&checksum_be, src + 28, sizeof(checksum_be));
  entry->checksum = dtlv_from_be32(checksum_be);
  size_t expected = (size_t)DTLV_DELTA_HEADER_LEN + entry->key_len + entry->payload_len;
  if (val->mv_size != expected) return MDB_CORRUPTED;
  entry->key_bytes = src + DTLV_DELTA_HEADER_LEN;
  entry->payload_bytes = entry->key_bytes + entry->key_len;
  uint32_t computed = dtlv_crc32c(entry->key_bytes, entry->key_len + entry->payload_len);
  if (computed != entry->checksum) return EIO;
  return 0;
}

static int dtlv_delta_extract_key(const dtlv_delta_entry_view *entry, usearch_key_t *key_out) {
  if (!entry || !key_out) return EINVAL;
  if (!entry->key_bytes || entry->key_len != sizeof(uint64_t)) return MDB_CORRUPTED;
  uint64_t be = 0;
  memcpy(&be, entry->key_bytes, sizeof(be));
  *key_out = (usearch_key_t)dtlv_from_be64(be);
  return 0;
}

static int dtlv_delta_payload_view(const dtlv_delta_entry_view *entry,
                                   const uint8_t **payload_out,
                                   size_t *payload_len_out) {
  if (!entry || !payload_out || !payload_len_out) return EINVAL;
  *payload_out = entry->payload_len ? entry->payload_bytes : NULL;
  *payload_len_out = entry->payload_len;
  return 0;
}

static int dtlv_apply_delta_entry(dtlv_usearch_domain *domain,
                                  MDB_txn *txn,
                                  usearch_index_t index,
                                  usearch_scalar_kind_t scalar_kind,
                                  const dtlv_delta_entry_view *entry) {
  if (!domain || !txn || !index || !entry) return EINVAL;
  usearch_key_t key = 0;
  int rc = dtlv_delta_extract_key(entry, &key);
  if (rc != 0) return rc;
  usearch_error_t err = NULL;
  switch (entry->op) {
    case DTLV_USEARCH_OP_ADD:
    case DTLV_USEARCH_OP_REPLACE: {
      const uint8_t *payload = NULL;
      size_t payload_len = 0;
      rc = dtlv_delta_payload_view(entry, &payload, &payload_len);
      if (rc != 0) return rc;
      if (!payload || payload_len == 0) return MDB_CORRUPTED;
      if (entry->op == DTLV_USEARCH_OP_REPLACE) {
        err = NULL;
        usearch_remove(index, key, &err);
      }
      err = NULL;
      usearch_add(index, key, payload, scalar_kind, &err);
      if (err && entry->op == DTLV_USEARCH_OP_ADD) {
        err = NULL;
        usearch_remove(index, key, &err);
        err = NULL;
        usearch_add(index, key, payload, scalar_kind, &err);
      }
      return dtlv_usearch_error_status(err);
    }
    case DTLV_USEARCH_OP_DELETE:
      usearch_remove(index, key, &err);
      return dtlv_usearch_error_status(err);
    default:
      return EINVAL;
  }
}

static int dtlv_publish_apply_entry(dtlv_usearch_domain *domain,
                                    MDB_txn *txn,
                                    const dtlv_delta_entry_view *entry) {
  if (!domain || !entry) return EINVAL;
  dtlv_usearch_handle *handle = domain->handles_head;
  while (handle) {
    if (handle->index) {
      int rc = dtlv_apply_delta_entry(domain, txn, handle->index, handle->scalar_kind, entry);
      if (rc != 0) return rc;
    }
    handle = handle->next;
  }
  return 0;
}

static int dtlv_usearch_replay_wal_file(dtlv_usearch_domain *domain,
                                        const char *path,
                                        const dtlv_uuid128 *expected_token,
                                        uint32_t start_ordinal,
                                        int unlink_after_publish) {
  if (!domain || !path || !expected_token) return EINVAL;
  FILE *fp = fopen(path, "rb");
  if (!fp) return errno ? errno : EIO;
  dtlv_ulog_header_v1 header;
  size_t read_bytes = fread(&header, 1, sizeof(header), fp);
  if (read_bytes != sizeof(header)) {
    fclose(fp);
    return EIO;
  }
  if (memcmp(header.magic, DTLV_ULOG_MAGIC, sizeof(header.magic)) != 0) {
    dtlv_tracef("[wal] magic mismatch for %s\n", path);
    fclose(fp);
    return EINVAL;
  }
  if (header.version != DTLV_ULOG_VERSION) {
    dtlv_tracef("[wal] version mismatch for %s\n", path);
    fclose(fp);
    return EINVAL;
  }
  uint32_t stored_checksum = dtlv_from_be32(header.checksum);
  size_t checksum_offset = offsetof(dtlv_ulog_header_v1, header_len);
  size_t checksum_len = sizeof(header) - checksum_offset - sizeof(uint32_t);
  uint32_t computed_checksum = dtlv_crc32c(((const uint8_t *)&header) + checksum_offset, checksum_len);
  if (computed_checksum != stored_checksum) {
    fclose(fp);
    return EIO;
  }
  uint16_t header_len = dtlv_from_be16(header.header_len);
  if (header_len != sizeof(header)) {
    dtlv_tracef("[wal] header length mismatch for %s\n", path);
    fclose(fp);
    return MDB_CORRUPTED;
  }
  dtlv_uuid128 file_token = {
      .hi = dtlv_from_be64(header.txn_token_hi),
      .lo = dtlv_from_be64(header.txn_token_lo),
  };
  if (file_token.hi != expected_token->hi || file_token.lo != expected_token->lo) {
    dtlv_tracef("[wal] token mismatch for %s\n", path);
    fclose(fp);
    return EINVAL;
  }
  uint32_t frame_count = dtlv_from_be32(header.frame_count);
  if (start_ordinal <= 1) start_ordinal = 1;
  dtlv_tracef("[wal] reader_header_size=%zu\n", sizeof(dtlv_ulog_header_v1));
  dtlv_tracef("[wal] replay path=%s start=%u frames=%u\n", path, (unsigned)start_ordinal, (unsigned)frame_count);
  MDB_txn *read_txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &read_txn);
  if (rc != 0) {
    fclose(fp);
    return rc;
  }
  uint32_t processed = 0;
  for (uint32_t ordinal = 1; ordinal <= frame_count; ++ordinal) {
    dtlv_ulog_frame_prefix_v1 prefix;
    read_bytes = fread(&prefix, 1, sizeof(prefix), fp);
    if (read_bytes != sizeof(prefix)) {
      dtlv_tracef("[wal] short read on prefix at ordinal %u\n", (unsigned)ordinal);
      rc = EIO;
      break;
    }
    uint32_t frame_ordinal = dtlv_from_be32(prefix.ordinal);
    uint32_t delta_bytes = dtlv_from_be32(prefix.delta_bytes);
    uint32_t checksum = dtlv_from_be32(prefix.checksum);
    if (frame_ordinal != ordinal) {
      dtlv_tracef("[wal] ordinal mismatch %u != %u\n", (unsigned)frame_ordinal, (unsigned)ordinal);
      rc = MDB_CORRUPTED;
      break;
    }
    if (delta_bytes == 0) {
      dtlv_tracef("[wal] zero-length frame at ordinal %u\n", (unsigned)ordinal);
      rc = MDB_CORRUPTED;
      break;
    }
    uint8_t *payload = (uint8_t *)malloc(delta_bytes);
    if (!payload) {
      rc = ENOMEM;
      break;
    }
    size_t payload_read = fread(payload, 1, delta_bytes, fp);
    if (payload_read != delta_bytes) {
      free(payload);
      dtlv_tracef("[wal] payload short read at ordinal %u\n", (unsigned)ordinal);
      rc = EIO;
      break;
    }
    uint32_t payload_crc = dtlv_crc32c(payload, delta_bytes);
    if (payload_crc != checksum) {
      free(payload);
      dtlv_tracef("[wal] crc mismatch at ordinal %u\n", (unsigned)ordinal);
      rc = EIO;
      break;
    }
    if (ordinal >= start_ordinal) {
      MDB_val val = {.mv_size = delta_bytes, .mv_data = payload};
      dtlv_delta_entry_view entry;
      rc = dtlv_decode_delta(&val, &entry);
      if (rc != 0) dtlv_tracef("[wal] decode error %d at ordinal %u\n", rc, (unsigned)ordinal);
      if (rc == 0) rc = dtlv_publish_apply_entry(domain, read_txn, &entry);
      if (rc == 0) rc = dtlv_usearch_update_published_tail(domain, &file_token, ordinal);
      if (rc != 0) {
        dtlv_tracef("[wal] apply/update error %d at ordinal %u\n", rc, (unsigned)ordinal);
        free(payload);
        break;
      }
    }
    free(payload);
    processed = ordinal;
  }
  mdb_txn_abort(read_txn);
  int close_rc = fclose(fp);
  if (rc == 0 && close_rc != 0) rc = errno ? errno : EIO;
  if (rc == 0 && unlink_after_publish && processed == frame_count) {
    if (remove(path) != 0 && errno != ENOENT) {
      dtlv_tracef("[wal] unlink failed for %s\n", path);
      rc = errno ? errno : EIO;
    }
  }
  return rc;
}

static int dtlv_replay_deltas(dtlv_usearch_domain *domain,
                              MDB_txn *txn,
                              usearch_index_t index,
                              usearch_scalar_kind_t scalar_kind,
                              uint64_t snapshot_seq,
                              uint64_t log_seq) {
  if (!domain || !txn || !index) return EINVAL;
  if (log_seq <= snapshot_seq) return 0;
  if (snapshot_seq == UINT64_MAX) return 0;
  MDB_cursor *cursor = NULL;
  int rc = mdb_cursor_open(txn, domain->delta_dbi, &cursor);
  if (rc != 0) return rc;
  uint64_t start_seq = snapshot_seq + 1;
  uint64_t start_be = dtlv_to_be64(start_seq);
  MDB_val key = {.mv_size = sizeof(start_be), .mv_data = &start_be};
  MDB_val val;
  rc = mdb_cursor_get(cursor, &key, &val, MDB_SET_RANGE);
  while (rc == 0) {
    if (key.mv_size != sizeof(uint64_t)) {
      rc = MDB_CORRUPTED;
      break;
    }
    uint64_t seq_be;
    memcpy(&seq_be, key.mv_data, sizeof(seq_be));
    uint64_t seq = dtlv_from_be64(seq_be);
    if (seq > log_seq) break;
    dtlv_delta_entry_view entry;
    rc = dtlv_decode_delta(&val, &entry);
    if (rc != 0) break;
    rc = dtlv_apply_delta_entry(domain, txn, index, scalar_kind, &entry);
    if (rc != 0) break;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  if (rc == MDB_NOTFOUND) rc = 0;
  mdb_cursor_close(cursor);
  return rc;
}

static int dtlv_usearch_build_index(dtlv_usearch_domain *domain,
                                    MDB_txn *txn,
                                    const usearch_init_options_t *init_opts,
                                    uint64_t snapshot_seq,
                                    uint64_t log_seq,
                                    uint64_t reserve_hint,
                                    usearch_index_t *index_out) {
  if (!domain || !txn || !init_opts || !index_out) return EINVAL;
  *index_out = NULL;
  usearch_error_t err = NULL;
  usearch_index_t index = usearch_init((usearch_init_options_t *)init_opts, &err);
  int rc = dtlv_usearch_error_status(err);
  if (rc != 0 || !index) return rc != 0 ? rc : ENOMEM;
  uint8_t *snapshot_buf = NULL;
  size_t snapshot_len = 0;
  rc = dtlv_snapshot_load(domain, txn, snapshot_seq, &snapshot_buf, &snapshot_len);
  if (rc != 0) goto fail;
  if (snapshot_len > 0 && snapshot_buf) {
    err = NULL;
    usearch_load_buffer(index, snapshot_buf, snapshot_len, &err);
    rc = dtlv_usearch_error_status(err);
    if (rc != 0) goto fail;
  }
  if (snapshot_buf) {
    free(snapshot_buf);
    snapshot_buf = NULL;
  }
  uint64_t hint = reserve_hint < 16u ? 16u : reserve_hint;
  size_t reserve_members = dtlv_u64_to_size(hint);
  err = NULL;
  usearch_reserve(index, reserve_members, &err);
  rc = dtlv_usearch_error_status(err);
  if (rc != 0) goto fail;
  rc = dtlv_replay_deltas(domain, txn, index, init_opts->quantization, snapshot_seq, log_seq);
  if (rc != 0) goto fail;
  *index_out = index;
  return 0;
fail:
  if (snapshot_buf) free(snapshot_buf);
  if (index) {
    usearch_error_t free_err = NULL;
    usearch_free(index, &free_err);
  }
  return rc;
}

static void dtlv_pack_init_record(const usearch_init_options_t *opts,
                                  uint8_t out[DTLV_INIT_RECORD_SIZE]) {
  size_t offset = 0;
  memset(out, 0, DTLV_INIT_RECORD_SIZE);
  out[offset++] = (uint8_t)DTLV_INIT_RECORD_VERSION;
  out[offset++] = opts && opts->multi ? 1u : 0u;
  uint16_t zero16 = dtlv_to_be16(0);
  memcpy(out + offset, &zero16, sizeof(zero16));
  offset += sizeof(zero16);
  uint32_t metric = dtlv_to_be32(opts ? (uint32_t)opts->metric_kind : 0u);
  memcpy(out + offset, &metric, sizeof(metric));
  offset += sizeof(metric);
  uint32_t quant = dtlv_to_be32(opts ? (uint32_t)opts->quantization : 0u);
  memcpy(out + offset, &quant, sizeof(quant));
  offset += sizeof(quant);
  uint64_t dims = dtlv_to_be64(opts ? (uint64_t)opts->dimensions : 0u);
  memcpy(out + offset, &dims, sizeof(dims));
  offset += sizeof(dims);
  uint64_t conn = dtlv_to_be64(opts ? (uint64_t)opts->connectivity : 0u);
  memcpy(out + offset, &conn, sizeof(conn));
  offset += sizeof(conn);
  uint64_t exp_add = dtlv_to_be64(opts ? (uint64_t)opts->expansion_add : 0u);
  memcpy(out + offset, &exp_add, sizeof(exp_add));
  offset += sizeof(exp_add);
  uint64_t exp_search = dtlv_to_be64(opts ? (uint64_t)opts->expansion_search : 0u);
  memcpy(out + offset, &exp_search, sizeof(exp_search));
}

static int dtlv_unpack_init_record(const MDB_val *val, usearch_init_options_t *opts) {
  if (!val || !opts) return EINVAL;
  if (val->mv_size != DTLV_INIT_RECORD_SIZE) return MDB_CORRUPTED;
  const uint8_t *src = (const uint8_t *)val->mv_data;
  if (src[0] != (uint8_t)DTLV_INIT_RECORD_VERSION) return EINVAL;
  size_t offset = 0;
  offset += 1;  // version
  uint8_t multi = src[offset++];
  offset += 2;  // reserved
  uint32_t metric_be;
  memcpy(&metric_be, src + offset, sizeof(metric_be));
  offset += sizeof(metric_be);
  uint32_t quant_be;
  memcpy(&quant_be, src + offset, sizeof(quant_be));
  offset += sizeof(quant_be);
  uint64_t dims_be;
  memcpy(&dims_be, src + offset, sizeof(dims_be));
  offset += sizeof(dims_be);
  uint64_t conn_be;
  memcpy(&conn_be, src + offset, sizeof(conn_be));
  offset += sizeof(conn_be);
  uint64_t exp_add_be;
  memcpy(&exp_add_be, src + offset, sizeof(exp_add_be));
  offset += sizeof(exp_add_be);
  uint64_t exp_search_be;
  memcpy(&exp_search_be, src + offset, sizeof(exp_search_be));
  usearch_init_options_t decoded;
  memset(&decoded, 0, sizeof(decoded));
  decoded.metric_kind = (usearch_metric_kind_t)dtlv_from_be32(metric_be);
  decoded.metric = NULL;
  decoded.quantization = (usearch_scalar_kind_t)dtlv_from_be32(quant_be);
  decoded.dimensions = (size_t)dtlv_from_be64(dims_be);
  decoded.connectivity = (size_t)dtlv_from_be64(conn_be);
  decoded.expansion_add = (size_t)dtlv_from_be64(exp_add_be);
  decoded.expansion_search = (size_t)dtlv_from_be64(exp_search_be);
  decoded.multi = multi ? true : false;
  *opts = decoded;
  return 0;
}

static int dtlv_meta_put_init_options(MDB_txn *txn,
                                      MDB_dbi dbi,
                                      const usearch_init_options_t *opts) {
  if (!txn || !opts) return EINVAL;
  uint8_t buf[DTLV_INIT_RECORD_SIZE];
  dtlv_pack_init_record(opts, buf);
  return dtlv_meta_put_bytes(txn, dbi, DTLV_META_KEY_INIT, buf, sizeof(buf));
}

static int dtlv_meta_get_init_options(MDB_txn *txn,
                                      MDB_dbi dbi,
                                      usearch_init_options_t *opts,
                                      int *found) {
  if (found) *found = 0;
  if (!txn || !opts) return EINVAL;
  MDB_val key = dtlv_meta_key(DTLV_META_KEY_INIT);
  MDB_val val;
  int rc = mdb_get(txn, dbi, &key, &val);
  if (rc == MDB_NOTFOUND) return 0;
  if (rc != 0) return rc;
  rc = dtlv_unpack_init_record(&val, opts);
  if (rc != 0) return rc;
  if (found) *found = 1;
  return 0;
}

static void dtlv_pack_checkpoint_pending(const dtlv_checkpoint_pending_record *pending,
                                         uint8_t out[DTLV_CHECKPOINT_RECORD_SIZE]) {
  size_t offset = 0;
  memset(out, 0, DTLV_CHECKPOINT_RECORD_SIZE);
  out[offset++] = pending ? pending->version : 0;
  out[offset++] = pending ? pending->stage : 0;
  uint16_t reserved = dtlv_to_be16(pending ? pending->reserved : 0);
  memcpy(out + offset, &reserved, sizeof(reserved));
  offset += sizeof(reserved);
  uint32_t chunk = dtlv_to_be32(pending ? pending->chunk_cursor : 0);
  memcpy(out + offset, &chunk, sizeof(chunk));
  offset += sizeof(chunk);
  uint64_t snapshot_seq = dtlv_to_be64(pending ? pending->snapshot_seq : 0);
  memcpy(out + offset, &snapshot_seq, sizeof(snapshot_seq));
  offset += sizeof(snapshot_seq);
  uint64_t uuid_hi = dtlv_to_be64(pending ? pending->writer_uuid.hi : 0);
  memcpy(out + offset, &uuid_hi, sizeof(uuid_hi));
  offset += sizeof(uuid_hi);
  uint64_t uuid_lo = dtlv_to_be64(pending ? pending->writer_uuid.lo : 0);
  memcpy(out + offset, &uuid_lo, sizeof(uuid_lo));
}

static int dtlv_unpack_checkpoint_pending(const MDB_val *val,
                                          dtlv_checkpoint_pending_record *pending) {
  if (!val || !pending) return EINVAL;
  if (val->mv_size != DTLV_CHECKPOINT_RECORD_SIZE) return MDB_CORRUPTED;
  const uint8_t *src = (const uint8_t *)val->mv_data;
  pending->version = src[0];
  pending->stage = src[1];
  uint16_t reserved;
  memcpy(&reserved, src + 2, sizeof(reserved));
  pending->reserved = dtlv_from_be16(reserved);
  uint32_t chunk_be;
  memcpy(&chunk_be, src + 4, sizeof(chunk_be));
  pending->chunk_cursor = dtlv_from_be32(chunk_be);
  uint64_t seq_be;
  memcpy(&seq_be, src + 8, sizeof(seq_be));
  pending->snapshot_seq = dtlv_from_be64(seq_be);
  uint64_t uuid_hi;
  memcpy(&uuid_hi, src + 16, sizeof(uuid_hi));
  pending->writer_uuid.hi = dtlv_from_be64(uuid_hi);
  uint64_t uuid_lo;
  memcpy(&uuid_lo, src + 24, sizeof(uuid_lo));
  pending->writer_uuid.lo = dtlv_from_be64(uuid_lo);
  return 0;
}

static int dtlv_meta_put_checkpoint_pending(MDB_txn *txn,
                                            MDB_dbi dbi,
                                            const dtlv_checkpoint_pending_record *pending) {
  if (!txn || !pending) return EINVAL;
  uint8_t buf[DTLV_CHECKPOINT_RECORD_SIZE];
  dtlv_pack_checkpoint_pending(pending, buf);
  return dtlv_meta_put_bytes(txn, dbi, DTLV_META_KEY_CHECKPOINT_PENDING, buf, sizeof(buf));
}

static int dtlv_meta_get_checkpoint_pending(MDB_txn *txn,
                                            MDB_dbi dbi,
                                            dtlv_checkpoint_pending_record *pending,
                                            int *found) {
  if (found) *found = 0;
  if (!txn || !pending) return EINVAL;
  MDB_val key = dtlv_meta_key(DTLV_META_KEY_CHECKPOINT_PENDING);
  MDB_val val;
  int rc = mdb_get(txn, dbi, &key, &val);
  if (rc == MDB_NOTFOUND) return 0;
  if (rc != 0) return rc;
  rc = dtlv_unpack_checkpoint_pending(&val, pending);
  if (rc != 0) return rc;
  if (found) *found = 1;
  return 0;
}

static int dtlv_snapshot_store_chunk(dtlv_usearch_domain *domain,
                                     MDB_txn *txn,
                                     uint64_t snapshot_seq,
                                     uint32_t chunk_ordinal,
                                     const uint8_t *chunk_data,
                                     size_t chunk_len) {
  if (!domain || !txn) return EINVAL;
  if (chunk_len > UINT32_MAX) return EINVAL;
  uint8_t key_buf[12];
  uint64_t seq_be = dtlv_to_be64(snapshot_seq);
  memcpy(key_buf, &seq_be, sizeof(seq_be));
  uint32_t ordinal_be = dtlv_to_be32(chunk_ordinal);
  memcpy(key_buf + sizeof(seq_be), &ordinal_be, sizeof(ordinal_be));
  MDB_val key = {.mv_data = key_buf, .mv_size = sizeof(key_buf)};
  size_t value_len = DTLV_SNAPSHOT_CHUNK_HEADER_SIZE + chunk_len;
  MDB_val val = {.mv_size = value_len, .mv_data = NULL};
  int rc = mdb_put(txn, domain->snapshot_dbi, &key, &val, MDB_RESERVE);
  if (rc != 0) return rc;
  uint8_t *dst = (uint8_t *)val.mv_data;
  if (!dst && value_len > 0) return EIO;
  size_t offset = 0;
  dst[offset++] = (uint8_t)DTLV_USEARCH_SNAPSHOT_CHUNK_VERSION;
  dst[offset++] = 0;
  uint16_t header_len_be = dtlv_to_be16((uint16_t)DTLV_SNAPSHOT_CHUNK_HEADER_SIZE);
  memcpy(dst + offset, &header_len_be, sizeof(header_len_be));
  offset += sizeof(header_len_be);
  uint32_t chunk_len_be = dtlv_to_be32((uint32_t)chunk_len);
  memcpy(dst + offset, &chunk_len_be, sizeof(chunk_len_be));
  offset += sizeof(chunk_len_be);
  uint32_t crc = dtlv_crc32c(chunk_data, chunk_len);
  if (dtlv_env_flag_enabled("DTLV_FAULT_SNAPSHOT_CRC")) crc ^= 0xa5a5a5a5u;
  uint32_t crc_be = dtlv_to_be32(crc);
  memcpy(dst + offset, &crc_be, sizeof(crc_be));
  offset += sizeof(crc_be);
  if (chunk_len && chunk_data) memcpy(dst + offset, chunk_data, chunk_len);
  return 0;
}

static int dtlv_ensure_defaults(MDB_txn *txn, MDB_dbi meta_dbi) {
  uint64_t value;
  int rc = dtlv_meta_get_u64(txn, meta_dbi, DTLV_META_KEY_LOG_SEQ, 0, &value);
  if (rc != 0) return rc;
  rc = dtlv_meta_put_u64(txn, meta_dbi, DTLV_META_KEY_LOG_SEQ, value);
  if (rc != 0) return rc;
  rc = dtlv_meta_get_u64(txn, meta_dbi, DTLV_META_KEY_SNAPSHOT_SEQ, 0, &value);
  if (rc != 0) return rc;
  return dtlv_meta_put_u64(txn, meta_dbi, DTLV_META_KEY_SNAPSHOT_SEQ, value);
}

static int dtlv_build_dbi_name(const char *domain, const char *suffix, char *out, size_t out_len) {
  int written = snprintf(out, out_len, "%s/%s", domain, suffix);
  if (written < 0 || (size_t)written >= out_len) return ENAMETOOLONG;
  return 0;
}

int dtlv_usearch_domain_open(MDB_env *env,
                             const char *domain_name,
                             const char *filesystem_root,
                             dtlv_usearch_domain **domain_out) {
  if (!env || !domain_name || !filesystem_root || !domain_out) return EINVAL;
  dtlv_usearch_domain *domain = calloc(1, sizeof(*domain));
  if (!domain) return ENOMEM;
  domain->env = env;
  domain->pin_ttl_ms = DTLV_READER_PIN_DEFAULT_TTL_MS;
  domain->pin_heartbeat_ms = DTLV_READER_PIN_DEFAULT_HEARTBEAT_MS;
#ifndef _WIN32
  domain->pins_fd = -1;
#else
  domain->pins_file = NULL;
  domain->pins_mapping = NULL;
#endif
  if (strlen(domain_name) >= sizeof(domain->domain_name)) {
    free(domain);
    return ENAMETOOLONG;
  }
  strcpy(domain->domain_name, domain_name);
  if (strlen(filesystem_root) >= sizeof(domain->filesystem_root)) {
    free(domain);
    return ENAMETOOLONG;
  }
  strcpy(domain->filesystem_root, filesystem_root);
  if (snprintf(domain->pending_root, sizeof(domain->pending_root), "%s/pending", filesystem_root) >= (int)sizeof(domain->pending_root)) {
    free(domain);
    return ENAMETOOLONG;
  }
  int rc = dtlv_reader_pins_open(domain);
  if (rc != 0) {
    free(domain);
    return rc;
  }
  rc = dtlv_build_dbi_name(domain_name, "usearch-meta", domain->meta_name, sizeof(domain->meta_name));
  if (rc != 0) {
    dtlv_reader_pins_close(domain);
    free(domain);
    return rc;
  }
  rc = dtlv_build_dbi_name(domain_name, "usearch-delta", domain->delta_name, sizeof(domain->delta_name));
  if (rc != 0) {
    dtlv_reader_pins_close(domain);
    free(domain);
    return rc;
  }
  rc = dtlv_build_dbi_name(domain_name, "usearch-snapshot", domain->snapshot_name, sizeof(domain->snapshot_name));
  if (rc != 0) {
    dtlv_reader_pins_close(domain);
    free(domain);
    return rc;
  }

  MDB_txn *txn = NULL;
  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc != 0) {
    dtlv_reader_pins_close(domain);
    free(domain);
    return rc;
  }
  rc = mdb_dbi_open(txn, domain->meta_name, MDB_CREATE, &domain->meta_dbi);
  if (rc == 0) rc = mdb_dbi_open(txn, domain->delta_name, MDB_CREATE, &domain->delta_dbi);
  if (rc == 0) rc = mdb_dbi_open(txn, domain->snapshot_name, MDB_CREATE, &domain->snapshot_dbi);
  if (rc == 0) rc = dtlv_ensure_defaults(txn, domain->meta_dbi);
  uint32_t chunk_bytes = DTLV_USEARCH_DEFAULT_CHUNK_BYTES;
  if (rc == 0) rc = dtlv_meta_get_u32(txn,
                                      domain->meta_dbi,
                                      DTLV_META_KEY_CHUNK_BYTES,
                                      DTLV_USEARCH_DEFAULT_CHUNK_BYTES,
                                      &chunk_bytes);
  if (chunk_bytes == 0) chunk_bytes = DTLV_USEARCH_DEFAULT_CHUNK_BYTES;
  if (rc == 0) rc = dtlv_meta_put_u32(txn, domain->meta_dbi, DTLV_META_KEY_CHUNK_BYTES, chunk_bytes);
  uint32_t checkpoint_chunk_batch = DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  if (rc == 0) rc = dtlv_meta_get_u32(txn,
                                      domain->meta_dbi,
                                      DTLV_META_KEY_CHECKPOINT_CHUNK_BATCH,
                                      DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH,
                                      &checkpoint_chunk_batch);
  if (checkpoint_chunk_batch == 0) checkpoint_chunk_batch = DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  if (rc == 0) {
    rc = dtlv_meta_put_u32(txn,
                           domain->meta_dbi,
                           DTLV_META_KEY_CHECKPOINT_CHUNK_BATCH,
                           checkpoint_chunk_batch);
  }
  uint32_t retention_count = DTLV_USEARCH_DEFAULT_SNAPSHOT_RETENTION;
  if (rc == 0) rc = dtlv_meta_get_u32(txn,
                                      domain->meta_dbi,
                                      DTLV_META_KEY_SNAPSHOT_RETENTION_COUNT,
                                      DTLV_USEARCH_DEFAULT_SNAPSHOT_RETENTION,
                                      &retention_count);
  if (retention_count == 0) retention_count = DTLV_USEARCH_DEFAULT_SNAPSHOT_RETENTION;
  if (rc == 0) rc = dtlv_meta_put_u32(txn, domain->meta_dbi, DTLV_META_KEY_SNAPSHOT_RETENTION_COUNT, retention_count);
  uint64_t retained_floor = 0;
  if (rc == 0) rc = dtlv_meta_get_u64(txn,
                                      domain->meta_dbi,
                                      DTLV_META_KEY_SNAPSHOT_RETAINED_FLOOR,
                                      0,
                                      &retained_floor);
  if (rc == 0) rc = dtlv_meta_put_u64(txn, domain->meta_dbi, DTLV_META_KEY_SCHEMA_VERSION, DTLV_USEARCH_SCHEMA_VERSION);
  if (rc == 0) rc = mdb_txn_commit(txn);
  else mdb_txn_abort(txn);
  if (rc != 0) {
    free(domain);
    return rc;
  }
  domain->chunk_bytes = chunk_bytes;
  domain->checkpoint_chunk_batch = checkpoint_chunk_batch;
  domain->snapshot_retention_count = retention_count;
  rc = dtlv_usearch_checkpoint_recover(domain);
  if (rc != 0) {
    dtlv_usearch_domain_close(domain);
    return rc;
  }
  rc = dtlv_usearch_wal_recover(domain);
  if (rc != 0) {
    dtlv_usearch_domain_close(domain);
    return rc;
  }
  *domain_out = domain;
  return 0;
}

void dtlv_usearch_domain_close(dtlv_usearch_domain *domain) {
  if (!domain) return;
  dtlv_usearch_handle *handle = domain->handles_head;
  while (handle) {
    dtlv_usearch_handle *next = handle->next;
    dtlv_usearch_deactivate(handle);  // Drop handles before DBIs/env vanish.
    handle = next;
  }
  if (domain->env) {
    mdb_dbi_close(domain->env, domain->meta_dbi);
    mdb_dbi_close(domain->env, domain->delta_dbi);
    mdb_dbi_close(domain->env, domain->snapshot_dbi);
  }
  dtlv_reader_pins_close(domain);
  free(domain);
}

int dtlv_usearch_activate(dtlv_usearch_domain *domain, dtlv_usearch_handle **handle_out) {
  if (!domain || !handle_out) return EINVAL;
  *handle_out = NULL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  uint64_t schema_version = 0;
  rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_SCHEMA_VERSION, 0, &schema_version);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  if (schema_version != DTLV_USEARCH_SCHEMA_VERSION) {
    mdb_txn_abort(txn);
    return EINVAL;
  }
  usearch_init_options_t init_opts;
  int init_found = 0;
  rc = dtlv_meta_get_init_options(txn, domain->meta_dbi, &init_opts, &init_found);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  if (!init_found) {
    mdb_txn_abort(txn);
    return ENOENT;
  }
  uint64_t snapshot_seq = 0;
  rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_SNAPSHOT_SEQ, 0, &snapshot_seq);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  uint64_t log_seq = 0;
  rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_LOG_SEQ, 0, &log_seq);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  uint64_t reserve_hint = dtlv_usearch_reserve_hint(snapshot_seq, log_seq);
  usearch_index_t index = NULL;
  rc = dtlv_usearch_build_index(domain, txn, &init_opts, snapshot_seq, log_seq, reserve_hint, &index);
  mdb_txn_abort(txn);
  if (rc != 0) return rc;
  dtlv_usearch_handle *handle = calloc(1, sizeof(*handle));
  if (!handle) {
    if (index) {
      usearch_error_t free_err = NULL;
      usearch_free(index, &free_err);
    }
    return ENOMEM;
  }
  handle->domain = domain;
  handle->index = index;
  handle->scalar_kind = init_opts.quantization;
  handle->snapshot_seq = snapshot_seq;
  handle->log_seq = log_seq;
  dtlv_domain_register_handle(domain, handle);
  *handle_out = handle;
  return 0;
}

int dtlv_usearch_refresh(dtlv_usearch_handle *handle, MDB_txn *txn) {
  if (!handle || !txn) return EINVAL;
  if (!handle->domain || !handle->index) return EINVAL;
  dtlv_usearch_domain *domain = handle->domain;
  uint64_t snapshot_seq = 0;
  int rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_SNAPSHOT_SEQ, 0, &snapshot_seq);
  if (rc != 0) return rc;
  uint64_t log_seq = 0;
  rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_LOG_SEQ, 0, &log_seq);
  if (rc != 0) return rc;
  int needs_rebuild = snapshot_seq > handle->snapshot_seq || log_seq < handle->log_seq;
  if (!needs_rebuild && log_seq == handle->log_seq) return 0;
  if (needs_rebuild) {
    usearch_init_options_t init_opts;
    int init_found = 0;
    rc = dtlv_meta_get_init_options(txn, domain->meta_dbi, &init_opts, &init_found);
    if (rc != 0) return rc;
    if (!init_found) return ENOENT;
    uint64_t reserve_hint = dtlv_usearch_reserve_hint(snapshot_seq, log_seq);
    usearch_index_t new_index = NULL;
    rc = dtlv_usearch_build_index(domain, txn, &init_opts, snapshot_seq, log_seq, reserve_hint, &new_index);
    if (rc != 0) return rc;
    usearch_index_t old_index = handle->index;
    handle->index = new_index;
    handle->scalar_kind = init_opts.quantization;
    handle->snapshot_seq = snapshot_seq;
    handle->log_seq = log_seq;
    if (old_index) {
      usearch_error_t err = NULL;
      usearch_free(old_index, &err);
    }
    return 0;
  }
  if (log_seq > handle->log_seq) {
    rc = dtlv_replay_deltas(domain, txn, handle->index, handle->scalar_kind, handle->log_seq, log_seq);
    if (rc != 0) return rc;
    handle->log_seq = log_seq;
  }
  return 0;
}

int dtlv_usearch_pin_handle(dtlv_usearch_domain *domain,
                            const dtlv_uuid128 *reader_uuid,
                            uint64_t snapshot_seq,
                            uint64_t log_seq,
                            int64_t expires_at_ms) {
  if (!domain || !reader_uuid) return EINVAL;
  if (!domain->pins_map) return ENOSYS;
  int rc = dtlv_reader_pins_lock(domain);
  if (rc != 0) return rc;
  size_t found_slot = (size_t)-1;
  size_t empty_slot = (size_t)-1;
  size_t expired_slot = (size_t)-1;
  int64_t now_ms = dtlv_now_ms();
  for (size_t i = 0; i < domain->pins_slot_count; ++i) {
    uint8_t *slot = dtlv_pin_slot_ptr(domain, i);
    if (!slot) break;
    dtlv_reader_pin_record rec;
    int decode = dtlv_pin_unpack_record(slot, &rec);
    if (decode == ENOENT) {
      if (empty_slot == (size_t)-1) empty_slot = i;
      continue;
    }
    if (decode != 0) {
      memset(slot, 0, DTLV_READER_PIN_RECORD_SIZE);
      if (empty_slot == (size_t)-1) empty_slot = i;
      continue;
    }
    if (rec.reader_uuid.hi == reader_uuid->hi && rec.reader_uuid.lo == reader_uuid->lo) {
      found_slot = i;
      break;
    }
    if (rec.expires_at_ms <= now_ms && expired_slot == (size_t)-1) expired_slot = i;
  }
  size_t target = found_slot != (size_t)-1 ? found_slot : (empty_slot != (size_t)-1 ? empty_slot : expired_slot);
  if (target == (size_t)-1) {
    dtlv_reader_pins_unlock(domain);
    return ENOSPC;
  }
  dtlv_pin_write_slot(dtlv_pin_slot_ptr(domain, target), reader_uuid, snapshot_seq, log_seq, expires_at_ms);
  dtlv_reader_pins_unlock(domain);
  return 0;
}

int dtlv_usearch_touch_pin(dtlv_usearch_domain *domain,
                           const dtlv_uuid128 *reader_uuid,
                           int64_t expires_at_ms) {
  if (!domain || !reader_uuid) return EINVAL;
  if (!domain->pins_map) return ENOSYS;
  int rc = dtlv_reader_pins_lock(domain);
  if (rc != 0) return rc;
  size_t found_slot = (size_t)-1;
  dtlv_reader_pin_record rec;
  for (size_t i = 0; i < domain->pins_slot_count; ++i) {
    uint8_t *slot = dtlv_pin_slot_ptr(domain, i);
    if (!slot) break;
    int decode = dtlv_pin_unpack_record(slot, &rec);
    if (decode == ENOENT) continue;
    if (decode != 0) {
      memset(slot, 0, DTLV_READER_PIN_RECORD_SIZE);
      continue;
    }
    if (rec.reader_uuid.hi == reader_uuid->hi && rec.reader_uuid.lo == reader_uuid->lo) {
      found_slot = i;
      break;
    }
  }
  if (found_slot == (size_t)-1) {
    dtlv_reader_pins_unlock(domain);
    return ENOENT;
  }
  rec.expires_at_ms = expires_at_ms;
  dtlv_pin_write_slot(dtlv_pin_slot_ptr(domain, found_slot),
                      &rec.reader_uuid,
                      rec.snapshot_seq,
                      rec.log_seq,
                      rec.expires_at_ms);
  dtlv_reader_pins_unlock(domain);
  return 0;
}

int dtlv_usearch_release_pin(dtlv_usearch_domain *domain,
                             const dtlv_uuid128 *reader_uuid) {
  if (!domain || !reader_uuid) return EINVAL;
  if (!domain->pins_map) return ENOSYS;
  int rc = dtlv_reader_pins_lock(domain);
  if (rc != 0) return rc;
  int found = 0;
  for (size_t i = 0; i < domain->pins_slot_count; ++i) {
    uint8_t *slot = dtlv_pin_slot_ptr(domain, i);
    if (!slot) break;
    dtlv_reader_pin_record rec;
    int decode = dtlv_pin_unpack_record(slot, &rec);
    if (decode == ENOENT) continue;
    if (decode != 0) {
      memset(slot, 0, DTLV_READER_PIN_RECORD_SIZE);
      continue;
    }
    if (rec.reader_uuid.hi == reader_uuid->hi && rec.reader_uuid.lo == reader_uuid->lo) {
      memset(slot, 0, DTLV_READER_PIN_RECORD_SIZE);
      found = 1;
      break;
    }
  }
  dtlv_reader_pins_unlock(domain);
  (void)found;
  return 0;
}

static int dtlv_hex_value(char c, uint8_t *out) {
  if (c >= '0' && c <= '9') {
    *out = (uint8_t)(c - '0');
    return 0;
  }
  if (c >= 'a' && c <= 'f') {
    *out = (uint8_t)(10 + (c - 'a'));
    return 0;
  }
  if (c >= 'A' && c <= 'F') {
    *out = (uint8_t)(10 + (c - 'A'));
    return 0;
  }
  return EINVAL;
}

static int dtlv_parse_token_hex(const char *hex, dtlv_uuid128 *token) {
  if (!hex || !token) return EINVAL;
  uint64_t hi = 0;
  uint64_t lo = 0;
  for (size_t i = 0; i < DTLV_ULOG_TOKEN_HEX; ++i) {
    uint8_t nib = 0;
    if (dtlv_hex_value(hex[i], &nib) != 0) return EINVAL;
    if (i < 16) hi = (hi << 4) | nib;
    else lo = (lo << 4) | nib;
  }
  token->hi = hi;
  token->lo = lo;
  return 0;
}

static int dtlv_pending_name_parse(const char *name,
                                   const char *suffix,
                                   dtlv_uuid128 *token) {
  size_t suffix_len = strlen(suffix);
  size_t name_len = strlen(name);
  if (name_len != DTLV_ULOG_TOKEN_HEX + suffix_len) return 0;
  if (memcmp(name + DTLV_ULOG_TOKEN_HEX, suffix, suffix_len) != 0) return 0;
  if (!token) return 1;
  return dtlv_parse_token_hex(name, token) == 0 ? 1 : -EINVAL;
}

static int dtlv_delete_file_best_effort(const char *path) {
  if (!path || !*path) return 0;
  if (remove(path) != 0) {
    if (errno == ENOENT) return 0;
    return errno ? errno : EIO;
  }
  return 0;
}

static int dtlv_is_regular_file(const char *path) {
  if (!path || !*path) return 0;
#ifdef _WIN32
  DWORD attrs = GetFileAttributesA(path);
  if (attrs == INVALID_FILE_ATTRIBUTES) return 0;
  return (attrs & FILE_ATTRIBUTE_DIRECTORY) == 0;
#else
  struct stat st;
  if (stat(path, &st) != 0) return 0;
  return S_ISREG(st.st_mode);
#endif
}

#ifdef _WIN32
static void dtlv_fix_windows_path(char *path) {
  if (!path) return;
  for (char *p = path; *p; ++p) {
    if (*p == '/') *p = '\\';
  }
}
#endif

typedef struct {
  dtlv_usearch_domain *domain;
  int sealed_found;
  dtlv_uuid128 sealed_token;
  int ready_found;
  char ready_path[PATH_MAX];
} dtlv_pending_scan_ctx;

static int dtlv_pending_entry_callback(const char *dir,
                                       const char *name,
                                       void *ctx_void) {
  dtlv_pending_scan_ctx *ctx = (dtlv_pending_scan_ctx *)ctx_void;
  if (!dir || !name || !ctx) return EINVAL;
  char full_path[PATH_MAX];
  if (snprintf(full_path, sizeof(full_path), "%s/%s", dir, name) >= (int)sizeof(full_path)) {
    return ENAMETOOLONG;
  }
  if (!dtlv_is_regular_file(full_path)) return 0;
  if (dtlv_pending_name_parse(name, ".ulog.open", NULL) > 0) {
    return dtlv_delete_file_best_effort(full_path);
  }
  dtlv_uuid128 token;
  int match = dtlv_pending_name_parse(name, ".ulog.sealed", &token);
  if (match < 0) {
    return dtlv_delete_file_best_effort(full_path);
  }
  if (match > 0) {
    if (!ctx->sealed_found ||
        token.hi != ctx->sealed_token.hi ||
        token.lo != ctx->sealed_token.lo) {
      return dtlv_delete_file_best_effort(full_path);
    }
    strncpy(ctx->ready_path, full_path, sizeof(ctx->ready_path) - 1);
    ctx->ready_path[sizeof(ctx->ready_path) - 1] = '\0';
    ctx->ready_found = 1;
    return 0;
  }
  match = dtlv_pending_name_parse(name, ".ulog", &token);
  if (match < 0) {
    return dtlv_delete_file_best_effort(full_path);
  }
  if (match > 0) {
    if (!ctx->sealed_found ||
        token.hi != ctx->sealed_token.hi ||
        token.lo != ctx->sealed_token.lo) {
      return dtlv_delete_file_best_effort(full_path);
    }
    char target_path[PATH_MAX];
    if (snprintf(target_path,
                 sizeof(target_path),
                 "%s/%.*s.ulog.sealed",
                 dir,
                 DTLV_ULOG_TOKEN_HEX,
                 name) >= (int)sizeof(target_path)) {
      return ENAMETOOLONG;
    }
    int rc = dtlv_delete_file_best_effort(target_path);
    if (rc != 0) return rc;
    if (rename(full_path, target_path) != 0) return errno ? errno : EIO;
    strncpy(ctx->ready_path, target_path, sizeof(ctx->ready_path) - 1);
    ctx->ready_path[sizeof(ctx->ready_path) - 1] = '\0';
    ctx->ready_found = 1;
    return 0;
  }
  return dtlv_delete_file_best_effort(full_path);
}

static int dtlv_pending_dir_scan(dtlv_usearch_domain *domain,
                                 int (*cb)(const char *dir, const char *name, void *ctx),
                                 void *ctx) {
  if (!domain || !cb) return EINVAL;
#ifdef _WIN32
  char pattern[PATH_MAX];
  if (snprintf(pattern, sizeof(pattern), "%s/*", domain->pending_root) >= (int)sizeof(pattern)) {
    return ENAMETOOLONG;
  }
  dtlv_fix_windows_path(pattern);
  WIN32_FIND_DATAA data;
  HANDLE handle = FindFirstFileA(pattern, &data);
  if (handle == INVALID_HANDLE_VALUE) {
    DWORD err = GetLastError();
    if (err == ERROR_FILE_NOT_FOUND || err == ERROR_PATH_NOT_FOUND) return 0;
    return (int)err;
  }
  int rc = 0;
  do {
    if (!strcmp(data.cFileName, ".") || !strcmp(data.cFileName, "..")) continue;
    rc = cb(domain->pending_root, data.cFileName, ctx);
    if (rc != 0) break;
  } while (FindNextFileA(handle, &data));
  FindClose(handle);
  return rc;
#else
  DIR *dir = opendir(domain->pending_root);
  if (!dir) {
    if (errno == ENOENT) return 0;
    return errno;
  }
  int rc = 0;
  struct dirent *entry = NULL;
  while ((entry = readdir(dir)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
    rc = cb(domain->pending_root, entry->d_name, ctx);
    if (rc != 0) break;
  }
  closedir(dir);
  return rc;
#endif
}

static int dtlv_usearch_wal_recover(dtlv_usearch_domain *domain) {
  if (!domain || !domain->env) return EINVAL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  dtlv_pending_scan_ctx ctx;
  memset(&ctx, 0, sizeof(ctx));
  ctx.domain = domain;
  rc = dtlv_meta_get_sealed_log_seq(txn, domain->meta_dbi, &ctx.sealed_token, NULL, &ctx.sealed_found);
  mdb_txn_abort(txn);
  if (rc != 0) return rc;
  rc = dtlv_pending_dir_scan(domain, dtlv_pending_entry_callback, &ctx);
  if (rc != 0) return rc;
  if (!ctx.ready_found || !ctx.sealed_found) return 0;
  dtlv_uuid128 tail_token;
  uint32_t tail_ordinal = 0;
  int tail_found = 0;
  rc = dtlv_usearch_read_published_tail(domain, &tail_token, &tail_ordinal, &tail_found);
  if (rc != 0) return rc;
  uint32_t start = 1;
  if (tail_found &&
      tail_token.hi == ctx.sealed_token.hi &&
      tail_token.lo == ctx.sealed_token.lo) {
    if (tail_ordinal == UINT32_MAX) return EOVERFLOW;
    start = tail_ordinal + 1;
  }
  return dtlv_usearch_replay_wal_file(domain, ctx.ready_path, &ctx.sealed_token, start, 1);
}

void dtlv_usearch_deactivate(dtlv_usearch_handle *handle) {
  if (!handle) return;
  if (handle->domain) dtlv_domain_unregister_handle(handle->domain, handle);
  if (handle->index) {
    usearch_error_t err = NULL;
    usearch_free(handle->index, &err);
  }
  free(handle);
}

usearch_index_t dtlv_usearch_handle_index(const dtlv_usearch_handle *handle) {
  return handle ? handle->index : NULL;
}

int dtlv_usearch_store_init_options(dtlv_usearch_domain *domain,
                                    MDB_txn *txn,
                                    const usearch_init_options_t *opts) {
  if (!domain || !txn || !opts) return EINVAL;
  return dtlv_meta_put_init_options(txn, domain->meta_dbi, opts);
}

int dtlv_usearch_load_init_options(dtlv_usearch_domain *domain,
                                   MDB_txn *txn,
                                   usearch_init_options_t *opts,
                                   int *found) {
  if (!domain || !txn || !opts) return EINVAL;
  return dtlv_meta_get_init_options(txn, domain->meta_dbi, opts, found);
}

// Serializes the provided USearch index into chunked LMDB snapshot entries.
// Each chunk is written via MDB_RESERVE and checkpoint metadata is updated
// after every successful write so recovery can resume mid-stream.
int dtlv_usearch_checkpoint_write_snapshot(dtlv_usearch_domain *domain,
                                           usearch_index_t index,
                                           uint64_t snapshot_seq,
                                           const dtlv_uuid128 *writer_uuid,
                                           size_t *chunk_count_out) {
  if (!domain || !domain->env || !index) return EINVAL;
  dtlv_checkpoint_pending_record existing_pending;
  int pending_found = 0;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  rc = dtlv_meta_get_checkpoint_pending(txn, domain->meta_dbi, &existing_pending, &pending_found);
  mdb_txn_abort(txn);
  if (rc != 0) return rc;
  if (pending_found && existing_pending.stage != DTLV_CHECKPOINT_STAGE_NONE) return EBUSY;
  usearch_error_t err = NULL;
  size_t serialized_len = usearch_serialized_length(index, &err);
  rc = dtlv_usearch_error_status(err);
  if (rc != 0) return rc;
  size_t buffer_len = serialized_len;
  if (buffer_len == 0) buffer_len = 1;
  uint8_t *snapshot = (uint8_t *)malloc(buffer_len);
  if (!snapshot) return ENOMEM;
  err = NULL;
  usearch_save_buffer(index, snapshot, serialized_len, &err);
  rc = dtlv_usearch_error_status(err);
  if (rc != 0) {
    free(snapshot);
    return rc;
  }
  dtlv_checkpoint_pending_record pending;
  memset(&pending, 0, sizeof(pending));
  pending.version = (uint8_t)DTLV_USEARCH_CHECKPOINT_VERSION;
  pending.stage = (uint8_t)DTLV_CHECKPOINT_STAGE_INIT;
  pending.snapshot_seq = snapshot_seq;
  pending.chunk_cursor = 0;
  pending.writer_uuid = writer_uuid ? *writer_uuid : (dtlv_uuid128){0};
  int checkpoint_started = 0;
  rc = mdb_txn_begin(domain->env, NULL, 0, &txn);
  if (rc == 0) rc = dtlv_meta_put_checkpoint_pending(txn, domain->meta_dbi, &pending);
  if (rc == 0) rc = mdb_txn_commit(txn);
  else if (txn) mdb_txn_abort(txn);
  if (rc != 0) {
    free(snapshot);
    return rc;
  }
  checkpoint_started = 1;
  size_t chunk_limit = domain->chunk_bytes ? domain->chunk_bytes : DTLV_USEARCH_DEFAULT_CHUNK_BYTES;
  size_t chunks_written = 0;
  size_t offset = 0;
  size_t batch_limit = domain->checkpoint_chunk_batch ? (size_t)domain->checkpoint_chunk_batch
                                                      : (size_t)DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  if (batch_limit == 0) batch_limit = DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  while (offset < serialized_len) {
    txn = NULL;
    rc = mdb_txn_begin(domain->env, NULL, 0, &txn);
    if (rc != 0) break;
    size_t batch_written = 0;
    while (offset < serialized_len && batch_written < batch_limit) {
      size_t remaining = serialized_len - offset;
      size_t chunk_len = remaining < chunk_limit ? remaining : chunk_limit;
      if (chunks_written >= UINT32_MAX) {
        rc = EOVERFLOW;
        break;
      }
      rc = dtlv_snapshot_store_chunk(domain,
                                     txn,
                                     snapshot_seq,
                                     (uint32_t)chunks_written,
                                     snapshot + offset,
                                     chunk_len);
      if (rc != 0) break;
      offset += chunk_len;
      chunks_written++;
      batch_written++;
    }
    if (rc == 0) {
      pending.stage = (uint8_t)DTLV_CHECKPOINT_STAGE_WRITING;
      pending.chunk_cursor = (uint32_t)chunks_written;
      rc = dtlv_meta_put_checkpoint_pending(txn, domain->meta_dbi, &pending);
    }
    if (rc == 0) rc = mdb_txn_commit(txn);
    else if (txn) mdb_txn_abort(txn);
    if (rc != 0) break;
  }
  if (rc == 0 && serialized_len == 0) {
    txn = NULL;
    rc = mdb_txn_begin(domain->env, NULL, 0, &txn);
    if (rc == 0) {
      pending.stage = (uint8_t)DTLV_CHECKPOINT_STAGE_WRITING;
      pending.chunk_cursor = 0;
      rc = dtlv_meta_put_checkpoint_pending(txn, domain->meta_dbi, &pending);
    }
    if (rc == 0) rc = mdb_txn_commit(txn);
    else if (txn) mdb_txn_abort(txn);
  }
  if (rc == MDB_MAP_FULL && checkpoint_started && dtlv_trace_enabled()) {
    dtlv_tracef("[checkpoint] map full after %zu chunks (seq=%llu)\n",
                chunks_written,
                (unsigned long long)snapshot_seq);
  }
  if (rc != 0 && rc != MDB_MAP_FULL && checkpoint_started) {
    MDB_txn *cleanup_txn = NULL;
    if (mdb_txn_begin(domain->env, NULL, 0, &cleanup_txn) == 0) {
      dtlv_meta_delete(cleanup_txn, domain->meta_dbi, DTLV_META_KEY_CHECKPOINT_PENDING);
      mdb_txn_commit(cleanup_txn);
    }
  }
  free(snapshot);
  if (chunk_count_out) *chunk_count_out = chunks_written;
  return rc;
}

int dtlv_usearch_checkpoint_finalize(dtlv_usearch_domain *domain,
                                     uint64_t snapshot_seq,
                                     uint64_t prune_log_seq) {
  if (!domain || !domain->env) return EINVAL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, 0, &txn);
  if (rc != 0) return rc;
  dtlv_checkpoint_pending_record pending;
  int pending_found = 0;
  rc = dtlv_meta_get_checkpoint_pending(txn, domain->meta_dbi, &pending, &pending_found);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  if (!pending_found || pending.snapshot_seq != snapshot_seq) {
    mdb_txn_abort(txn);
    return ENOENT;
  }
  if (pending.stage < DTLV_CHECKPOINT_STAGE_WRITING) {
    mdb_txn_abort(txn);
    return EBUSY;
  }
  pending.stage = (uint8_t)DTLV_CHECKPOINT_STAGE_FINALIZING;
  rc = dtlv_meta_put_checkpoint_pending(txn, domain->meta_dbi, &pending);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = dtlv_meta_put_u64(txn, domain->meta_dbi, DTLV_META_KEY_SNAPSHOT_SEQ, snapshot_seq);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = dtlv_meta_put_u64(txn, domain->meta_dbi, DTLV_META_KEY_LOG_SEQ, snapshot_seq);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = dtlv_delta_delete_upto(domain, txn, prune_log_seq);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = dtlv_meta_put_u64(txn, domain->meta_dbi, DTLV_META_KEY_LOG_TAIL_SEQ, prune_log_seq);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  uint32_t retention = domain->snapshot_retention_count;
  if (retention == 0) retention = DTLV_USEARCH_DEFAULT_SNAPSHOT_RETENTION;
  uint64_t retained_floor = 0;
  if (retention > 0 && snapshot_seq + 1 > retention) {
    retained_floor = snapshot_seq + 1 - retention;
  }
  if (retained_floor > 0) {
    rc = dtlv_snapshot_delete_before(domain, txn, retained_floor);
    if (rc != 0) {
      mdb_txn_abort(txn);
      return rc;
    }
  }
  rc = dtlv_meta_put_u64(txn, domain->meta_dbi, DTLV_META_KEY_SNAPSHOT_RETAINED_FLOOR, retained_floor);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = dtlv_meta_delete(txn, domain->meta_dbi, DTLV_META_KEY_CHECKPOINT_PENDING);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  rc = mdb_txn_commit(txn);
  return rc;
}

int dtlv_usearch_checkpoint_recover(dtlv_usearch_domain *domain) {
  if (!domain || !domain->env) return EINVAL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  dtlv_checkpoint_pending_record pending;
  int pending_found = 0;
  rc = dtlv_meta_get_checkpoint_pending(txn, domain->meta_dbi, &pending, &pending_found);
  mdb_txn_abort(txn);
  if (rc != 0 || !pending_found) return rc;
  switch (pending.stage) {
    case DTLV_CHECKPOINT_STAGE_INIT:
    case DTLV_CHECKPOINT_STAGE_WRITING: {
      MDB_txn *cleanup_txn = NULL;
      rc = mdb_txn_begin(domain->env, NULL, 0, &cleanup_txn);
      if (rc != 0) return rc;
      rc = dtlv_snapshot_delete_chunks_from(domain, cleanup_txn, pending.snapshot_seq, 0);
      if (rc == 0) rc = dtlv_meta_delete(cleanup_txn, domain->meta_dbi, DTLV_META_KEY_CHECKPOINT_PENDING);
      if (rc == 0) rc = mdb_txn_commit(cleanup_txn);
      else if (cleanup_txn) mdb_txn_abort(cleanup_txn);
      return rc;
    }
    case DTLV_CHECKPOINT_STAGE_FINALIZING:
      return dtlv_usearch_checkpoint_finalize(domain, pending.snapshot_seq, pending.snapshot_seq);
    default: {
      MDB_txn *cleanup_txn = NULL;
      rc = mdb_txn_begin(domain->env, NULL, 0, &cleanup_txn);
      if (rc != 0) return rc;
      rc = dtlv_meta_delete(cleanup_txn, domain->meta_dbi, DTLV_META_KEY_CHECKPOINT_PENDING);
      if (rc == 0) rc = mdb_txn_commit(cleanup_txn);
      else if (cleanup_txn) mdb_txn_abort(cleanup_txn);
      return rc;
    }
  }
}


static int dtlv_encode_delta_payload(const dtlv_usearch_txn_ctx *ctx,
                                     const dtlv_usearch_update *update,
                                     uint32_t ordinal,
                                     uint8_t **out_buf,
                                     size_t *out_len) {
  if (!ctx || !update || !out_buf || !out_len) return EINVAL;
  if (update->key_len > 255) return EINVAL;
  if (update->payload_len > UINT32_MAX) return EINVAL;
  const dtlv_uuid128 *token = dtlv_usearch_wal_token(ctx->wal);
  if (!token) return EINVAL;
  size_t payload_len = update->payload && update->payload_len ? update->payload_len : 0;
  size_t header_len = DTLV_DELTA_HEADER_LEN;
  size_t total_len = header_len + update->key_len + payload_len;
  uint8_t *buffer = malloc(total_len ? total_len : header_len);
  if (!buffer) return ENOMEM;
  memset(buffer, 0, header_len);
  buffer[0] = 1;
  buffer[1] = (uint8_t)update->op;
  buffer[2] = (uint8_t)update->key_len;
  buffer[3] = 0;
  uint32_t be32 = dtlv_to_be32(ordinal);
  memcpy(buffer + 4, &be32, sizeof(be32));
  uint64_t behi = dtlv_to_be64(token->hi);
  memcpy(buffer + 8, &behi, sizeof(behi));
  uint64_t belo = dtlv_to_be64(token->lo);
  memcpy(buffer + 16, &belo, sizeof(belo));
  uint32_t payload_len_be = dtlv_to_be32((uint32_t)payload_len);
  memcpy(buffer + 24, &payload_len_be, sizeof(payload_len_be));
  if (update->key_len && update->key) {
    memcpy(buffer + header_len, update->key, update->key_len);
  }
  if (payload_len && update->payload) {
    memcpy(buffer + header_len + update->key_len, update->payload, payload_len);
  }
  uint32_t crc = dtlv_crc32c(buffer + header_len, update->key_len + payload_len);
  uint32_t crc_be = dtlv_to_be32(crc);
  memcpy(buffer + 28, &crc_be, sizeof(crc_be));
  *out_buf = buffer;
  *out_len = header_len + update->key_len + payload_len;
  return 0;
}

static int dtlv_append_delta(dtlv_usearch_txn_ctx *ctx,
                             uint64_t log_seq,
                             const uint8_t *payload,
                             size_t payload_len) {
  uint64_t key_be = dtlv_to_be64(log_seq);
  MDB_val key = {.mv_size = sizeof(key_be), .mv_data = &key_be};
  MDB_val val = {.mv_size = payload_len, .mv_data = (void *)payload};
  return mdb_put(ctx->txn, ctx->domain->delta_dbi, &key, &val, 0);
}

static dtlv_usearch_txn_ctx *dtlv_usearch_txn_ctx_new(dtlv_usearch_domain *domain,
                                                      MDB_txn *txn) {
  dtlv_usearch_txn_ctx *ctx = calloc(1, sizeof(*ctx));
  if (!ctx) return NULL;
  ctx->domain = domain;
  ctx->txn = txn;
  return ctx;
}

int dtlv_usearch_stage_update(dtlv_usearch_domain *domain,
                              MDB_txn *txn,
                              const dtlv_usearch_update *update,
                              dtlv_usearch_txn_ctx **ctx_inout) {
  if (!domain || !txn || !update || !ctx_inout) return EINVAL;
  if (!update->key || update->key_len == 0) return EINVAL;
  if (update->payload_len > 0 && !update->payload) return EINVAL;
  if (update->payload_len > UINT32_MAX) return EINVAL;
  dtlv_usearch_txn_ctx *ctx = *ctx_inout;
  if (ctx && ctx->txn != txn) return EINVAL;
  if (!ctx) {
    ctx = dtlv_usearch_txn_ctx_new(domain, txn);
    if (!ctx) return ENOMEM;
    int rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_SNAPSHOT_SEQ, 0, &ctx->snapshot_seq);
    if (rc == 0) rc = dtlv_meta_get_u64(txn, domain->meta_dbi, DTLV_META_KEY_LOG_SEQ, 0, &ctx->log_seq_head);
    if (rc != 0) {
      free(ctx);
      return rc;
    }
    ctx->last_log_seq = ctx->log_seq_head;
    rc = dtlv_usearch_wal_open(domain->pending_root,
                               ctx->snapshot_seq,
                               ctx->log_seq_head + 1,
                               &ctx->wal);
    if (rc != 0) {
      free(ctx);
      return rc;
    }
    *ctx_inout = ctx;
  }

  uint8_t *delta_buf = NULL;
  size_t delta_len = 0;
  uint32_t ordinal = ctx->frames_appended + 1;
  int rc = dtlv_encode_delta_payload(ctx, update, ordinal, &delta_buf, &delta_len);
  if (rc == 0 && dtlv_trace_enabled()) {
    dtlv_tracef("[delta] ordinal=%u len=%zu\n", (unsigned)ordinal, delta_len);
    size_t preview = delta_len < 32 ? delta_len : 32;
    dtlv_tracef("[delta] bytes:");
    for (size_t i = 0; i < preview; ++i) {
      dtlv_tracef(" %02x", delta_buf[i]);
    }
    dtlv_tracef("\n");
  }
  if (rc != 0) {
    free(delta_buf);
    return rc;
  }
  ctx->last_log_seq = ctx->log_seq_head + 1;
  rc = dtlv_append_delta(ctx, ctx->last_log_seq, delta_buf, delta_len);
  if (rc != 0) {
    free(delta_buf);
    return rc;
  }
  rc = dtlv_meta_put_u64(txn, domain->meta_dbi, DTLV_META_KEY_LOG_SEQ, ctx->last_log_seq);
  if (rc != 0) {
    free(delta_buf);
    return rc;
  }
  rc = dtlv_usearch_wal_append(ctx->wal, delta_buf, delta_len);
  free(delta_buf);
  if (rc != 0) return rc;
  ctx->frames_appended = ordinal;
  ctx->log_seq_head = ctx->last_log_seq;
  return 0;
}

int dtlv_usearch_apply_pending(dtlv_usearch_txn_ctx *ctx) {
  if (!ctx || !ctx->wal) return EINVAL;
  if (ctx->frames_appended == 0) return EINVAL;
  int rc = dtlv_usearch_wal_seal(ctx->wal);
  if (rc != 0) return rc;
  const dtlv_uuid128 *token = dtlv_usearch_wal_token(ctx->wal);
  if (!token) return EINVAL;
  struct {
    uint64_t token_hi;
    uint64_t token_lo;
    uint64_t log_seq;
  } record;
  record.token_hi = dtlv_to_be64(token->hi);
  record.token_lo = dtlv_to_be64(token->lo);
  record.log_seq = dtlv_to_be64(ctx->last_log_seq);
  return dtlv_meta_put_bytes(ctx->txn,
                             ctx->domain->meta_dbi,
                             DTLV_META_KEY_SEALED_LOG_SEQ,
                             &record,
                             sizeof(record));
}

int dtlv_usearch_publish_log(dtlv_usearch_txn_ctx *ctx, int unlink_after_publish) {
  if (!ctx || !ctx->wal) return EINVAL;
  if (ctx->frames_appended == 0) return EINVAL;
  int rc = dtlv_usearch_wal_mark_ready(ctx->wal);
  if (rc != 0) return rc;
  const dtlv_uuid128 *token = dtlv_usearch_wal_token(ctx->wal);
  if (!token) return EINVAL;
  dtlv_uuid128 tail_token;
  uint32_t tail_ordinal = 0;
  int tail_found = 0;
  rc = dtlv_usearch_read_published_tail(ctx->domain, &tail_token, &tail_ordinal, &tail_found);
  if (rc != 0) return rc;
  uint32_t start = 1;
  if (tail_found && tail_token.hi == token->hi && tail_token.lo == token->lo) {
    if (tail_ordinal == UINT32_MAX) return EOVERFLOW;
    start = tail_ordinal + 1;
  }
  const char *path = dtlv_usearch_wal_ready_path(ctx->wal);
  if (!path) return EINVAL;
  if (dtlv_trace_enabled()) {
#ifndef _WIN32
    struct stat st;
    if (stat(path, &st) == 0) {
      dtlv_tracef("[wal] ready_size=%lld\n", (long long)st.st_size);
    }
#endif
  }
  return dtlv_usearch_replay_wal_file(ctx->domain, path, token, start, unlink_after_publish);
}

void dtlv_usearch_txn_ctx_abort(dtlv_usearch_txn_ctx *ctx) {
  if (!ctx) return;
  if (ctx->wal) {
    dtlv_usearch_wal_close(ctx->wal, 1);
    ctx->wal = NULL;
  }
  free(ctx);
}

void dtlv_usearch_txn_ctx_close(dtlv_usearch_txn_ctx *ctx) {
  if (!ctx) return;
  if (ctx->wal) {
    dtlv_usearch_wal_close(ctx->wal, 0);
    ctx->wal = NULL;
  }
  free(ctx);
}

static uint16_t dtlv_read_le16(const uint8_t *ptr) {
  return (uint16_t)((uint16_t)ptr[0] | ((uint16_t)ptr[1] << 8));
}

static uint32_t dtlv_read_le32(const uint8_t *ptr) {
  uint32_t value = 0;
  value |= (uint32_t)ptr[0];
  value |= (uint32_t)ptr[1] << 8;
  value |= (uint32_t)ptr[2] << 16;
  value |= (uint32_t)ptr[3] << 24;
  return value;
}

static uint64_t dtlv_read_le64(const uint8_t *ptr) {
  uint64_t lo = dtlv_read_le32(ptr);
  uint64_t hi = dtlv_read_le32(ptr + 4);
  return lo | (hi << 32);
}

static uint32_t dtlv_pack_usearch_version(uint16_t major, uint16_t minor, uint16_t patch) {
  uint32_t packed_major = ((uint32_t)major & 0xFFFu) << 20;
  uint32_t packed_minor = ((uint32_t)minor & 0x3FFu) << 10;
  uint32_t packed_patch = (uint32_t)patch & 0x3FFu;
  return packed_major | packed_minor | packed_patch;
}

static int dtlv_safe_offset(uint64_t a, uint64_t b, uint64_t addend, uint64_t *out) {
  if (!out) return EINVAL;
  if (a != 0 && b > UINT64_MAX / a) return ERANGE;
  uint64_t product = a * b;
  if (UINT64_MAX - product < addend) return ERANGE;
  *out = product + addend;
  return 0;
}

static int dtlv_usearch_file_size(const char *path, uint64_t *size_out) {
  if (!path || !size_out) return EINVAL;
#ifdef _WIN32
  struct _stat64 st;
  if (_stat64(path, &st) != 0) return errno ? errno : EIO;
#else
  struct stat st;
  if (stat(path, &st) != 0) return errno ? errno : EIO;
#endif
  if (st.st_size < 0) return EINVAL;
  *size_out = (uint64_t)st.st_size;
  return 0;
}

static int dtlv_usearch_file_seek(FILE *file, uint64_t offset) {
  if (!file) return EINVAL;
#ifdef _WIN32
  if (_fseeki64(file, (long long)offset, SEEK_SET) != 0) return errno ? errno : EIO;
#else
  if (fseeko(file, (off_t)offset, SEEK_SET) != 0) return errno ? errno : EIO;
#endif
  return 0;
}

static int dtlv_usearch_try_header(FILE *file,
                                   uint64_t offset,
                                   uint8_t out[DTLV_USEARCH_HEADER_BYTES]) {
  int rc = dtlv_usearch_file_seek(file, offset);
  if (rc != 0) return rc;
  size_t read = fread(out, 1, DTLV_USEARCH_HEADER_BYTES, file);
  if (read != DTLV_USEARCH_HEADER_BYTES) return ferror(file) ? (errno ? errno : EIO) : EINVAL;
  if (memcmp(out, DTLV_USEARCH_MAGIC, DTLV_USEARCH_MAGIC_LEN) == 0) return 0;
  return ENOENT;
}

static int dtlv_usearch_load_head(const char *path,
                                  uint8_t out[DTLV_USEARCH_HEADER_BYTES]) {
  if (!path || !out) return EINVAL;
  FILE *file = fopen(path, "rb");
  if (!file) return errno ? errno : EIO;
  uint8_t first[DTLV_USEARCH_HEADER_BYTES];
  size_t read = fread(first, 1, DTLV_USEARCH_HEADER_BYTES, file);
  if (read != DTLV_USEARCH_HEADER_BYTES) {
    int err = ferror(file) ? (errno ? errno : EIO) : EINVAL;
    fclose(file);
    return err;
  }
  if (memcmp(first, DTLV_USEARCH_MAGIC, DTLV_USEARCH_MAGIC_LEN) == 0) {
    memcpy(out, first, DTLV_USEARCH_HEADER_BYTES);
    fclose(file);
    return 0;
  }
  uint64_t file_size = 0;
  int rc = dtlv_usearch_file_size(path, &file_size);
  if (rc != 0) {
    fclose(file);
    return rc;
  }
  if (file_size < DTLV_USEARCH_HEADER_BYTES) {
    fclose(file);
    return EINVAL;
  }
  uint64_t offset = 0;
  rc = dtlv_safe_offset((uint64_t)dtlv_read_le32(first),
                        (uint64_t)dtlv_read_le32(first + sizeof(uint32_t)),
                        (uint64_t)sizeof(uint32_t) * 2u,
                        &offset);
  if (rc == 0 && offset + DTLV_USEARCH_HEADER_BYTES <= file_size) {
    rc = dtlv_usearch_try_header(file, offset, out);
    if (rc == 0) {
      fclose(file);
      return 0;
    }
  }
  rc = dtlv_safe_offset(dtlv_read_le64(first),
                        dtlv_read_le64(first + sizeof(uint64_t)),
                        (uint64_t)sizeof(uint64_t) * 2u,
                        &offset);
  if (rc == 0 && offset + DTLV_USEARCH_HEADER_BYTES <= file_size) {
    rc = dtlv_usearch_try_header(file, offset, out);
    if (rc == 0) {
      fclose(file);
      return 0;
    }
  }
  fclose(file);
  return EINVAL;
}

int dtlv_usearch_probe_filesystem(const char *path, dtlv_usearch_format_info *info) {
  if (!path || !info) return EINVAL;
  memset(info, 0, sizeof(*info));
  uint8_t header[DTLV_USEARCH_HEADER_BYTES];
  int rc = dtlv_usearch_load_head(path, header);
  if (rc != 0) return rc;
  usearch_init_options_t opts;
  memset(&opts, 0, sizeof(opts));
  usearch_error_t err = NULL;
  usearch_metadata(path, &opts, &err);
  rc = dtlv_usearch_error_status(err);
  if (rc != 0) return rc;
  size_t version_offset = DTLV_USEARCH_MAGIC_LEN;
  uint16_t major = dtlv_read_le16(header + version_offset);
  uint16_t minor = dtlv_read_le16(header + version_offset + sizeof(uint16_t));
  uint16_t patch = dtlv_read_le16(header + version_offset + sizeof(uint16_t) * 2u);
  info->schema_version = dtlv_pack_usearch_version(major, minor, patch);
  info->metric_kind = opts.metric_kind;
  info->scalar_kind = opts.quantization;
  info->dimensions = dtlv_size_to_u32(opts.dimensions);
  info->connectivity = dtlv_size_to_u32(opts.connectivity);
  info->multi = opts.multi ? true : false;
  memset(info->reserved, 0, sizeof(info->reserved));
  return 0;
}

int dtlv_usearch_inspect_domain(dtlv_usearch_domain *domain,
                                MDB_txn *txn,
                                dtlv_usearch_format_info *info) {
  if (!domain || !domain->env || !info) return EINVAL;
  memset(info, 0, sizeof(*info));
  MDB_txn *local_txn = txn;
  int owned_txn = 0;
  if (!local_txn) {
    int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &local_txn);
    if (rc != 0) return rc;
    owned_txn = 1;
  }
  usearch_init_options_t opts;
  memset(&opts, 0, sizeof(opts));
  int found = 0;
  int rc = dtlv_usearch_load_init_options(domain, local_txn, &opts, &found);
  if (rc != 0) {
    if (owned_txn) mdb_txn_abort(local_txn);
    return rc;
  }
  if (!found) {
    if (owned_txn) mdb_txn_abort(local_txn);
    return ENOENT;
  }
  uint64_t schema_u64 = DTLV_USEARCH_SCHEMA_VERSION;
  rc = dtlv_meta_get_u64(local_txn,
                         domain->meta_dbi,
                         DTLV_META_KEY_SCHEMA_VERSION,
                         DTLV_USEARCH_SCHEMA_VERSION,
                         &schema_u64);
  if (owned_txn) mdb_txn_abort(local_txn);
  if (rc != 0) return rc;
  info->schema_version = schema_u64 > UINT32_MAX ? UINT32_MAX : (uint32_t)schema_u64;
  info->metric_kind = opts.metric_kind;
  info->scalar_kind = opts.quantization;
  info->dimensions = dtlv_size_to_u32(opts.dimensions);
  info->connectivity = dtlv_size_to_u32(opts.connectivity);
  info->multi = opts.multi ? true : false;
  memset(info->reserved, 0, sizeof(info->reserved));
  return 0;
}

int dtlv_usearch_set_checkpoint_chunk_batch(dtlv_usearch_domain *domain, uint32_t batch) {
  if (!domain || !domain->env) return EINVAL;
  if (batch == 0) batch = DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, 0, &txn);
  if (rc != 0) return rc;
  rc = dtlv_meta_put_u32(txn, domain->meta_dbi, DTLV_META_KEY_CHECKPOINT_CHUNK_BATCH, batch);
  if (rc == 0) rc = mdb_txn_commit(txn);
  else if (txn) mdb_txn_abort(txn);
  if (rc == 0) domain->checkpoint_chunk_batch = batch;
  return rc;
}

int dtlv_usearch_get_checkpoint_chunk_batch(dtlv_usearch_domain *domain, uint32_t *batch_out) {
  if (!domain || !batch_out || !domain->env) return EINVAL;
  MDB_txn *txn = NULL;
  int rc = mdb_txn_begin(domain->env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  uint32_t batch = DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  rc = dtlv_meta_get_u32(txn,
                         domain->meta_dbi,
                         DTLV_META_KEY_CHECKPOINT_CHUNK_BATCH,
                         DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH,
                         &batch);
  mdb_txn_abort(txn);
  if (rc != 0) return rc;
  if (batch == 0) batch = DTLV_USEARCH_DEFAULT_CHECKPOINT_CHUNK_BATCH;
  *batch_out = batch;
  return 0;
}
