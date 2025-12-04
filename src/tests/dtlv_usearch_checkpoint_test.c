#include "dtlv_usearch.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dtlv_bytes.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define META_KEY_LOG_TAIL_SEQ "log_tail_seq"
#define META_KEY_LOG_SEQ "log_seq"
#define META_KEY_SCHEMA_VERSION "schema_version"
#define META_KEY_SNAPSHOT_SEQ "snapshot_seq"
#define META_KEY_SNAPSHOT_RETAINED_FLOOR "snapshot_retained_floor"
#define META_KEY_CHECKPOINT_PENDING "checkpoint_pending"
#define META_KEY_PUBLISHED_LOG_TAIL "published_log_tail"
#define META_KEY_COMPACTION_PENDING "compaction_pending"
#define META_KEY_COMPACTION_APPLIED "compaction_applied"
#define PIN_RECORD_SIZE 48
#define PIN_SLOT_COUNT 64

#define TRACE_TEST(name)                              \
  do {                                                \
    if (getenv("DTLV_TRACE_TESTS")) {                 \
      fprintf(stderr, "[dtlv_test] %s\n", (name));    \
    }                                                 \
  } while (0)

#ifdef _WIN32
#include <direct.h>
#include <shellapi.h>
#include <windows.h>
#else
#include <dirent.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

static int ensure_directory(const char *path) {
#ifdef _WIN32
  if (_mkdir(path) == 0) return 0;
  if (errno == EEXIST) return 0;
  return errno;
#else
  if (mkdir(path, 0775) == 0) return 0;
  if (errno == EEXIST) return 0;
  return errno;
#endif
}

static void remove_tree(const char *path) {
  if (!path || !*path) return;
#ifdef _WIN32
  char buffer[PATH_MAX + 2];
  snprintf(buffer, sizeof(buffer), "%s", path);
  size_t len = strlen(buffer);
  buffer[len + 1] = '\0';
  SHFILEOPSTRUCTA op;
  memset(&op, 0, sizeof(op));
  op.wFunc = FO_DELETE;
  op.pFrom = buffer;
  op.fFlags = FOF_NO_UI;
  SHFileOperationA(&op);
#else
  DIR *dir = opendir(path);
  if (!dir) {
    rmdir(path);
    return;
  }
  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
    char child[PATH_MAX];
    snprintf(child, sizeof(child), "%s/%s", path, entry->d_name);
    struct stat st;
    if (lstat(child, &st) != 0) continue;
    if (S_ISDIR(st.st_mode)) remove_tree(child);
    else unlink(child);
  }
  closedir(dir);
  rmdir(path);
#endif
}

static void expect_domain_open(MDB_env *env,
                               const char *domain_name,
                               const char *filesystem_root,
                               dtlv_usearch_domain **domain_out);

static int dtlv_should_run_test(const char *name) {
  const char *filter = getenv("DTLV_TEST_FILTER");
  if (!filter || !*filter) return 1;
  return strstr(name, filter) != NULL;
}

static void dtlv_run_test(const char *name, void (*fn)(void)) {
  if (getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] %s %s\n", dtlv_should_run_test(name) ? "run" : "skip", name);
  }
  if (!dtlv_should_run_test(name)) return;
  fn();
}

static uint32_t pack_usearch_version_components(uint32_t major,
                                                uint32_t minor,
                                                uint32_t patch) {
  uint32_t packed_major = (major & 0xFFFu) << 20;
  uint32_t packed_minor = (minor & 0x3FFu) << 10;
  uint32_t packed_patch = patch & 0x3FFu;
  return packed_major | packed_minor | packed_patch;
}

static uint32_t parse_usearch_version_code(const char *version) {
  if (!version) return 0;
  unsigned int major = 0;
  unsigned int minor = 0;
  unsigned int patch = 0;
  if (sscanf(version, "%u.%u.%u", &major, &minor, &patch) != 3) return 0;
  return pack_usearch_version_components(major, minor, patch);
}

static int64_t test_now_ms(void) {
#ifdef _WIN32
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  ULARGE_INTEGER li;
  li.LowPart = ft.dwLowDateTime;
  li.HighPart = ft.dwHighDateTime;
  return (int64_t)(li.QuadPart / 10000);
#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
#endif
}

static char *create_temp_dir(void) {
#ifdef _WIN32
  char base[MAX_PATH];
  if (!GetTempPathA(sizeof(base), base)) return NULL;
  char temp[MAX_PATH];
  if (!GetTempFileNameA(base, "dtlv", 0, temp)) return NULL;
  DeleteFileA(temp);
  if (CreateDirectoryA(temp, NULL) == 0) return NULL;
  return _strdup(temp);
#else
  char templ[PATH_MAX];
  snprintf(templ, sizeof(templ), "./dtlvtestXXXXXX");
  char *dir = mkdtemp(templ);
  if (!dir) return NULL;
  char resolved[PATH_MAX];
  if (realpath(dir, resolved)) return strdup(resolved);
  return strdup(dir);
#endif
}

static MDB_env *create_env_with_mapsize(const char *path, size_t mapsize) {
  MDB_env *env = NULL;
  if (ensure_directory(path) != 0) return NULL;
  if (mdb_env_create(&env) != 0) return NULL;
  mdb_env_set_maxdbs(env, 64);
  if (mapsize == 0) mapsize = 64 * 1024 * 1024;
  mdb_env_set_mapsize(env, mapsize);
  int rc = mdb_env_open(env, path, MDB_NOLOCK, 0664);
  if (rc != 0) {
    mdb_env_close(env);
    return NULL;
  }
  return env;
}

static MDB_env *create_env(const char *path) {
  return create_env_with_mapsize(path, 64 * 1024 * 1024);
}

static int open_domain_dbi(MDB_txn *txn,
                           const char *domain,
                           const char *suffix,
                           MDB_dbi *dbi) {
  char name[PATH_MAX];
  if (snprintf(name, sizeof(name), "%s/%s", domain, suffix) >= (int)sizeof(name)) return ENAMETOOLONG;
  return mdb_dbi_open(txn, name, 0, dbi);
}

static int meta_get_u64(MDB_env *env,
                        const char *domain,
                        const char *key,
                        uint64_t *out,
                        int *found) {
  MDB_txn *txn = NULL;
  MDB_dbi meta;
  MDB_val k = {.mv_size = strlen(key) + 1, .mv_data = (void *)key};
  MDB_val v;
  int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  rc = open_domain_dbi(txn, domain, "usearch-meta", &meta);
  if (rc == 0) rc = mdb_get(txn, meta, &k, &v);
  if (rc == MDB_NOTFOUND) {
    if (found) *found = 0;
    mdb_txn_abort(txn);
    return 0;
  }
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  if (v.mv_size != sizeof(uint64_t)) {
    mdb_txn_abort(txn);
    return EINVAL;
  }
  uint64_t be = 0;
  memcpy(&be, v.mv_data, sizeof(be));
  if (out) *out = dtlv_from_be64(be);
  if (found) *found = 1;
  mdb_txn_abort(txn);
  return 0;
}

static int meta_key_exists(MDB_env *env, const char *domain, const char *key) {
  MDB_txn *txn = NULL;
  MDB_dbi meta;
  MDB_val k = {.mv_size = strlen(key) + 1, .mv_data = (void *)key};
  int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return 0;
  rc = open_domain_dbi(txn, domain, "usearch-meta", &meta);
  if (rc != 0) {
    mdb_txn_abort(txn);
    return 0;
  }
  MDB_val v;
  rc = mdb_get(txn, meta, &k, &v);
  mdb_txn_abort(txn);
  if (rc == MDB_NOTFOUND) return 0;
  if (rc != 0) return 0;
  return 1;
}

static int delta_count(MDB_env *env, const char *domain) {
  MDB_txn *txn = NULL;
  MDB_dbi delta;
  int count = 0;
  if (mdb_txn_begin(env, NULL, MDB_RDONLY, &txn) != 0) return -1;
  if (open_domain_dbi(txn, domain, "usearch-delta", &delta) != 0) {
    mdb_txn_abort(txn);
    return -1;
  }
  MDB_cursor *cursor = NULL;
  if (mdb_cursor_open(txn, delta, &cursor) != 0) {
    mdb_txn_abort(txn);
    return -1;
  }
  MDB_val key;
  MDB_val val;
  int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
  while (rc == 0) {
    count++;
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  return count;
}

static int meta_get_published_tail(MDB_env *env,
                                   const char *domain,
                                   uint32_t *ordinal_out,
                                   int *found) {
  MDB_txn *txn = NULL;
  MDB_dbi meta;
  MDB_val key = {.mv_size = strlen(META_KEY_PUBLISHED_LOG_TAIL) + 1,
                 .mv_data = (void *)META_KEY_PUBLISHED_LOG_TAIL};
  MDB_val val;
  int rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
  if (rc != 0) return rc;
  rc = open_domain_dbi(txn, domain, "usearch-meta", &meta);
  if (rc == 0) rc = mdb_get(txn, meta, &key, &val);
  if (rc == MDB_NOTFOUND) {
    if (found) *found = 0;
    mdb_txn_abort(txn);
    return 0;
  }
  if (rc != 0) {
    mdb_txn_abort(txn);
    return rc;
  }
  if (val.mv_size != sizeof(uint64_t) * 2 + sizeof(uint32_t) * 2) {
    mdb_txn_abort(txn);
    return EINVAL;
  }
  uint32_t ordinal_be = 0;
  memcpy(&ordinal_be, (uint8_t *)val.mv_data + sizeof(uint64_t) * 2, sizeof(ordinal_be));
  if (ordinal_out) *ordinal_out = dtlv_from_be32(ordinal_be);
  if (found) *found = 1;
  mdb_txn_abort(txn);
  return 0;
}

static size_t snapshot_chunks_for_seq(MDB_env *env,
                                      const char *domain,
                                      uint64_t seq) {
  MDB_txn *txn = NULL;
  MDB_cursor *cursor = NULL;
  MDB_dbi snapshot;
  size_t count = 0;
  if (mdb_txn_begin(env, NULL, MDB_RDONLY, &txn) != 0) return 0;
  if (open_domain_dbi(txn, domain, "usearch-snapshot", &snapshot) != 0) {
    mdb_txn_abort(txn);
    return 0;
  }
  if (mdb_cursor_open(txn, snapshot, &cursor) != 0) {
    mdb_txn_abort(txn);
    return 0;
  }
  MDB_val key;
  MDB_val val;
  int rc = mdb_cursor_get(cursor, &key, &val, MDB_FIRST);
  while (rc == 0) {
    if (key.mv_size == sizeof(uint64_t) + sizeof(uint32_t)) {
      uint64_t seq_be;
      memcpy(&seq_be, key.mv_data, sizeof(seq_be));
      if (dtlv_from_be64(seq_be) == seq) count++;
    }
    rc = mdb_cursor_get(cursor, &key, &val, MDB_NEXT);
  }
  mdb_cursor_close(cursor);
  mdb_txn_abort(txn);
  return count;
}

typedef struct {
  uint8_t version;
  dtlv_uuid128 reader_uuid;
  uint64_t snapshot_seq;
  uint64_t log_seq;
  int64_t expires_at_ms;
} test_pin_record;

static void pack_pin_key(const dtlv_uuid128 *uuid, uint8_t out[16]) {
  if (!uuid || !out) return;
  uint64_t hi_be = dtlv_to_be64(uuid->hi);
  uint64_t lo_be = dtlv_to_be64(uuid->lo);
  memcpy(out, &hi_be, sizeof(hi_be));
  memcpy(out + sizeof(hi_be), &lo_be, sizeof(lo_be));
}

static int unpack_pin_record(const uint8_t *src, test_pin_record *record) {
  if (!src || !record) return EINVAL;
  if (src[0] == 0) return ENOENT;
  if (src[0] != 1) return EINVAL;
  memset(record, 0, sizeof(*record));
  record->version = src[0];
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
  return 0;
}

static int load_pin_record(const char *pins_path,
                           const dtlv_uuid128 *uuid,
                           test_pin_record *record,
                           int *found) {
  if (found) *found = 0;
  if (!pins_path || !uuid) return EINVAL;
  size_t expected = PIN_RECORD_SIZE * PIN_SLOT_COUNT;
#ifdef _WIN32
  char win_path[PATH_MAX];
  snprintf(win_path, sizeof(win_path), "%s", pins_path);
  for (char *p = win_path; *p; ++p) {
    if (*p == '/') *p = '\\';
  }
  HANDLE file = CreateFileA(win_path,
                            GENERIC_READ,
                            FILE_SHARE_READ | FILE_SHARE_WRITE,
                            NULL,
                            OPEN_EXISTING,
                            FILE_ATTRIBUTE_NORMAL,
                            NULL);
  if (file == INVALID_HANDLE_VALUE) {
    DWORD err = GetLastError();
    if (err == ERROR_FILE_NOT_FOUND) return 0;
    return (int)err;
  }
  uint8_t *buf = (uint8_t *)malloc(expected);
  if (!buf) {
    CloseHandle(file);
    return ENOMEM;
  }
  DWORD read_bytes = 0;
  BOOL ok = ReadFile(file, buf, (DWORD)expected, &read_bytes, NULL);
  CloseHandle(file);
  if (!ok) {
    int err = (int)GetLastError();
    free(buf);
    return err ? err : EIO;
  }
  if (read_bytes < expected) {
    free(buf);
    return EIO;
  }
#else
  int fd = open(pins_path, O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) return 0;
    return errno ? errno : EIO;
  }
  uint8_t *buf = (uint8_t *)malloc(expected);
  if (!buf) {
    close(fd);
    return ENOMEM;
  }
  ssize_t read_bytes = read(fd, buf, expected);
  close(fd);
  if (read_bytes < 0) {
    int err = errno ? errno : EIO;
    free(buf);
    return err;
  }
  if ((size_t)read_bytes < expected) {
    free(buf);
    return EIO;
  }
#endif
  uint8_t key_buf[16];
  pack_pin_key(uuid, key_buf);
  for (size_t i = 0; i < PIN_SLOT_COUNT; ++i) {
    const uint8_t *slot = buf + i * PIN_RECORD_SIZE;
    test_pin_record tmp;
    int rc = unpack_pin_record(slot, &tmp);
    if (rc == ENOENT) continue;
    if (rc != 0) {
      memset(buf + i * PIN_RECORD_SIZE, 0, PIN_RECORD_SIZE);
      continue;
    }
    uint8_t slot_key[16];
    pack_pin_key(&tmp.reader_uuid, slot_key);
    if (memcmp(slot_key, key_buf, sizeof(slot_key)) == 0) {
      if (record) memcpy(record, &tmp, sizeof(tmp));
      if (found) *found = 1;
      break;
    }
  }
  free(buf);
  return 0;
}

static int directory_has_suffix(const char *dir, const char *suffix) {
  if (!dir || !suffix) return 0;
#ifdef _WIN32
  char pattern[PATH_MAX];
  if (snprintf(pattern, sizeof(pattern), "%s\\*", dir) >= (int)sizeof(pattern)) return 0;
  for (size_t i = 0; i < strlen(pattern); ++i) {
    if (pattern[i] == '/') pattern[i] = '\\';
  }
  WIN32_FIND_DATAA data;
  HANDLE handle = FindFirstFileA(pattern, &data);
  if (handle == INVALID_HANDLE_VALUE) return 0;
  int found = 0;
  size_t suffix_len = strlen(suffix);
  do {
    if (!strcmp(data.cFileName, ".") || !strcmp(data.cFileName, "..")) continue;
    size_t name_len = strlen(data.cFileName);
    if (name_len >= suffix_len &&
        strcmp(data.cFileName + name_len - suffix_len, suffix) == 0) {
      found = 1;
      break;
    }
  } while (FindNextFileA(handle, &data));
  FindClose(handle);
  return found;
#else
  DIR *d = opendir(dir);
  if (!d) return 0;
  int found = 0;
  struct dirent *entry = NULL;
  size_t suffix_len = strlen(suffix);
  while ((entry = readdir(d)) != NULL) {
    if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
    size_t name_len = strlen(entry->d_name);
    if (name_len >= suffix_len &&
        strcmp(entry->d_name + name_len - suffix_len, suffix) == 0) {
      found = 1;
      break;
    }
  }
  closedir(d);
  return found;
#endif
}

static void setup_paths(const char *root,
                        char *env_path,
                        size_t env_len,
                        char *fs_path,
                        size_t fs_len) {
  snprintf(env_path, env_len, "%s/env", root);
  snprintf(fs_path, fs_len, "%s/fs", root);
  ensure_directory(env_path);
  ensure_directory(fs_path);
}

static void fill_default_init_options(usearch_init_options_t *opts) {
  memset(opts, 0, sizeof(*opts));
  opts->metric_kind = usearch_metric_l2sq_k;
  opts->quantization = usearch_scalar_f32_k;
  opts->dimensions = 4;
  opts->connectivity = 8;
  opts->expansion_add = 16;
  opts->expansion_search = 16;
  opts->multi = false;
}

static usearch_index_t create_index(void) {
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  usearch_error_t err = NULL;
  usearch_index_t index = usearch_init(&opts, &err);
  assert(index != NULL);
  assert(err == NULL);
  err = NULL;
  usearch_reserve(index, 16, &err);
  assert(err == NULL);
  return index;
}

static void destroy_index(usearch_index_t index) {
  usearch_error_t err = NULL;
  usearch_free(index, &err);
  assert(err == NULL);
}

static void persist_init_options(MDB_env *env,
                                 dtlv_usearch_domain *domain,
                                 const usearch_init_options_t *opts) {
  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  assert(dtlv_usearch_store_init_options(domain, txn, opts) == 0);
  assert(mdb_txn_commit(txn) == 0);
}

static void stage_domain_vector(dtlv_usearch_domain *domain,
                                MDB_env *env,
                                uint64_t key,
                                const float *vector,
                                size_t dims,
                                const char *label,
                                int publish_after) {
  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  uint64_t key_be = dtlv_to_be64(key);
  dtlv_usearch_update update = {.op = DTLV_USEARCH_OP_ADD,
                                .key = &key_be,
                                .key_len = sizeof(key_be),
                                .payload = vector,
                                .payload_len = dims * sizeof(float),
                                .scalar_kind = usearch_scalar_f32_k,
                                .dimensions = (uint16_t)dims};
  dtlv_usearch_txn_ctx *ctx = NULL;
  int rc = dtlv_usearch_stage_update(domain, txn, &update, &ctx);
  if (rc != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] stage_domain_vector(%s) rc=%d\n", label ? label : "vector", rc);
  }
  assert(rc == 0);
  assert(dtlv_usearch_apply_pending(ctx) == 0);
  assert(mdb_txn_commit(txn) == 0);
  if (publish_after) {
    rc = dtlv_usearch_publish_log(ctx, 1);
    if (rc != 0 && getenv("DTLV_TRACE_TESTS")) {
      fprintf(stderr, "[dtlv_test] publish(%s) rc=%d\n", label ? label : "vector", rc);
    }
    assert(rc == 0);
  }
  dtlv_usearch_txn_ctx_close(ctx);
}

static void stage_domain_delete(dtlv_usearch_domain *domain,
                                MDB_env *env,
                                uint64_t key,
                                const char *label) {
  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  uint64_t key_be = dtlv_to_be64(key);
  dtlv_usearch_update update = {.op = DTLV_USEARCH_OP_DELETE,
                                .key = &key_be,
                                .key_len = sizeof(key_be),
                                .payload = NULL,
                                .payload_len = 0,
                                .scalar_kind = 0,
                                .dimensions = 0};
  dtlv_usearch_txn_ctx *ctx = NULL;
  int rc = dtlv_usearch_stage_update(domain, txn, &update, &ctx);
  if (rc != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] stage_domain_delete(%s) rc=%d\n", label ? label : "delete", rc);
  }
  assert(rc == 0);
  assert(dtlv_usearch_apply_pending(ctx) == 0);
  assert(mdb_txn_commit(txn) == 0);
  rc = dtlv_usearch_publish_log(ctx, 1);
  if (rc != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] publish_delete(%s) rc=%d\n", label ? label : "delete", rc);
  }
  assert(rc == 0);
  dtlv_usearch_txn_ctx_close(ctx);
}

#ifndef _WIN32
typedef void (*dtlv_child_fn)(const char *env_path, const char *fs_path);

static void dtlv_run_child_process(const char *label,
                                   const char *env_path,
                                   const char *fs_path,
                                   dtlv_child_fn fn,
                                   int expected_exit) {
  pid_t pid = fork();
  assert(pid >= 0);
  if (pid == 0) {
    fn(env_path, fs_path);
    _exit(expected_exit);
  }
  int status = 0;
  pid_t waited = waitpid(pid, &status, 0);
  assert(waited == pid);
  if (status != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] child %s exit_status=%d\n", label ? label : "child", status);
  }
  assert(WIFEXITED(status));
  assert(WEXITSTATUS(status) == expected_exit);
}

static void dtlv_child_stage_vector(const char *env_path, const char *fs_path) {
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "domain-mproc", fs_path, &domain);
  float vec[4] = {0.2f, 0.3f, 0.4f, 0.5f};
  stage_domain_vector(domain, env, 77, vec, 4, "child-writer", 1);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
}

static void dtlv_child_verify_vector(const char *env_path, const char *fs_path) {
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "domain-mproc", fs_path, &domain);
  dtlv_usearch_handle *handle = NULL;
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  usearch_index_t index = dtlv_usearch_handle_index(handle);
  assert(index);
  usearch_error_t err = NULL;
  assert(usearch_contains(index, 77, &err));
  assert(err == NULL);
  err = NULL;
  assert(!usearch_contains(index, 999, &err));
  assert(err == NULL);
  dtlv_usearch_deactivate(handle);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
}

static void dtlv_child_verify_replayed_vector(const char *env_path, const char *fs_path) {
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "domain-mproc", fs_path, &domain);
  dtlv_usearch_handle *handle = NULL;
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  usearch_index_t index = dtlv_usearch_handle_index(handle);
  assert(index);
  usearch_error_t err = NULL;
  assert(usearch_contains(index, 88, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(index, 77, &err));
  assert(err == NULL);
  dtlv_usearch_deactivate(handle);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
}

static void dtlv_child_crash_before_publish(const char *env_path, const char *fs_path) {
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "domain-mproc", fs_path, &domain);
  float vec[4] = {1.5f, 1.6f, 1.7f, 1.8f};
  stage_domain_vector(domain, env, 88, vec, 4, "crash-child", 0);
  _exit(42);
}

static void dtlv_child_checkpoint_crash(const char *env_path, const char *fs_path) {
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "checkpoint-crash", fs_path, &domain);
  usearch_index_t index = create_index();
  float vec[4] = {2.0f, 2.1f, 2.2f, 2.3f};
  usearch_error_t err = NULL;
  usearch_add(index, 55, vec, usearch_scalar_f32_k, &err);
  assert(err == NULL);
  size_t chunk_count = 0;
  dtlv_uuid128 writer = {.hi = 123, .lo = 456};
  int rc = dtlv_usearch_checkpoint_write_snapshot(domain, index, 9, &writer, &chunk_count);
  if (rc != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] child checkpoint write rc=%d\n", rc);
  }
  assert(rc == 0);
  destroy_index(index);
  _exit(17);
}
#endif

static void expect_domain_open(MDB_env *env,
                               const char *domain_name,
                               const char *filesystem_root,
                               dtlv_usearch_domain **domain_out) {
  int rc = dtlv_usearch_domain_open(env, domain_name, filesystem_root, domain_out);
  if (rc != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] domain_open(%s) rc=%d\n", domain_name, rc);
  }
  assert(rc == 0);
}

static void test_checkpoint_finalize_roundtrip(void) {
  TRACE_TEST("checkpoint_finalize_roundtrip");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "roundtrip", fs_path, &domain);
  usearch_index_t index = create_index();
  size_t chunk_count = 0;
  dtlv_uuid128 writer = {.hi = 42, .lo = 7};
  assert(dtlv_usearch_checkpoint_write_snapshot(domain, index, 5, &writer, &chunk_count) == 0);
  assert(chunk_count >= 1);
  assert(dtlv_usearch_checkpoint_finalize(domain, 5, 5) == 0);
  uint64_t snapshot_seq = 0;
  int found = 0;
  assert(meta_get_u64(env, "roundtrip", "snapshot_seq", &snapshot_seq, &found) == 0 && found == 1);
  assert(snapshot_seq == 5);
  uint64_t log_tail = 0;
  assert(meta_get_u64(env, "roundtrip", META_KEY_LOG_TAIL_SEQ, &log_tail, &found) == 0 && found == 1);
  assert(log_tail == 5);
  uint64_t retained_floor = 0;
  assert(meta_get_u64(env, "roundtrip", META_KEY_SNAPSHOT_RETAINED_FLOOR, &retained_floor, &found) == 0 && found == 1);
  assert(retained_floor == 4);
  assert(meta_key_exists(env, "roundtrip", META_KEY_CHECKPOINT_PENDING) == 0);
  size_t chunks = snapshot_chunks_for_seq(env, "roundtrip", 5);
  assert(chunks == chunk_count);
  destroy_index(index);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_checkpoint_recover_writing(void) {
  TRACE_TEST("checkpoint_recover_writing");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "recover", fs_path, &domain);
  usearch_index_t index = create_index();
  size_t chunk_count = 0;
  dtlv_uuid128 writer = {.hi = 9, .lo = 99};
  assert(dtlv_usearch_checkpoint_write_snapshot(domain, index, 7, &writer, &chunk_count) == 0);
  assert(chunk_count >= 1);
  assert(meta_key_exists(env, "recover", META_KEY_CHECKPOINT_PENDING) == 1);
  assert(snapshot_chunks_for_seq(env, "recover", 7) == chunk_count);
  assert(dtlv_usearch_checkpoint_recover(domain) == 0);
  assert(meta_key_exists(env, "recover", META_KEY_CHECKPOINT_PENDING) == 0);
  assert(snapshot_chunks_for_seq(env, "recover", 7) == 0);
  destroy_index(index);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_checkpoint_mapfull_retains_pending(void) {
  TRACE_TEST("checkpoint_mapfull_retains_pending");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env_with_mapsize(env_path, 256 * 1024);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "mapfull", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  opts.dimensions = 32;
  persist_init_options(env, domain, &opts);
  if (getenv("DTLV_TRACE_TESTS")) fprintf(stderr, "[dtlv_test] mapfull: env open\n");

  usearch_error_t err = NULL;
  usearch_index_t index = usearch_init(&opts, &err);
  assert(index != NULL);
  assert(err == NULL);
  err = NULL;
  usearch_reserve(index, 4096, &err);
  assert(err == NULL);
  float vec[32];
  for (size_t i = 0; i < 32; ++i) vec[i] = (float)(i + 1);
  for (size_t i = 0; i < 2048; ++i) {
    err = NULL;
    usearch_add(index, (usearch_key_t)(i + 1), vec, opts.quantization, &err);
    assert(err == NULL);
  }
  if (getenv("DTLV_TRACE_TESTS")) fprintf(stderr, "[dtlv_test] mapfull: populated index\n");
  size_t serialized_len = usearch_serialized_length(index, &err);
  assert(err == NULL);
  assert(serialized_len > 200 * 1024);

  dtlv_uuid128 writer = {.hi = 0xABCDEF01u, .lo = 0x10203040u};
  size_t chunk_count = 0;
  int rc = dtlv_usearch_checkpoint_write_snapshot(domain, index, 5, &writer, &chunk_count);
  if (getenv("DTLV_TRACE_TESTS")) fprintf(stderr, "[dtlv_test] mapfull: checkpoint rc=%d chunk_count=%zu\n", rc, chunk_count);
  assert(rc == MDB_MAP_FULL);
  assert(meta_key_exists(env, "mapfull", META_KEY_CHECKPOINT_PENDING) == 1);

  assert(mdb_env_set_mapsize(env, 8 * 1024 * 1024) == 0);
  assert(dtlv_usearch_checkpoint_recover(domain) == 0);
  assert(meta_key_exists(env, "mapfull", META_KEY_CHECKPOINT_PENDING) == 0);
  assert(snapshot_chunks_for_seq(env, "mapfull", 5) == 0);

  chunk_count = 0;
  assert(dtlv_usearch_checkpoint_write_snapshot(domain, index, 5, &writer, &chunk_count) == 0);
  assert(chunk_count >= 1);
  assert(dtlv_usearch_checkpoint_finalize(domain, 5, 5) == 0);

  destroy_index(index);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_activation_replays_snapshot_and_delta(void) {
  TRACE_TEST("activation_replays_snapshot_and_delta");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "activate", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);

  usearch_index_t snapshot_index = create_index();
  float base_vec[4] = {0.f, 1.f, 2.f, 3.f};
  usearch_error_t err = NULL;
  usearch_add(snapshot_index, 1, base_vec, usearch_scalar_f32_k, &err);
  assert(err == NULL);
  size_t chunk_count = 0;
  dtlv_uuid128 writer = {.hi = 3, .lo = 17};
  assert(dtlv_usearch_checkpoint_write_snapshot(domain, snapshot_index, 1, &writer, &chunk_count) == 0);
  assert(dtlv_usearch_checkpoint_finalize(domain, 1, 1) == 0);
  destroy_index(snapshot_index);

  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  float new_vec[4] = {0.f, 0.5f, 1.f, 1.5f};
  uint64_t key_be = dtlv_to_be64(2);
  dtlv_usearch_update update = {.op = DTLV_USEARCH_OP_ADD,
                                .key = &key_be,
                                .key_len = sizeof(key_be),
                                .payload = new_vec,
                                .payload_len = sizeof(new_vec),
                                .scalar_kind = usearch_scalar_f32_k,
                                .dimensions = 4};
  dtlv_usearch_txn_ctx *txn_ctx = NULL;
  assert(dtlv_usearch_stage_update(domain, txn, &update, &txn_ctx) == 0);
  assert(dtlv_usearch_apply_pending(txn_ctx) == 0);
  assert(mdb_txn_commit(txn) == 0);
  int publish_rc = dtlv_usearch_publish_log(txn_ctx, 1);
  if (publish_rc != 0 && getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "publish_log rc=%d\n", publish_rc);
  }
  assert(publish_rc == 0);
  dtlv_usearch_txn_ctx_close(txn_ctx);
  uint64_t meta_log_seq = 0;
  int meta_found = 0;
  assert(meta_get_u64(env, "activate", "log_seq", &meta_log_seq, &meta_found) == 0 && meta_found == 1);
  assert(meta_log_seq == 2);

  dtlv_usearch_handle *handle = NULL;
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  assert(handle != NULL);
  usearch_index_t restored = dtlv_usearch_handle_index(handle);
  assert(restored != NULL);
  err = NULL;
  assert(usearch_contains(restored, 1, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(restored, 2, &err));
  assert(err == NULL);
  usearch_key_t keys[2] = {0};
  usearch_distance_t distances[2] = {0};
  float query[4] = {0.f, 0.5f, 1.f, 1.5f};
  err = NULL;
  size_t found = usearch_search(restored,
                                query,
                                usearch_scalar_f32_k,
                                2,
                                keys,
                                distances,
                                &err);
  assert(err == NULL);
  assert(found >= 1);
  int matched_delta = 0;
  for (size_t i = 0; i < found; ++i) {
    if (keys[i] == 2) matched_delta = 1;
  }
  assert(matched_delta == 1);
  dtlv_usearch_deactivate(handle);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_handle_refresh_tracks_updates(void) {
  TRACE_TEST("handle_refresh_tracks_updates");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "refresh", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);

  dtlv_usearch_handle *handle = NULL;
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  assert(handle != NULL);

  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  float vec[4] = {1.f, 2.f, 3.f, 4.f};
  uint64_t key_be = dtlv_to_be64(11);
  dtlv_usearch_update update = {.op = DTLV_USEARCH_OP_ADD,
                                .key = &key_be,
                                .key_len = sizeof(key_be),
                                .payload = vec,
                                .payload_len = sizeof(vec),
                                .scalar_kind = usearch_scalar_f32_k,
                                .dimensions = 4};
  dtlv_usearch_txn_ctx *txn_ctx = NULL;
  assert(dtlv_usearch_stage_update(domain, txn, &update, &txn_ctx) == 0);
  assert(dtlv_usearch_apply_pending(txn_ctx) == 0);
  assert(mdb_txn_commit(txn) == 0);

  usearch_index_t index = dtlv_usearch_handle_index(handle);
  assert(index != NULL);
  usearch_error_t err = NULL;
  assert(!usearch_contains(index, 11, &err));
  assert(err == NULL);

  MDB_txn *refresh_txn = NULL;
  assert(mdb_txn_begin(env, NULL, MDB_RDONLY, &refresh_txn) == 0);
  assert(dtlv_usearch_refresh(handle, refresh_txn) == 0);
  mdb_txn_abort(refresh_txn);
  index = dtlv_usearch_handle_index(handle);
  assert(index != NULL);
  err = NULL;
  assert(usearch_contains(index, 11, &err));
  assert(err == NULL);

  dtlv_usearch_deactivate(handle);
  handle = NULL;
  assert(dtlv_usearch_publish_log(txn_ctx, 1) == 0);
  dtlv_usearch_txn_ctx_close(txn_ctx);
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  assert(handle != NULL);
  index = dtlv_usearch_handle_index(handle);
  assert(index != NULL);

  dtlv_uuid128 writer = {.hi = 10, .lo = 20};
  size_t chunk_count = 0;
  assert(dtlv_usearch_checkpoint_write_snapshot(domain, index, 1, &writer, &chunk_count) == 0);
  assert(dtlv_usearch_checkpoint_finalize(domain, 1, 1) == 0);

  err = NULL;
  usearch_remove(index, 11, &err);
  assert(err == NULL);

  assert(mdb_txn_begin(env, NULL, MDB_RDONLY, &refresh_txn) == 0);
  assert(dtlv_usearch_refresh(handle, refresh_txn) == 0);
  mdb_txn_abort(refresh_txn);

  index = dtlv_usearch_handle_index(handle);
  assert(index != NULL);
  err = NULL;
  assert(usearch_contains(index, 11, &err));
  assert(err == NULL);

  dtlv_usearch_deactivate(handle);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_publish_updates_active_handles(void) {
  TRACE_TEST("publish_updates_active_handles");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "publish", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);
  dtlv_usearch_handle *handle = NULL;
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  assert(handle != NULL);
  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  float vec[4] = {5.f, 6.f, 7.f, 8.f};
  uint64_t key_be = dtlv_to_be64(77);
  dtlv_usearch_update update = {.op = DTLV_USEARCH_OP_ADD,
                                .key = &key_be,
                                .key_len = sizeof(key_be),
                                .payload = vec,
                                .payload_len = sizeof(vec),
                                .scalar_kind = usearch_scalar_f32_k,
                                .dimensions = 4};
  dtlv_usearch_txn_ctx *txn_ctx = NULL;
  assert(dtlv_usearch_stage_update(domain, txn, &update, &txn_ctx) == 0);
  assert(dtlv_usearch_apply_pending(txn_ctx) == 0);
  assert(mdb_txn_commit(txn) == 0);
  usearch_index_t index = dtlv_usearch_handle_index(handle);
  usearch_error_t err = NULL;
  assert(!usearch_contains(index, 77, &err));
  assert(err == NULL);
  assert(dtlv_usearch_publish_log(txn_ctx, 1) == 0);
  dtlv_usearch_txn_ctx_close(txn_ctx);
  index = dtlv_usearch_handle_index(handle);
  err = NULL;
  assert(usearch_contains(index, 77, &err));
  assert(err == NULL);
  dtlv_usearch_deactivate(handle);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_compaction_recover_pending(void) {
  TRACE_TEST("compaction_recover_pending");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);

  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "compact", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);

  float vec[4] = {1.f, 0.f, 0.f, 0.f};
  stage_domain_vector(domain, env, 101, vec, 4, "compact-stage", 1);
  dtlv_usearch_domain_close(domain);

  /* Inject compaction_pending manually to simulate crash before apply. */
  MDB_txn *txn = NULL;
  MDB_dbi meta;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  assert(open_domain_dbi(txn, "compact", "usearch-meta", &meta) == 0);
  uint8_t rec[16] = {0};
  rec[0] = 1; /* version */
  uint64_t upto_be = dtlv_to_be64(1);
  memcpy(rec + 8, &upto_be, sizeof(upto_be));
  MDB_val key = {.mv_size = strlen(META_KEY_COMPACTION_PENDING) + 1, .mv_data = (void *)META_KEY_COMPACTION_PENDING};
  MDB_val val = {.mv_size = sizeof(rec), .mv_data = rec};
  assert(mdb_put(txn, meta, &key, &val, 0) == 0);
  assert(mdb_txn_commit(txn) == 0);

  int found = meta_key_exists(env, "compact", META_KEY_COMPACTION_PENDING);
  assert(found == 1);

  /* Re-open domain to trigger compaction recovery. */
  domain = NULL;
  expect_domain_open(env, "compact", fs_path, &domain);
  dtlv_usearch_domain_close(domain);

  int delta_after = delta_count(env, "compact");
  assert(delta_after == 0);

  int found_flag = 0;
  uint64_t meta_val = 0;
  assert(meta_get_u64(env, "compact", META_KEY_COMPACTION_PENDING, &meta_val, &found_flag) == 0);
  assert(found_flag == 0);
  assert(meta_get_u64(env, "compact", META_KEY_COMPACTION_APPLIED, &meta_val, &found_flag) == 0 && found_flag == 1);
  assert(meta_val == 1);
  assert(meta_get_u64(env, "compact", META_KEY_LOG_TAIL_SEQ, &meta_val, &found_flag) == 0 && found_flag == 1);
  assert(meta_val == 1);

  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_wal_recover_replays_pending_log(void) {
  TRACE_TEST("wal_recover_replays_pending_log");
  char *root = create_temp_dir();
  assert(root != NULL);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "walrecover", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);
  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  float vec[4] = {9.f, 1.f, 2.f, 3.f};
  uint64_t key_be = dtlv_to_be64(88);
  dtlv_usearch_update update = {.op = DTLV_USEARCH_OP_ADD,
                                .key = &key_be,
                                .key_len = sizeof(key_be),
                                .payload = vec,
                                .payload_len = sizeof(vec),
                                .scalar_kind = usearch_scalar_f32_k,
                                .dimensions = 4};
  dtlv_usearch_txn_ctx *txn_ctx = NULL;
  assert(dtlv_usearch_stage_update(domain, txn, &update, &txn_ctx) == 0);
  assert(dtlv_usearch_apply_pending(txn_ctx) == 0);
  assert(mdb_txn_commit(txn) == 0);
  dtlv_usearch_txn_ctx_close(txn_ctx);
  char pending_path[PATH_MAX];
  snprintf(pending_path, sizeof(pending_path), "%s/pending", fs_path);
  assert(directory_has_suffix(pending_path, ".ulog") == 1);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  env = create_env(env_path);
  assert(env != NULL);
  expect_domain_open(env, "walrecover", fs_path, &domain);
  assert(directory_has_suffix(pending_path, ".ulog") == 0);
  assert(directory_has_suffix(pending_path, ".ulog.sealed") == 0);
  uint32_t ordinal = 0;
  int tail_found = 0;
  assert(meta_get_published_tail(env, "walrecover", &ordinal, &tail_found) == 0);
  assert(tail_found == 1);
  assert(ordinal == 1);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_usearch_probe_filesystem(void) {
  TRACE_TEST("test_usearch_probe_filesystem");
  char *root = create_temp_dir();
  assert(root);
  char file_path[PATH_MAX];
  snprintf(file_path, sizeof(file_path), "%s/probe.usearch", root);

  usearch_init_options_t opts;
  memset(&opts, 0, sizeof(opts));
  opts.metric_kind = usearch_metric_ip_k;
  opts.quantization = usearch_scalar_f32_k;
  opts.dimensions = 8;
  opts.connectivity = 13;
  opts.multi = true;
  usearch_error_t err = NULL;
  usearch_index_t index = usearch_init(&opts, &err);
  assert(index != NULL);
  assert(err == NULL);

  err = NULL;
  usearch_save(index, file_path, &err);
  assert(err == NULL);

  dtlv_usearch_format_info info;
  int rc = dtlv_usearch_probe_filesystem(file_path, &info);
  assert(rc == 0);
  assert(info.metric_kind == opts.metric_kind);
  assert(info.scalar_kind == opts.quantization);
  assert(info.dimensions == (uint32_t)opts.dimensions);
  assert(info.multi == opts.multi);
  assert(info.connectivity == 0);
  uint32_t expected_schema = parse_usearch_version_code(usearch_version());
  assert(expected_schema != 0);
  assert(info.schema_version == expected_schema);

  usearch_error_t free_err = NULL;
  usearch_free(index, &free_err);
  remove_tree(root);
  free(root);
}

static void test_reader_pin_lifecycle(void) {
  TRACE_TEST("test_reader_pin_lifecycle");
  char *root = create_temp_dir();
  assert(root);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env != NULL);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "pins", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);

  float vec[4] = {0.15f, 0.25f, 0.35f, 0.45f};
  stage_domain_vector(domain, env, 5, vec, 4, "pin-baseline", 1);
  uint64_t snapshot_seq = 0;
  uint64_t log_seq = 0;
  int found = 0;
  assert(meta_get_u64(env, "pins", META_KEY_SNAPSHOT_SEQ, &snapshot_seq, &found) == 0 && found == 1);
  assert(meta_get_u64(env, "pins", META_KEY_LOG_SEQ, &log_seq, &found) == 0 && found == 1);

  dtlv_uuid128 reader = {.hi = 0xA1B2C3D4u, .lo = 0x01020304u};
  int64_t now = test_now_ms();
  assert(dtlv_usearch_pin_handle(domain, &reader, snapshot_seq, log_seq, now + 5000) == 0);

  char pins_path[PATH_MAX];
  snprintf(pins_path, sizeof(pins_path), "%s/reader-pins.lock", fs_path);
  test_pin_record record;
  assert(load_pin_record(pins_path, &reader, &record, &found) == 0);
  assert(found == 1);
  assert(record.version == 1);
  assert(record.reader_uuid.hi == reader.hi && record.reader_uuid.lo == reader.lo);
  assert(record.snapshot_seq == snapshot_seq);
  assert(record.log_seq == log_seq);
  assert(record.expires_at_ms == now + 5000);

  int64_t refreshed_expires = now + 12000;
  assert(dtlv_usearch_touch_pin(domain, &reader, refreshed_expires) == 0);
  memset(&record, 0, sizeof(record));
  assert(load_pin_record(pins_path, &reader, &record, &found) == 0);
  assert(found == 1);
  assert(record.reader_uuid.hi == reader.hi && record.reader_uuid.lo == reader.lo);
  assert(record.expires_at_ms == refreshed_expires);

  assert(dtlv_usearch_release_pin(domain, &reader) == 0);
  assert(load_pin_record(pins_path, &reader, &record, &found) == 0);
  assert(found == 0);

  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_usearch_inspect_domain(void) {
  TRACE_TEST("test_usearch_inspect_domain");
  char *root = create_temp_dir();
  assert(root);
  char env_path[PATH_MAX];
  snprintf(env_path, sizeof(env_path), "%s/env", root);
  char fs_root[PATH_MAX];
  snprintf(fs_root, sizeof(fs_root), "%s/fs", root);
  assert(ensure_directory(fs_root) == 0);

  MDB_env *env = create_env(env_path);
  assert(env != NULL);

  dtlv_usearch_domain *domain = NULL;
  assert(dtlv_usearch_domain_open(env, "formats", fs_root, &domain) == 0);

  MDB_txn *txn = NULL;
  assert(mdb_txn_begin(env, NULL, 0, &txn) == 0);
  usearch_init_options_t opts;
  memset(&opts, 0, sizeof(opts));
  opts.metric_kind = usearch_metric_l2sq_k;
  opts.quantization = usearch_scalar_f16_k;
  opts.dimensions = 16;
  opts.connectivity = 32;
  opts.expansion_add = 64;
  opts.expansion_search = 32;
  opts.multi = true;
  assert(dtlv_usearch_store_init_options(domain, txn, &opts) == 0);
  assert(mdb_txn_commit(txn) == 0);

  dtlv_usearch_format_info info;
  assert(dtlv_usearch_inspect_domain(domain, NULL, &info) == 0);
  uint64_t schema_value = 0;
  int found = 0;
  assert(meta_get_u64(env, "formats", META_KEY_SCHEMA_VERSION, &schema_value, &found) == 0);
  assert(found == 1);
  assert(info.schema_version == (uint32_t)schema_value);
  assert(info.metric_kind == opts.metric_kind);
  assert(info.scalar_kind == opts.quantization);
  assert(info.dimensions == (uint32_t)opts.dimensions);
  assert(info.connectivity == (uint32_t)opts.connectivity);
  assert(info.multi == opts.multi);

  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static uint64_t fuzz_random_key(uint64_t key_space) {
  if (key_space == 0) return 0;
  return ((uint64_t)(rand() % (int)key_space)) + 1;
}

static void fuzz_random_vector(float *out, size_t dims) {
  if (!out) return;
  for (size_t i = 0; i < dims; ++i) {
    out[i] = (float)rand() / (float)RAND_MAX * 2.0f - 1.0f;
  }
}

static int fuzz_pick_active_key(const int *present, size_t max_keys) {
  if (!present || max_keys == 0) return -1;
  for (size_t attempt = 0; attempt < max_keys * 2; ++attempt) {
    size_t idx = (size_t)(rand() % (int)max_keys);
    if (present[idx]) return (int)(idx + 1);
  }
  for (size_t idx = 0; idx < max_keys; ++idx) {
    if (present[idx]) return (int)(idx + 1);
  }
  return -1;
}

static void test_vector_dataset_fuzz(void) {
  TRACE_TEST("test_vector_dataset_fuzz");
  srand(12345);
  enum {
    fuzz_vector_dims = 8,
    fuzz_key_space = 64,
    fuzz_ops = 200
  };
  char *root = create_temp_dir();
  assert(root);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "fuzz-domain", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  opts.dimensions = fuzz_vector_dims;
  persist_init_options(env, domain, &opts);
  dtlv_usearch_handle *handle = NULL;
  assert(dtlv_usearch_activate(domain, &handle) == 0);
  usearch_index_t index = dtlv_usearch_handle_index(handle);
  assert(index);

  float vectors[fuzz_key_space][fuzz_vector_dims];
  int present[fuzz_key_space];
  memset(present, 0, sizeof(present));
  size_t active_count = 0;

  for (size_t step = 0; step < fuzz_ops; ++step) {
    int op = rand() % 3;
    uint64_t key = fuzz_random_key(fuzz_key_space);
    if (op == 0) {
      float vec[fuzz_vector_dims];
      fuzz_random_vector(vec, fuzz_vector_dims);
      stage_domain_vector(domain, env, key, vec, fuzz_vector_dims, "fuzz-add", 1);
      memcpy(vectors[key - 1], vec, sizeof(vec));
      if (!present[key - 1]) active_count++;
      present[key - 1] = 1;
      usearch_error_t err = NULL;
      assert(usearch_contains(index, key, &err));
      assert(err == NULL);
    } else if (op == 1) {
      stage_domain_delete(domain, env, key, "fuzz-delete");
      if (present[key - 1]) {
        present[key - 1] = 0;
        if (active_count > 0) active_count--;
      }
      usearch_error_t err = NULL;
      assert(!usearch_contains(index, key, &err));
      assert(err == NULL);
    } else {
      if (active_count == 0) continue;
      int selected = fuzz_pick_active_key(present, fuzz_key_space);
      assert(selected > 0);
      usearch_key_t results[4] = {0};
      usearch_distance_t distances[4] = {0};
      usearch_error_t err = NULL;
      size_t found = usearch_search(index,
                                    vectors[selected - 1],
                                    usearch_scalar_f32_k,
                                    sizeof(results) / sizeof(results[0]),
                                    results,
                                    distances,
                                    &err);
      assert(err == NULL);
      assert(found >= 1);
      int seen = 0;
      for (size_t i = 0; i < found; ++i) {
        if (results[i] == (usearch_key_t)selected) {
          seen = 1;
          break;
        }
      }
      assert(seen);
    }
  }

  for (size_t i = 0; i < fuzz_key_space; ++i) {
    usearch_error_t err = NULL;
    int contained = usearch_contains(index, (usearch_key_t)(i + 1), &err);
    assert(err == NULL);
    assert((present[i] != 0) == contained);
  }

  dtlv_usearch_deactivate(handle);
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_multi_process_replay(void) {
  TRACE_TEST("test_multi_process_replay");
#ifdef _WIN32
  fprintf(stderr, "[dtlv_test] multi_process_replay skipped on Windows\n");
#else
  char *root = create_temp_dir();
  assert(root);
  char env_path[PATH_MAX];
  char fs_path[PATH_MAX];
  setup_paths(root, env_path, sizeof(env_path), fs_path, sizeof(fs_path));
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain = NULL;
  expect_domain_open(env, "domain-mproc", fs_path, &domain);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain, &opts);
  dtlv_usearch_domain_close(domain);
  domain = NULL;
  mdb_env_close(env);
  env = NULL;
  dtlv_run_child_process("writer", env_path, fs_path, dtlv_child_stage_vector, 0);
  dtlv_run_child_process("reader", env_path, fs_path, dtlv_child_verify_vector, 0);
  dtlv_run_child_process("crash-writer", env_path, fs_path, dtlv_child_crash_before_publish, 42);
  dtlv_run_child_process("reader-after-crash", env_path, fs_path, dtlv_child_verify_replayed_vector, 0);
  dtlv_run_child_process("checkpoint-crash", env_path, fs_path, dtlv_child_checkpoint_crash, 17);
  env = create_env(env_path);
  assert(env);
  domain = NULL;
  expect_domain_open(env, "checkpoint-crash", fs_path, &domain);
  assert(dtlv_usearch_checkpoint_recover(domain) == 0);
  uint64_t snapshot_seq = 0;
  int found = 0;
  assert(meta_get_u64(env, "checkpoint-crash", META_KEY_CHECKPOINT_PENDING, &snapshot_seq, &found) == 0);
  assert(found == 0);
  size_t chunks = snapshot_chunks_for_seq(env, "checkpoint-crash", 9);
  uint64_t meta_snapshot = 0;
  assert(meta_get_u64(env, "checkpoint-crash", "snapshot_seq", &meta_snapshot, &found) == 0);
  if (chunks > 0) {
    assert(meta_snapshot != 9);
  }
  dtlv_usearch_domain_close(domain);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
#endif
}

static void test_multi_domain_isolation(void) {
  TRACE_TEST("test_multi_domain_isolation");
  char *root = create_temp_dir();
  assert(root);
  char env_path[PATH_MAX];
  char fs_a[PATH_MAX];
  char fs_b[PATH_MAX];
  snprintf(env_path, sizeof(env_path), "%s/env", root);
  snprintf(fs_a, sizeof(fs_a), "%s/fsA", root);
  snprintf(fs_b, sizeof(fs_b), "%s/fsB", root);
  assert(ensure_directory(env_path) == 0);
  assert(ensure_directory(fs_a) == 0);
  assert(ensure_directory(fs_b) == 0);
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain_a = NULL;
  dtlv_usearch_domain *domain_b = NULL;
  expect_domain_open(env, "domain-a", fs_a, &domain_a);
  expect_domain_open(env, "domain-b", fs_b, &domain_b);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain_a, &opts);
  persist_init_options(env, domain_b, &opts);
  float vec_a[4] = {0.05f, 0.15f, 0.25f, 0.35f};
  float vec_b[4] = {0.6f, 0.7f, 0.8f, 0.9f};
  stage_domain_vector(domain_a, env, 11, vec_a, 4, "domain-a", 1);
  stage_domain_vector(domain_b, env, 22, vec_b, 4, "domain-b", 1);
  dtlv_usearch_handle *handle_a = NULL;
  dtlv_usearch_handle *handle_b = NULL;
  assert(dtlv_usearch_activate(domain_a, &handle_a) == 0);
  assert(dtlv_usearch_activate(domain_b, &handle_b) == 0);
  usearch_error_t err = NULL;
  usearch_index_t index_a = dtlv_usearch_handle_index(handle_a);
  usearch_index_t index_b = dtlv_usearch_handle_index(handle_b);
  assert(index_a && index_b);
  err = NULL;
  assert(usearch_contains(index_a, 11, &err));
  assert(err == NULL);
  err = NULL;
  assert(!usearch_contains(index_a, 22, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(index_b, 22, &err));
  assert(err == NULL);
  err = NULL;
  assert(!usearch_contains(index_b, 11, &err));
  assert(err == NULL);
  dtlv_usearch_deactivate(handle_a);
  dtlv_usearch_deactivate(handle_b);
  handle_a = NULL;
  handle_b = NULL;
  float vec_b2[4] = {1.1f, 1.2f, 1.3f, 1.4f};
  stage_domain_vector(domain_b, env, 33, vec_b2, 4, "domain-b-second", 1);
  assert(dtlv_usearch_activate(domain_a, &handle_a) == 0);
  assert(dtlv_usearch_activate(domain_b, &handle_b) == 0);
  index_a = dtlv_usearch_handle_index(handle_a);
  index_b = dtlv_usearch_handle_index(handle_b);
  err = NULL;
  assert(usearch_contains(index_a, 11, &err));
  assert(err == NULL);
  err = NULL;
  assert(!usearch_contains(index_a, 33, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(index_b, 22, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(index_b, 33, &err));
  assert(err == NULL);
  dtlv_usearch_deactivate(handle_a);
  dtlv_usearch_deactivate(handle_b);
  dtlv_usearch_domain_close(domain_a);
  dtlv_usearch_domain_close(domain_b);
  mdb_env_close(env);
  remove_tree(root);
  free(root);
}

static void test_multi_domain_wal_recovery(void) {
  TRACE_TEST("test_multi_domain_wal_recovery");
  char *root = create_temp_dir();
  assert(root);
  char env_path[PATH_MAX];
  char fs_a[PATH_MAX];
  char fs_b[PATH_MAX];
  snprintf(env_path, sizeof(env_path), "%s/env", root);
  snprintf(fs_a, sizeof(fs_a), "%s/fsA", root);
  snprintf(fs_b, sizeof(fs_b), "%s/fsB", root);
  assert(ensure_directory(env_path) == 0);
  assert(ensure_directory(fs_a) == 0);
  assert(ensure_directory(fs_b) == 0);
  MDB_env *env = create_env(env_path);
  assert(env);
  dtlv_usearch_domain *domain_a = NULL;
  dtlv_usearch_domain *domain_b = NULL;
  expect_domain_open(env, "wal-domain-a", fs_a, &domain_a);
  expect_domain_open(env, "wal-domain-b", fs_b, &domain_b);
  usearch_init_options_t opts;
  fill_default_init_options(&opts);
  persist_init_options(env, domain_a, &opts);
  persist_init_options(env, domain_b, &opts);
  float vec_a1[4] = {0.12f, 0.22f, 0.32f, 0.42f};
  float vec_a2[4] = {0.52f, 0.62f, 0.72f, 0.82f};
  float vec_b1[4] = {1.2f, 1.3f, 1.4f, 1.5f};
  float vec_b2[4] = {1.6f, 1.7f, 1.8f, 1.9f};
  stage_domain_vector(domain_a, env, 101, vec_a1, 4, "domain-a-wal-published", 1);
  stage_domain_vector(domain_b, env, 303, vec_b1, 4, "domain-b-wal-published", 1);
  stage_domain_vector(domain_a, env, 102, vec_a2, 4, "domain-a-wal-pending", 0);
  stage_domain_vector(domain_b, env, 304, vec_b2, 4, "domain-b-wal-pending", 0);
  char pending_a[PATH_MAX];
  char pending_b[PATH_MAX];
  snprintf(pending_a, sizeof(pending_a), "%s/pending", fs_a);
  snprintf(pending_b, sizeof(pending_b), "%s/pending", fs_b);
  assert(directory_has_suffix(pending_a, ".ulog") == 1);
  assert(directory_has_suffix(pending_b, ".ulog") == 1);
  dtlv_usearch_domain_close(domain_a);
  dtlv_usearch_domain_close(domain_b);
  mdb_env_close(env);
  env = NULL;
  domain_a = NULL;
  domain_b = NULL;

  env = create_env(env_path);
  assert(env);
  expect_domain_open(env, "wal-domain-a", fs_a, &domain_a);
  expect_domain_open(env, "wal-domain-b", fs_b, &domain_b);

  dtlv_usearch_handle *handle_a = NULL;
  dtlv_usearch_handle *handle_b = NULL;
  assert(dtlv_usearch_activate(domain_a, &handle_a) == 0);
  assert(dtlv_usearch_activate(domain_b, &handle_b) == 0);
  usearch_error_t err = NULL;
  usearch_index_t index_a = dtlv_usearch_handle_index(handle_a);
  usearch_index_t index_b = dtlv_usearch_handle_index(handle_b);
  assert(index_a && index_b);
  err = NULL;
  assert(usearch_contains(index_a, 101, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(index_a, 102, &err));
  assert(err == NULL);
  err = NULL;
  assert(!usearch_contains(index_a, 303, &err));
  assert(err == NULL);

  err = NULL;
  assert(usearch_contains(index_b, 303, &err));
  assert(err == NULL);
  err = NULL;
  assert(usearch_contains(index_b, 304, &err));
  assert(err == NULL);
  err = NULL;
  assert(!usearch_contains(index_b, 101, &err));
  assert(err == NULL);

  dtlv_usearch_deactivate(handle_a);
  dtlv_usearch_deactivate(handle_b);
  dtlv_usearch_domain_close(domain_a);
  dtlv_usearch_domain_close(domain_b);
  mdb_env_close(env);
  assert(directory_has_suffix(pending_a, ".ulog") == 0);
  assert(directory_has_suffix(pending_a, ".ulog.sealed") == 0);
  assert(directory_has_suffix(pending_b, ".ulog") == 0);
  assert(directory_has_suffix(pending_b, ".ulog.sealed") == 0);
  remove_tree(root);
  free(root);
}

int main(void) {
  if (getenv("DTLV_TRACE_TESTS")) {
    fprintf(stderr, "[dtlv_test] main start\n");
    fflush(stderr);
  }
  dtlv_run_test("checkpoint_finalize_roundtrip", test_checkpoint_finalize_roundtrip);
  dtlv_run_test("checkpoint_recover_writing", test_checkpoint_recover_writing);
  dtlv_run_test("checkpoint_mapfull_retains_pending", test_checkpoint_mapfull_retains_pending);
  dtlv_run_test("activation_replays_snapshot_and_delta", test_activation_replays_snapshot_and_delta);
  dtlv_run_test("handle_refresh_tracks_updates", test_handle_refresh_tracks_updates);
  dtlv_run_test("publish_updates_active_handles", test_publish_updates_active_handles);
  dtlv_run_test("compaction_recover_pending", test_compaction_recover_pending);
  dtlv_run_test("wal_recover_replays_pending_log", test_wal_recover_replays_pending_log);
  dtlv_run_test("test_usearch_probe_filesystem", test_usearch_probe_filesystem);
  dtlv_run_test("test_usearch_inspect_domain", test_usearch_inspect_domain);
  dtlv_run_test("test_vector_dataset_fuzz", test_vector_dataset_fuzz);
  dtlv_run_test("test_reader_pin_lifecycle", test_reader_pin_lifecycle);
  dtlv_run_test("test_multi_process_replay", test_multi_process_replay);
  dtlv_run_test("test_multi_domain_isolation", test_multi_domain_isolation);
  dtlv_run_test("test_multi_domain_wal_recovery", test_multi_domain_wal_recovery);
  printf("dtlv_usearch_checkpoint_test: ok\n");
  return 0;
}
