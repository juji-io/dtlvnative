#include "dtlv_usearch.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dtlv_bytes.h"
#include "dtlv_crc32c.h"

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DTLV_INLINE_PAYLOAD_THRESHOLD 512

struct dtlv_usearch_domain {
  MDB_env *env;
  MDB_dbi meta_dbi;
  MDB_dbi delta_dbi;
  MDB_dbi blobs_dbi;
  char domain_name[256];
  char meta_name[256];
  char delta_name[256];
  char blobs_name[256];
  char filesystem_root[PATH_MAX];
  char pending_root[PATH_MAX];
  size_t inline_threshold;
};

struct dtlv_usearch_txn_ctx {
  dtlv_usearch_domain *domain;
  MDB_txn *txn;
  dtlv_usearch_wal_ctx *wal;
  uint64_t snapshot_seq;
  uint64_t log_seq_head;
  uint64_t next_blob_id;
  uint64_t last_log_seq;
  uint32_t frames_appended;
};

typedef struct {
  uint64_t blob_id;
  const void *payload;
  size_t payload_len;
  uint8_t scalar_kind;
  uint16_t dimensions;
} dtlv_blob_params;

static MDB_val dtlv_meta_key(const char *key) {
  MDB_val k;
  k.mv_size = strlen(key) + 1;
  k.mv_data = (void *)key;
  return k;
}

static int dtlv_meta_put_u64(MDB_txn *txn, MDB_dbi dbi, const char *key, uint64_t value) {
  uint64_t be = dtlv_to_be64(value);
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

static int dtlv_meta_put_bytes(MDB_txn *txn,
                               MDB_dbi dbi,
                               const char *key,
                               const void *buf,
                               size_t len) {
  MDB_val k = dtlv_meta_key(key);
  MDB_val v = {.mv_size = len, .mv_data = (void *)buf};
  return mdb_put(txn, dbi, &k, &v, 0);
}

static int dtlv_ensure_defaults(MDB_txn *txn, MDB_dbi meta_dbi) {
  uint64_t value;
  int rc = dtlv_meta_get_u64(txn, meta_dbi, "next_blob_id", 1, &value);
  if (rc != 0) return rc;
  rc = dtlv_meta_put_u64(txn, meta_dbi, "next_blob_id", value);
  if (rc != 0) return rc;
  rc = dtlv_meta_get_u64(txn, meta_dbi, "log_seq", 0, &value);
  if (rc != 0) return rc;
  rc = dtlv_meta_put_u64(txn, meta_dbi, "log_seq", value);
  if (rc != 0) return rc;
  rc = dtlv_meta_get_u64(txn, meta_dbi, "snapshot_seq", 0, &value);
  if (rc != 0) return rc;
  return dtlv_meta_put_u64(txn, meta_dbi, "snapshot_seq", value);
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
  domain->inline_threshold = DTLV_INLINE_PAYLOAD_THRESHOLD;
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
  int rc = dtlv_build_dbi_name(domain_name, "usearch-meta", domain->meta_name, sizeof(domain->meta_name));
  if (rc != 0) {
    free(domain);
    return rc;
  }
  rc = dtlv_build_dbi_name(domain_name, "usearch-delta", domain->delta_name, sizeof(domain->delta_name));
  if (rc != 0) {
    free(domain);
    return rc;
  }
  rc = dtlv_build_dbi_name(domain_name, "usearch-blobs", domain->blobs_name, sizeof(domain->blobs_name));
  if (rc != 0) {
    free(domain);
    return rc;
  }

  MDB_txn *txn = NULL;
  rc = mdb_txn_begin(env, NULL, 0, &txn);
  if (rc != 0) {
    free(domain);
    return rc;
  }
  rc = mdb_dbi_open(txn, domain->meta_name, MDB_CREATE, &domain->meta_dbi);
  if (rc == 0) rc = mdb_dbi_open(txn, domain->delta_name, MDB_CREATE, &domain->delta_dbi);
  if (rc == 0) rc = mdb_dbi_open(txn, domain->blobs_name, MDB_CREATE, &domain->blobs_dbi);
  if (rc == 0) rc = dtlv_ensure_defaults(txn, domain->meta_dbi);
  if (rc == 0) rc = mdb_txn_commit(txn);
  else mdb_txn_abort(txn);
  if (rc != 0) {
    free(domain);
    return rc;
  }
  *domain_out = domain;
  return 0;
}

void dtlv_usearch_domain_close(dtlv_usearch_domain *domain) {
  if (!domain) return;
  if (domain->env) {
    mdb_dbi_close(domain->env, domain->meta_dbi);
    mdb_dbi_close(domain->env, domain->delta_dbi);
    mdb_dbi_close(domain->env, domain->blobs_dbi);
  }
  free(domain);
}

static int dtlv_put_blob_record(dtlv_usearch_txn_ctx *ctx, const dtlv_blob_params *params) {
  uint32_t refcount = dtlv_to_be32(1u);
  uint64_t zero64 = dtlv_to_be64(0);
  uint64_t be_blob_id = dtlv_to_be64(params->blob_id);
  uint32_t byte_length = dtlv_to_be32((uint32_t)params->payload_len);
  uint64_t owner_seq = dtlv_to_be64(0);
  uint32_t checksum = dtlv_to_be32(dtlv_crc32c(params->payload, params->payload_len));

  size_t value_len = sizeof(uint8_t) * 2 + sizeof(uint16_t) + sizeof(uint32_t) +
                     sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t) +
                     sizeof(uint32_t) + params->payload_len;
  uint8_t *buffer = malloc(value_len);
  if (!buffer) return ENOMEM;
  size_t offset = 0;
  buffer[offset++] = 1;  // version
  buffer[offset++] = params->scalar_kind;
  uint16_t dims_be = dtlv_to_be16(params->dimensions);
  memcpy(buffer + offset, &dims_be, sizeof(dims_be));
  offset += sizeof(dims_be);
  memcpy(buffer + offset, &byte_length, sizeof(byte_length));
  offset += sizeof(byte_length);
  memcpy(buffer + offset, &refcount, sizeof(refcount));
  offset += sizeof(refcount);
  memcpy(buffer + offset, &zero64, sizeof(zero64));
  offset += sizeof(zero64);
  memcpy(buffer + offset, &owner_seq, sizeof(owner_seq));
  offset += sizeof(owner_seq);
  memcpy(buffer + offset, &checksum, sizeof(checksum));
  offset += sizeof(checksum);
  memcpy(buffer + offset, params->payload, params->payload_len);

  MDB_val key = {.mv_size = sizeof(be_blob_id), .mv_data = &be_blob_id};
  MDB_val val = {.mv_size = value_len, .mv_data = buffer};
  int rc = mdb_put(ctx->txn, ctx->domain->blobs_dbi, &key, &val, 0);
  free(buffer);
  return rc;
}

static int dtlv_encode_delta_payload(const dtlv_usearch_txn_ctx *ctx,
                                     const dtlv_usearch_update *update,
                                     uint64_t blob_id,
                                     uint32_t ordinal,
                                     uint8_t **out_buf,
                                     size_t *out_len) {
  if (update->key_len > 255) return EINVAL;
  bool inline_payload = update->payload && update->payload_len <= ctx->domain->inline_threshold;
  size_t inline_len = inline_payload ? update->payload_len : 0;
  const dtlv_uuid128 *token = dtlv_usearch_wal_token(ctx->wal);
  if (!token) return EINVAL;
  size_t header_len = 40;
  size_t total_len = header_len + update->key_len + inline_len;
  uint8_t *buffer = malloc(total_len ? total_len : header_len);
  if (!buffer) return ENOMEM;
  memset(buffer, 0, header_len);
  buffer[0] = 1;
  buffer[1] = (uint8_t)update->op;
  buffer[2] = (uint8_t)update->key_len;
  buffer[3] = inline_payload ? 0x1 : 0x0;
  uint32_t be32 = dtlv_to_be32(ordinal);
  memcpy(buffer + 4, &be32, sizeof(be32));
  uint64_t behi = dtlv_to_be64(token->hi);
  memcpy(buffer + 8, &behi, sizeof(behi));
  uint64_t belo = dtlv_to_be64(token->lo);
  memcpy(buffer + 16, &belo, sizeof(belo));
  uint64_t be_blob = dtlv_to_be64(blob_id);
  memcpy(buffer + 24, &be_blob, sizeof(be_blob));
  uint32_t payload_len_be = dtlv_to_be32((uint32_t)update->payload_len);
  memcpy(buffer + 32, &payload_len_be, sizeof(payload_len_be));
  if (update->key_len && update->key) {
    memcpy(buffer + header_len, update->key, update->key_len);
  }
  if (inline_len) {
    memcpy(buffer + header_len + update->key_len, update->payload, inline_len);
  }
  uint32_t crc = dtlv_crc32c(buffer + header_len, update->key_len + inline_len);
  uint32_t crc_be = dtlv_to_be32(crc);
  memcpy(buffer + 36, &crc_be, sizeof(crc_be));
  *out_buf = buffer;
  *out_len = header_len + update->key_len + inline_len;
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
    int rc = dtlv_meta_get_u64(txn, domain->meta_dbi, "snapshot_seq", 0, &ctx->snapshot_seq);
    if (rc == 0) rc = dtlv_meta_get_u64(txn, domain->meta_dbi, "log_seq", 0, &ctx->log_seq_head);
    if (rc == 0) rc = dtlv_meta_get_u64(txn, domain->meta_dbi, "next_blob_id", 1, &ctx->next_blob_id);
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

  uint64_t blob_id = 0;
  if (update->payload && update->payload_len > 0) {
    blob_id = ctx->next_blob_id++;
    dtlv_blob_params params = {.blob_id = blob_id,
                               .payload = update->payload,
                               .payload_len = update->payload_len,
                               .scalar_kind = update->scalar_kind,
                               .dimensions = update->dimensions};
    int rc = dtlv_put_blob_record(ctx, &params);
    if (rc != 0) return rc;
    rc = dtlv_meta_put_u64(txn, domain->meta_dbi, "next_blob_id", ctx->next_blob_id);
    if (rc != 0) return rc;
  }

  uint8_t *delta_buf = NULL;
  size_t delta_len = 0;
  uint32_t ordinal = ctx->frames_appended + 1;
  int rc = dtlv_encode_delta_payload(ctx, update, blob_id, ordinal, &delta_buf, &delta_len);
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
  rc = dtlv_meta_put_u64(txn, domain->meta_dbi, "log_seq", ctx->last_log_seq);
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
                             "sealed_log_seq",
                             &record,
                             sizeof(record));
}

int dtlv_usearch_publish_log(dtlv_usearch_txn_ctx *ctx, int unlink_after_publish) {
  if (!ctx || !ctx->wal) return EINVAL;
  if (ctx->frames_appended == 0) return EINVAL;
  int rc = dtlv_usearch_wal_mark_ready(ctx->wal, unlink_after_publish);
  if (rc != 0) return rc;
  const dtlv_uuid128 *token = dtlv_usearch_wal_token(ctx->wal);
  if (!token) return EINVAL;
  MDB_txn *txn = NULL;
  rc = mdb_txn_begin(ctx->domain->env, NULL, 0, &txn);
  if (rc != 0) return rc;
  struct {
    uint64_t token_hi;
    uint64_t token_lo;
    uint32_t ordinal;
    uint32_t reserved;
  } tail;
  tail.token_hi = dtlv_to_be64(token->hi);
  tail.token_lo = dtlv_to_be64(token->lo);
  tail.ordinal = dtlv_to_be32(ctx->frames_appended);
  tail.reserved = 0;
  rc = dtlv_meta_put_bytes(txn,
                           ctx->domain->meta_dbi,
                           "published_log_tail",
                           &tail,
                           sizeof(tail));
  if (rc == 0) rc = mdb_txn_commit(txn);
  else mdb_txn_abort(txn);
  return rc;
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
