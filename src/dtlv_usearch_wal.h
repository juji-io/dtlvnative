#ifndef DTLV_USEARCH_WAL_H
#define DTLV_USEARCH_WAL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DTLV_ULOG_MAGIC "DTLVULOG"
#define DTLV_ULOG_VERSION 1
#define DTLV_ULOG_TOKEN_HEX 32

typedef struct {
  uint64_t hi;
  uint64_t lo;
} dtlv_uuid128;

typedef enum {
  DTLV_ULOG_STATE_WRITING = 0,
  DTLV_ULOG_STATE_SEALED = 1,
  DTLV_ULOG_STATE_READY_FOR_PUBLISH = 2
} dtlv_ulog_state;

typedef struct dtlv_usearch_wal_ctx dtlv_usearch_wal_ctx;

typedef struct {
  char magic[8];
  uint8_t version;
  uint8_t state;
  uint16_t header_len;
  uint64_t snapshot_seq_base;
  uint64_t log_seq_hint;
  uint64_t txn_token_hi;
  uint64_t txn_token_lo;
  uint32_t frame_count;
  uint32_t checksum;
} dtlv_ulog_header_v1;

typedef struct {
  uint32_t ordinal;
  uint32_t delta_bytes;
  uint32_t checksum;
} dtlv_ulog_frame_prefix_v1;

int dtlv_usearch_wal_open(const char *domain_root,
                          uint64_t snapshot_seq_base,
                          uint64_t log_seq_hint,
                          dtlv_usearch_wal_ctx **ctx_out);

int dtlv_usearch_wal_append(dtlv_usearch_wal_ctx *ctx,
                            const void *delta_payload,
                            size_t delta_bytes);

int dtlv_usearch_wal_seal(dtlv_usearch_wal_ctx *ctx);

int dtlv_usearch_wal_mark_ready(dtlv_usearch_wal_ctx *ctx);

void dtlv_usearch_wal_close(dtlv_usearch_wal_ctx *ctx, int best_effort_delete);

const dtlv_uuid128 *dtlv_usearch_wal_token(const dtlv_usearch_wal_ctx *ctx);

uint32_t dtlv_usearch_wal_frame_count(const dtlv_usearch_wal_ctx *ctx);

const char *dtlv_usearch_wal_open_path(const dtlv_usearch_wal_ctx *ctx);

const char *dtlv_usearch_wal_sealed_path(const dtlv_usearch_wal_ctx *ctx);

const char *dtlv_usearch_wal_ready_path(const dtlv_usearch_wal_ctx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* DTLV_USEARCH_WAL_H */
