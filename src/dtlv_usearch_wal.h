#ifndef DTLV_USEARCH_WAL_H
#define DTLV_USEARCH_WAL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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

int dtlv_usearch_wal_open(const char *domain_root,
                          uint64_t snapshot_seq_base,
                          uint64_t log_seq_hint,
                          dtlv_usearch_wal_ctx **ctx_out);

int dtlv_usearch_wal_append(dtlv_usearch_wal_ctx *ctx,
                            const void *delta_payload,
                            size_t delta_bytes);

int dtlv_usearch_wal_seal(dtlv_usearch_wal_ctx *ctx);

int dtlv_usearch_wal_mark_ready(dtlv_usearch_wal_ctx *ctx,
                                int unlink_after_publish);

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
