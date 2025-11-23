#ifndef DTLV_USEARCH_H
#define DTLV_USEARCH_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "lmdb/libraries/liblmdb/lmdb.h"
#include "usearch/c/usearch.h"
#include "dtlv_usearch_wal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dtlv_usearch_domain dtlv_usearch_domain;
typedef struct dtlv_usearch_txn_ctx dtlv_usearch_txn_ctx;
typedef struct dtlv_usearch_handle dtlv_usearch_handle;

typedef enum {
  DTLV_USEARCH_OP_ADD = 0,
  DTLV_USEARCH_OP_REPLACE = 1,
  DTLV_USEARCH_OP_DELETE = 2
} dtlv_usearch_op;

typedef struct {
  dtlv_usearch_op op;
  const void *key;
  size_t key_len;
  const void *payload;
  size_t payload_len;
  uint8_t scalar_kind;
  uint16_t dimensions;
} dtlv_usearch_update;

typedef struct dtlv_usearch_format_info {
  uint32_t schema_version;
  usearch_metric_kind_t metric_kind;
  usearch_scalar_kind_t scalar_kind;
  uint32_t dimensions;
  uint32_t connectivity;
  bool multi;
  uint8_t reserved[16];
} dtlv_usearch_format_info;

int dtlv_usearch_domain_open(MDB_env *env,
                             const char *domain_name,
                             const char *filesystem_root,
                             dtlv_usearch_domain **domain_out);

void dtlv_usearch_domain_close(dtlv_usearch_domain *domain);

int dtlv_usearch_activate(dtlv_usearch_domain *domain, dtlv_usearch_handle **handle_out);

void dtlv_usearch_deactivate(dtlv_usearch_handle *handle);

usearch_index_t dtlv_usearch_handle_index(const dtlv_usearch_handle *handle);

int dtlv_usearch_refresh(dtlv_usearch_handle *handle, MDB_txn *txn);

int dtlv_usearch_stage_update(dtlv_usearch_domain *domain,
                              MDB_txn *txn,
                              const dtlv_usearch_update *update,
                              dtlv_usearch_txn_ctx **ctx_inout);

int dtlv_usearch_store_init_options(dtlv_usearch_domain *domain,
                                    MDB_txn *txn,
                                    const usearch_init_options_t *opts);

int dtlv_usearch_load_init_options(dtlv_usearch_domain *domain,
                                   MDB_txn *txn,
                                   usearch_init_options_t *opts,
                                   int *found);

int dtlv_usearch_checkpoint_write_snapshot(dtlv_usearch_domain *domain,
                                           usearch_index_t index,
                                           uint64_t snapshot_seq,
                                           const dtlv_uuid128 *writer_uuid,
                                           size_t *chunk_count_out);

int dtlv_usearch_checkpoint_finalize(dtlv_usearch_domain *domain,
                                     uint64_t snapshot_seq,
                                     uint64_t prune_log_seq);

int dtlv_usearch_checkpoint_recover(dtlv_usearch_domain *domain);

int dtlv_usearch_apply_pending(dtlv_usearch_txn_ctx *ctx);

int dtlv_usearch_publish_log(dtlv_usearch_txn_ctx *ctx, int unlink_after_publish);

int dtlv_usearch_pin_handle(dtlv_usearch_domain *domain,
                            const dtlv_uuid128 *reader_uuid,
                            uint64_t snapshot_seq,
                            uint64_t log_seq,
                            int64_t expires_at_ms);

int dtlv_usearch_touch_pin(dtlv_usearch_domain *domain,
                           const dtlv_uuid128 *reader_uuid,
                           int64_t expires_at_ms);

int dtlv_usearch_release_pin(dtlv_usearch_domain *domain,
                             const dtlv_uuid128 *reader_uuid);

void dtlv_usearch_txn_ctx_abort(dtlv_usearch_txn_ctx *ctx);

void dtlv_usearch_txn_ctx_close(dtlv_usearch_txn_ctx *ctx);

int dtlv_usearch_probe_filesystem(const char *path, dtlv_usearch_format_info *info);

int dtlv_usearch_inspect_domain(dtlv_usearch_domain *domain,
                                MDB_txn *txn,
                                dtlv_usearch_format_info *info);

int dtlv_usearch_set_checkpoint_chunk_batch(dtlv_usearch_domain *domain, uint32_t batch);

int dtlv_usearch_get_checkpoint_chunk_batch(dtlv_usearch_domain *domain, uint32_t *batch_out);

#ifdef __cplusplus
}
#endif

#endif /* DTLV_USEARCH_H */
