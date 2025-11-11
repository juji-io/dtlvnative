#ifndef DTLV_USEARCH_H
#define DTLV_USEARCH_H

#include <stddef.h>
#include <stdint.h>

#include "lmdb/libraries/liblmdb/lmdb.h"
#include "dtlv_usearch_wal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dtlv_usearch_domain dtlv_usearch_domain;
typedef struct dtlv_usearch_txn_ctx dtlv_usearch_txn_ctx;

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

int dtlv_usearch_domain_open(MDB_env *env,
                             const char *domain_name,
                             const char *filesystem_root,
                             dtlv_usearch_domain **domain_out);

void dtlv_usearch_domain_close(dtlv_usearch_domain *domain);

int dtlv_usearch_stage_update(dtlv_usearch_domain *domain,
                              MDB_txn *txn,
                              const dtlv_usearch_update *update,
                              dtlv_usearch_txn_ctx **ctx_inout);

int dtlv_usearch_apply_pending(dtlv_usearch_txn_ctx *ctx);

int dtlv_usearch_publish_log(dtlv_usearch_txn_ctx *ctx, int unlink_after_publish);

void dtlv_usearch_txn_ctx_abort(dtlv_usearch_txn_ctx *ctx);

void dtlv_usearch_txn_ctx_close(dtlv_usearch_txn_ctx *ctx);

#ifdef __cplusplus
}
#endif

#endif /* DTLV_USEARCH_H */
