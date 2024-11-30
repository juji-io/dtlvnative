#include "lmdb/libraries/liblmdb/lmdb.h"

#ifdef __cplusplus
extern "C" {
#endif

  int dtlv_cmp_memn(const MDB_val *a, const MDB_val *b);

  int dtlv_set_comparator(MDB_txn *txn, int dbi);

  int dtlv_set_dupsort_comparator(MDB_txn *txn, int dbi);

#ifdef __cplusplus
}
#endif
