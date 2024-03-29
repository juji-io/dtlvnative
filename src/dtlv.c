#include <stdlib.h>
#include <string.h>
#include "dtlv.h"

#ifdef _WIN32
#include "unistd.h"
#endif


int dtlv_cmp_memn(const MDB_val *a, const MDB_val *b) {

  if (a == b) return 0;

	int diff;
	ssize_t len_diff;
	unsigned int len;

	len = a->mv_size;
	len_diff = (ssize_t) a->mv_size - (ssize_t) b->mv_size;
	if (len_diff > 0) {
		len = b->mv_size;
	}

  diff = memcmp(a->mv_data, b->mv_data, len);

	return diff ? diff : len_diff;
}

int dtlv_set_comparator(MDB_txn *txn, int dbi) {
  MDB_cmp_func *fp = dtlv_cmp_memn;

  return mdb_set_compare(txn, dbi, fp);
}

int dtlv_set_dupsort_comparator(MDB_txn *txn, int dbi) {
  MDB_cmp_func *fp = dtlv_cmp_memn;

  return mdb_set_dupsort(txn, dbi, fp);
}
