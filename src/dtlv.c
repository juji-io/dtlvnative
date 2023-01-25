#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "dtlv.h"

#ifdef _WIN32
#include "unistd.h"
#endif

int dtlv_memcmp(const void *a, const void *b, int n) {

# if defined(INTS_NEED_ALIGNED)
  intptr_t lp = (intptr_t)a;
  intptr_t rp = (intptr_t)b;
  if (( lp | rp ) & 0x3 ) {
    return memcmp(a, b, n);
  }
# endif

  uint64_t *li = (uint64_t *)a;
  uint64_t *ri = (uint64_t *)b;
  int oct_len = n >> 3;
  int rem_len = n & (0x7);

  int ii;
  for (ii=0; ii < oct_len; ii++) {
    uint64_t lc = *li++;
    uint64_t rc = *ri++;
    if (lc != rc) {
# if __BYTE_ORDER == __BIG_ENDIAN
      return lc < rc ? -1 : 1;
# else
      li-=1; ri-=1;
      rem_len = 8;
      break;
# endif
    }
  }

  unsigned char *l = (unsigned char *)li;
  unsigned char *r = (unsigned char *)ri;
  for (ii=0; ii < rem_len; ii++) {
    unsigned char lc = *l++;
    unsigned char rc = *r++;
    if ( lc != rc ) {
      return lc < rc ? -1 : 1;
    }
  }

  return 0;
}

int dtlv_cmp_memn(const MDB_val *a, const MDB_val *b) {
  if (a==b) return 0;

	int diff;
	ssize_t len_diff;
	unsigned int len;

	len = a->mv_size;
	len_diff = (ssize_t) a->mv_size - (ssize_t) b->mv_size;
	if (len_diff > 0) {
		len = b->mv_size;
	}

  diff = dtlv_memcmp(a->mv_data, b->mv_data, len);

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
