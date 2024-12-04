#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "dtlv.h"

#ifdef _WIN32
#include "win32/unistd.h"
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

void val_in(MDB_val *this, MDB_val *other) {
  this->mv_size = other->mv_size;
  this->mv_data = other->mv_data;
}

struct dtlv_key_iter {
  MDB_cursor *cur;
  MDB_val *key;
  MDB_val *val;
  int forward;
  int start;
  int end;
  MDB_val *start_key;
  MDB_val *end_key;
  int started;
};

int dtlv_key_iter_create(dtlv_key_iter **iter, MDB_cursor *cur,
                         MDB_val *key, MDB_val *val,
                         int forward, int start, int end,
                         MDB_val *start_key, MDB_val *end_key) {
  dtlv_key_iter *i;
  i = calloc(1, sizeof(struct dtlv_key_iter));
  if (!i) return ENOMEM;

  i->cur = cur;
  i->key = key;
  i->val = val;
  i->forward = forward;
  i->start = start;
  i->end = end;
  i->start_key = start_key;
  i->end_key = end_key;

  i->started = DTLV_FALSE;

  *iter = i;
  return MDB_SUCCESS;
}

int key_continue(dtlv_key_iter *iter);
int key_continue_back(dtlv_key_iter *iter);
int key_check(dtlv_key_iter *iter, int op);
int key_check_back(dtlv_key_iter *iter, int op);

int key_init(dtlv_key_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->start == DTLV_FALSE)
          && (dtlv_cmp_memn(iter->key, iter->start_key) == 0))
        return key_check(iter, MDB_NEXT_NODUP);
      else return key_continue(iter);
    } else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    else return rc;
  } else return key_check(iter, MDB_FIRST);
}

int key_init_back(dtlv_key_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->start == DTLV_TRUE)
          && (dtlv_cmp_memn(iter->key, iter->start_key) == 0))
        return key_continue_back(iter);
      else return key_check_back(iter, MDB_PREV_NODUP);
    } else if (rc == MDB_NOTFOUND) return key_check_back(iter, MDB_LAST);
    else return rc;
  } else return key_check_back(iter, MDB_LAST);
}

int key_continue(dtlv_key_iter *iter) {
  if (iter->end_key) {
    int r = dtlv_cmp_memn(iter->key, iter->end_key);
    if (r == 0) return iter->end;
    else if (r > 0) return DTLV_FALSE;
    else return DTLV_TRUE;
  } else return DTLV_TRUE;
}

int key_continue_back(dtlv_key_iter *iter) {
  if (iter->end_key) {
    int r = dtlv_cmp_memn(iter->key, iter->end_key);
    if (r == 0) return iter->end;
    else if (r > 0) return DTLV_TRUE;
    else return DTLV_FALSE;
  } else return DTLV_TRUE;
}

int key_check(dtlv_key_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return key_continue(iter);
  else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  else return rc;
}

int key_check_back(dtlv_key_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return key_continue_back(iter);
  else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  else return rc;
}

int key_advance(dtlv_key_iter *iter) {
  if (iter->forward == DTLV_TRUE) return key_check(iter, MDB_NEXT_NODUP);
  else return key_check_back(iter, MDB_PREV_NODUP);
}

int key_init_k(dtlv_key_iter *iter) {
  iter->started = DTLV_TRUE;

  if (iter->forward == DTLV_TRUE) return key_init(iter);
  else return key_init_back(iter);
}

int dtlv_key_iter_has_next(dtlv_key_iter *iter) {
  if (iter->started == DTLV_TRUE) return key_advance(iter);
  else return key_init_k(iter);
}

void dtlv_key_iter_destroy(dtlv_key_iter *iter) {
  if (iter) free(iter);
}

struct dtlv_list_iter {
  MDB_cursor *cur;
  MDB_val *key;
  MDB_val *val;
  int kforward;
  int kstart;
  int kend;
  MDB_val *start_key;
  MDB_val *end_key;
  int vforward;
  int vstart;
  int vend;
  MDB_val *start_val;
  MDB_val *end_val;
  int started;
  int key_ended;
};

int dtlv_list_iter_create(dtlv_list_iter **iter, MDB_cursor *cur,
                          MDB_val *key, MDB_val *val,
                          int kforward, int kstart, int kend,
                          MDB_val *start_key, MDB_val *end_key,
                          int vforward, int vstart, int vend,
                          MDB_val *start_val, MDB_val *end_val) {

  dtlv_list_iter *i;
  i = calloc(1, sizeof(struct dtlv_list_iter));
  if (!i) return ENOMEM;

  i->cur = cur;
  i->key = key;
  i->val = val;
  i->kforward = kforward;
  i->kstart = kstart;
  i->kend = kend;
  i->start_key = start_key;
  i->end_key = end_key;
  i->vforward = vforward;
  i->vstart = vstart;
  i->vend = vend;
  i->start_val = start_val;
  i->end_val = end_val;

  i->started = DTLV_FALSE;
  i->key_ended = DTLV_FALSE;

  *iter = i;
  return MDB_SUCCESS;
}

int list_key_continue(dtlv_list_iter *iter);
int list_key_continue_back(dtlv_list_iter *iter);
int list_check_key(dtlv_list_iter *iter, int op);
int list_check_key_back(dtlv_list_iter *iter, int op);
int list_val_continue(dtlv_list_iter *iter);
int list_val_continue_back(dtlv_list_iter *iter);
int list_check_val(dtlv_list_iter *iter, int op);
int list_check_val_back(dtlv_list_iter *iter, int op);
int list_advance_key(dtlv_list_iter *iter);

int list_init_key(dtlv_list_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->kstart == DTLV_FALSE)
          && (dtlv_cmp_memn(iter->key, iter->start_key) == 0))
        return list_check_key(iter, MDB_NEXT_NODUP);
      else return list_key_continue(iter);
    } else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    else return rc;
  } else return list_check_key(iter, MDB_FIRST);
}

int list_init_key_back(dtlv_list_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->kstart == DTLV_TRUE)
          && (dtlv_cmp_memn(iter->key, iter->start_key) == 0))
        return list_key_continue_back(iter);
      else return list_check_key_back(iter, MDB_PREV_NODUP);
    } else if (rc == MDB_NOTFOUND) return list_check_key_back(iter, MDB_LAST);
    else return rc;
  } else return list_check_key_back(iter, MDB_LAST);
}

int list_init_val(dtlv_list_iter *iter) {
  if (iter->start_val) {
    val_in(iter->val, iter->start_val);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_GET_BOTH_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->vstart == DTLV_FALSE)
          && (dtlv_cmp_memn(iter->val, iter->start_val) == 0))
        return list_check_val(iter, MDB_NEXT_DUP);
      else return list_val_continue(iter);
    } else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    else return rc;
  } else return list_check_val(iter, MDB_FIRST_DUP);
}

int list_init_val_back(dtlv_list_iter *iter) {
  if (iter->start_val) {
    val_in(iter->val, iter->start_val);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_GET_BOTH_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->vstart == DTLV_TRUE)
          && (dtlv_cmp_memn(iter->val, iter->start_val) == 0))
        return list_val_continue_back(iter);
      else return list_check_val_back(iter, MDB_PREV_DUP);
    } else if (rc == MDB_NOTFOUND)
      return list_check_val_back(iter, MDB_LAST_DUP);
    else return rc;
  } else return list_check_val_back(iter, MDB_LAST_DUP);
}

int list_key_end(dtlv_list_iter *iter) {
  iter->key_ended = DTLV_TRUE;
  return DTLV_FALSE;
}

int list_val_end(dtlv_list_iter *iter) {
  if (iter->key_ended == DTLV_TRUE) return DTLV_FALSE;
  else return list_advance_key(iter);
}

int list_key_continue(dtlv_list_iter *iter) {
  if (iter->end_key) {
    int r = dtlv_cmp_memn(iter->key, iter->end_key);
    if (r == 0) {
      iter->key_ended = DTLV_TRUE;
      return iter->kend;
    } else if (r > 0) return list_key_end(iter);
    else return DTLV_TRUE;
  } else return DTLV_TRUE;
}

int list_key_continue_back(dtlv_list_iter *iter) {
  if (iter->end_key) {
    int r = dtlv_cmp_memn(iter->key, iter->end_key);
    if (r == 0) {
      iter->key_ended = DTLV_TRUE;
      return iter->kend;
    } else if (r > 0) return DTLV_TRUE;
    else return list_key_end(iter);
  } else return DTLV_TRUE;
}

int list_check_key(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_key_continue(iter);
  else if (rc == MDB_NOTFOUND) return list_key_end(iter);
  else return rc;
}

int list_check_key_back(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_key_continue_back(iter);
  else if (rc == MDB_NOTFOUND) return list_key_end(iter);
  else return rc;
}

int list_advance_key(dtlv_list_iter *iter) {
  int kpass;
  if (iter->kforward == DTLV_TRUE)
    kpass = list_check_key(iter, MDB_NEXT_NODUP);
  else kpass = list_check_key_back(iter, MDB_PREV_NODUP);

  int vpass = DTLV_FALSE;
  if (kpass == DTLV_TRUE) {
    if (iter->vforward == DTLV_TRUE) vpass = list_init_val(iter);
    else vpass = list_init_val_back(iter);
  }

  if (vpass == DTLV_TRUE) return DTLV_TRUE;

  if (iter->key_ended == DTLV_TRUE) return DTLV_FALSE;
  else return list_advance_key(iter);
}

int list_init_kv(dtlv_list_iter *iter) {
  iter->started = DTLV_TRUE;

  int kpass;
  if (iter->kforward == DTLV_TRUE) kpass = list_init_key(iter);
  else kpass = list_init_key_back(iter);

  int vpass = DTLV_FALSE;
  if (kpass == DTLV_TRUE) {
    if (iter->vforward == DTLV_TRUE) vpass = list_init_val(iter);
    else vpass = list_init_val_back(iter);
  }

  if (vpass == DTLV_TRUE) return DTLV_TRUE;

  return list_advance_key(iter);
}

int list_val_continue(dtlv_list_iter *iter) {
  if (iter->end_val) {
    int r = dtlv_cmp_memn(iter->val, iter->end_val);
    if (r == 0) {
      if (iter->vend == DTLV_TRUE) return DTLV_TRUE;
      else return list_val_end(iter);
    } else if (r > 0) return list_val_end(iter);
    else return DTLV_TRUE;
  } else return DTLV_TRUE;
}

int list_val_continue_back(dtlv_list_iter *iter) {
  if (iter->end_val) {
    int r = dtlv_cmp_memn(iter->val, iter->end_val);
    if (r == 0) {
      if (iter->vend == DTLV_TRUE) return DTLV_TRUE;
      else return list_val_end(iter);
    } else if (r > 0) return DTLV_TRUE;
    else return list_val_end(iter);
  } else return DTLV_TRUE;
}

int list_check_val(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_val_continue(iter);
  else if (rc == MDB_NOTFOUND) return list_val_end(iter);
  else return rc;
}

int list_check_val_back(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_val_continue_back(iter);
  else if (rc == MDB_NOTFOUND) return list_val_end(iter);
  else return rc;
}

int list_advance_val(dtlv_list_iter *iter) {
  return list_check_val(iter, MDB_NEXT_DUP);
}

int list_advance_val_back(dtlv_list_iter *iter) {
  return list_check_val_back(iter, MDB_PREV_DUP);
}

int dtlv_list_iter_has_next(dtlv_list_iter *iter) {
  if (iter->started == DTLV_TRUE) {
    if (iter->vforward == DTLV_TRUE) return list_advance_val(iter);
    else return list_advance_val_back(iter);
  } else return list_init_kv(iter);
}

void dtlv_list_iter_destroy(dtlv_list_iter *iter) {
  if (iter) free(iter);
}

struct dtlv_list_val_iter {
  MDB_cursor *cur;
  MDB_val *key;
  MDB_val *val;
  int vstart;
  int vend;
  MDB_val *start_val;
  MDB_val *end_val;
};

int dtlv_list_val_iter_create(dtlv_list_val_iter **iter, MDB_cursor *cur,
                              MDB_val *key, MDB_val *val,
                              int vstart, int vend,
                              MDB_val *start_val,
                              MDB_val *end_val) {
  dtlv_list_val_iter *i;
  i = calloc(1, sizeof(struct dtlv_list_val_iter));
  if (!i) return ENOMEM;

  i->cur = cur;
  i->key = key;
  i->val = val;
  i->vstart = vstart;
  i->vend = vend;
  i->start_val = start_val;
  i->end_val = end_val;

  *iter = i;
  return MDB_SUCCESS;
}

int list_val_val_continue(dtlv_list_val_iter *iter);
int list_val_check_val(dtlv_list_val_iter *iter, int op);

int list_val_init_val(dtlv_list_val_iter *iter) {
  if (iter->start_val) {
    val_in(iter->val, iter->start_val);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_GET_BOTH_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->vstart == DTLV_FALSE)
          && (dtlv_cmp_memn(iter->val, iter->start_val) == 0))
        return list_val_check_val(iter, MDB_NEXT_DUP);
      else return list_val_val_continue(iter);
    } else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    else return rc;
  } else {
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) return list_val_check_val(iter, MDB_FIRST_DUP);
    else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    else return rc;
  }
}

int list_val_val_continue(dtlv_list_val_iter *iter) {
  if (iter->end_val) {
    int r = dtlv_cmp_memn(iter->val, iter->end_val);
    if (r == 0) {
      if (iter->vend == DTLV_TRUE) return DTLV_TRUE;
      else return DTLV_FALSE;
    } else if (r > 0) return DTLV_FALSE;
    else return DTLV_TRUE;
  } else return DTLV_TRUE;
}

int list_val_check_val(dtlv_list_val_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_val_val_continue(iter);
  else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  else return rc;
}

int list_val_advance_val(dtlv_list_val_iter *iter) {
  return list_val_check_val(iter, MDB_NEXT_DUP);
}

int dtlv_list_val_iter_seek(dtlv_list_val_iter *iter, MDB_val *k) {
  val_in(iter->key, k);
  return list_val_init_val(iter);
}

int dtlv_list_val_iter_has_next(dtlv_list_val_iter *iter) {
  return list_val_advance_val(iter);
}

void dtlv_list_val_iter_destroy(dtlv_list_val_iter *iter) {
  if (iter) free(iter);
}

struct dtlv_list_val_full_iter {
  MDB_cursor *cur;
  MDB_val *key;
  MDB_val *val;
  size_t n;
  size_t c;
};

int dtlv_list_val_full_iter_create(dtlv_list_val_full_iter **iter,
                                   MDB_cursor *cur,
                                   MDB_val *key, MDB_val *val) {
  dtlv_list_val_full_iter *i;
  i = calloc(1, sizeof(struct dtlv_list_val_full_iter));
  if (!i) return ENOMEM;

  i->cur = cur;
  i->key = key;
  i->val = val;

  *iter = i;
  return MDB_SUCCESS;
}

int dtlv_list_val_full_iter_seek(dtlv_list_val_full_iter *iter, MDB_val *k) {
  val_in(iter->key, k);

  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET);
  if (rc == MDB_SUCCESS) {

    rc = mdb_cursor_count(iter->cur, &iter->n);
    if (rc != MDB_SUCCESS) return rc;

    iter->c = 0;

    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_FIRST_DUP);
    if (rc == MDB_SUCCESS) {
      iter->c++;
      return DTLV_TRUE;
    } else return rc;

  } else if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  else return rc;
}

int dtlv_list_val_full_iter_has_next(dtlv_list_val_full_iter *iter) {
  if (iter->c < iter->n) {
    iter->c++;
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_NEXT_DUP);
    if (rc == MDB_SUCCESS) return DTLV_TRUE;
    else return rc;
  } else return DTLV_FALSE;
}

void dtlv_list_val_full_iter_destroy(dtlv_list_val_full_iter *iter) {
  if (iter) free(iter);
}
