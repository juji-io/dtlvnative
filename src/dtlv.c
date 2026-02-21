#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdint.h>
#include "dtlv.h"


void val_in(MDB_val *this, MDB_val *other) {
  this->mv_size = other->mv_size;
  this->mv_data = other->mv_data;
}

static int dtlv_copy_indices(size_t **dst, size_t *src, int samples) {
  if (!dst) return EINVAL;
  if (samples < 0) return EINVAL;
  if (samples == 0) {
    *dst = NULL;
    return MDB_SUCCESS;
  }
  if (!src) return EINVAL;
  size_t bytes = (size_t)samples * sizeof(size_t);
  size_t *copy = malloc(bytes);
  if (!copy) return ENOMEM;
  memcpy(copy, src, bytes);
  *dst = copy;
  return MDB_SUCCESS;
}
struct dtlv_key_iter {
  MDB_cursor *cur;
  MDB_txn *txn;
  MDB_dbi dbi;
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
  i->txn = mdb_cursor_txn(cur);
  i->dbi = mdb_cursor_dbi(cur);
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
          && (mdb_cmp(iter->txn, iter->dbi, iter->key, iter->start_key) == 0))
        return key_check(iter, MDB_NEXT_NODUP);
      return key_continue(iter);
    }
    if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    return rc;
  }
  return key_check(iter, MDB_FIRST);
}

int key_init_back(dtlv_key_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->start == DTLV_TRUE)
          && (mdb_cmp(iter->txn, iter->dbi, iter->key, iter->start_key) == 0))
        return key_continue_back(iter);
      return key_check_back(iter, MDB_PREV_NODUP);
    }
    if (rc == MDB_NOTFOUND) return key_check_back(iter, MDB_LAST);
    return rc;
  }
  return key_check_back(iter, MDB_LAST);
}

int key_continue(dtlv_key_iter *iter) {
  if (iter->end_key) {
    int r = mdb_cmp(iter->txn, iter->dbi, iter->key, iter->end_key);
    if (r == 0) return iter->end;
    if (r > 0) return DTLV_FALSE;
    return DTLV_TRUE;
  }
  return DTLV_TRUE;
}

int key_continue_back(dtlv_key_iter *iter) {
  if (iter->end_key) {
    int r = mdb_cmp(iter->txn, iter->dbi, iter->key, iter->end_key);
    if (r == 0) return iter->end;
    if (r > 0) return DTLV_TRUE;
    return DTLV_FALSE;
  }
  return DTLV_TRUE;
}

int key_check(dtlv_key_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return key_continue(iter);
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  return rc;
}

int key_check_back(dtlv_key_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return key_continue_back(iter);
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  return rc;
}

int key_advance(dtlv_key_iter *iter) {
  if (iter->forward == DTLV_TRUE) return key_check(iter, MDB_NEXT_NODUP);
  return key_check_back(iter, MDB_PREV_NODUP);
}

int key_init_k(dtlv_key_iter *iter) {
  iter->started = DTLV_TRUE;

  if (iter->forward == DTLV_TRUE) return key_init(iter);
  return key_init_back(iter);
}

int dtlv_key_iter_has_next(dtlv_key_iter *iter) {
  if (iter->started == DTLV_TRUE) return key_advance(iter);
  return key_init_k(iter);
}

void dtlv_key_iter_destroy(dtlv_key_iter *iter) {
  if (iter) free(iter);
}

struct dtlv_list_iter {
  MDB_cursor *cur;
  MDB_txn *txn;
  MDB_dbi dbi;
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
  i->txn = mdb_cursor_txn(cur);
  i->dbi = mdb_cursor_dbi(cur);
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
          && (mdb_cmp(iter->txn, iter->dbi, iter->key, iter->start_key) == 0))
        return list_check_key(iter, MDB_NEXT_NODUP);
      return list_key_continue(iter);
    }
    if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    return rc;
  }
  return list_check_key(iter, MDB_FIRST);
}

int list_init_key_back(dtlv_list_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->kstart == DTLV_TRUE)
          && (mdb_cmp(iter->txn, iter->dbi, iter->key, iter->start_key) == 0))
        return list_key_continue_back(iter);
      return list_check_key_back(iter, MDB_PREV_NODUP);
    }
    if (rc == MDB_NOTFOUND) return list_check_key_back(iter, MDB_LAST);
    return rc;
  }
  return list_check_key_back(iter, MDB_LAST);
}

int list_init_val(dtlv_list_iter *iter) {
  if (iter->start_val) {
    val_in(iter->val, iter->start_val);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_GET_BOTH_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->vstart == DTLV_FALSE)
          && (mdb_cmp(iter->txn, iter->dbi, iter->val, iter->start_val) == 0))
        return list_check_val(iter, MDB_NEXT_DUP);
      return list_val_continue(iter);
    }
    if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    return rc;
  }
  return list_check_val(iter, MDB_FIRST_DUP);
}

int list_init_val_back(dtlv_list_iter *iter) {
  if (iter->start_val) {
    val_in(iter->val, iter->start_val);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_GET_BOTH_RANGE);
    if (rc == MDB_SUCCESS) {
      if ((iter->vstart == DTLV_TRUE)
          && (mdb_cmp(iter->txn, iter->dbi, iter->val, iter->start_val) == 0))
        return list_val_continue_back(iter);
      return list_check_val_back(iter, MDB_PREV_DUP);
    }
    if (rc == MDB_NOTFOUND) return list_check_val_back(iter, MDB_LAST_DUP);
    return rc;
  }
  return list_check_val_back(iter, MDB_LAST_DUP);
}

int list_key_end(dtlv_list_iter *iter) {
  iter->key_ended = DTLV_TRUE;
  return DTLV_FALSE;
}

int list_val_end(dtlv_list_iter *iter) {
  if (iter->key_ended == DTLV_TRUE) return DTLV_FALSE;
  return list_advance_key(iter);
}

int list_key_continue(dtlv_list_iter *iter) {
  if (iter->end_key) {
    int r = mdb_cmp(iter->txn, iter->dbi, iter->key, iter->end_key);
    if (r == 0) {
      iter->key_ended = DTLV_TRUE;
      return iter->kend;
    }
    if (r > 0) return list_key_end(iter);
    return DTLV_TRUE;
  }
  return DTLV_TRUE;
}

int list_key_continue_back(dtlv_list_iter *iter) {
  if (iter->end_key) {
    int r = mdb_cmp(iter->txn, iter->dbi, iter->key, iter->end_key);
    if (r == 0) {
      iter->key_ended = DTLV_TRUE;
      return iter->kend;
    }
    if (r > 0) return DTLV_TRUE;
    return list_key_end(iter);
  }
  return DTLV_TRUE;
}

int list_check_key(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_key_continue(iter);
  if (rc == MDB_NOTFOUND) return list_key_end(iter);
  return rc;
}

int list_check_key_back(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_key_continue_back(iter);
  if (rc == MDB_NOTFOUND) return list_key_end(iter);
  return rc;
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
  return list_advance_key(iter);
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
    int r = mdb_cmp(iter->txn, iter->dbi, iter->val, iter->end_val);
    if (r == 0) {
      if (iter->vend == DTLV_TRUE) return DTLV_TRUE;
      return list_val_end(iter);
    }
    if (r > 0) return list_val_end(iter);
    return DTLV_TRUE;
  }
  return DTLV_TRUE;
}

int list_val_continue_back(dtlv_list_iter *iter) {
  if (iter->end_val) {
    int r = mdb_cmp(iter->txn, iter->dbi, iter->val, iter->end_val);
    if (r == 0) {
      if (iter->vend == DTLV_TRUE) return DTLV_TRUE;
      return list_val_end(iter);
    }
    if (r > 0) return DTLV_TRUE;
    return list_val_end(iter);
  }
  return DTLV_TRUE;
}

int list_check_val(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_val_continue(iter);
  if (rc == MDB_NOTFOUND) return list_val_end(iter);
  return rc;
}

int list_check_val_back(dtlv_list_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_val_continue_back(iter);
  if (rc == MDB_NOTFOUND) return list_val_end(iter);
  return rc;
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
    return list_advance_val_back(iter);
  }
  return list_init_kv(iter);
}

void dtlv_list_iter_destroy(dtlv_list_iter *iter) {
  if (iter) free(iter);
}

struct dtlv_list_val_iter {
  MDB_cursor *cur;
  MDB_txn *txn;
  MDB_dbi dbi;
  MDB_val *key;
  MDB_val *val;
  MDB_val *start_val;
  MDB_val *end_val;
};

int dtlv_list_val_iter_create(dtlv_list_val_iter **iter, MDB_cursor *cur,
                              MDB_val *key, MDB_val *val,
                              MDB_val *start_val,
                              MDB_val *end_val) {
  dtlv_list_val_iter *i;
  i = calloc(1, sizeof(struct dtlv_list_val_iter));
  if (!i) return ENOMEM;

  i->cur = cur;
  i->txn = mdb_cursor_txn(cur);
  i->dbi = mdb_cursor_dbi(cur);
  i->key = key;
  i->val = val;
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
      return list_val_val_continue(iter);
    }
    if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    return rc;
  } else {
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_SUCCESS) return list_val_check_val(iter, MDB_FIRST_DUP);
    if (rc == MDB_NOTFOUND) return DTLV_FALSE;
    return rc;
  }
}

int list_val_val_continue(dtlv_list_val_iter *iter) {
  if (iter->end_val) {
    int r = mdb_cmp(iter->txn, iter->dbi, iter->val, iter->end_val);
    if (r > 0) return DTLV_FALSE;
    return DTLV_TRUE;
  }
  return DTLV_TRUE;
}

int list_val_check_val(dtlv_list_val_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS) return list_val_val_continue(iter);
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  return rc;
}

int dtlv_list_val_iter_seek(dtlv_list_val_iter *iter, MDB_val *k) {
  val_in(iter->key, k);
  return list_val_init_val(iter);
}

int dtlv_list_val_iter_has_next(dtlv_list_val_iter *iter) {
  return list_val_check_val(iter, MDB_NEXT_DUP);
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
  const MDB_val *dup_vals;
  int fast_path;
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
  i->dup_vals = NULL;
  i->fast_path = DTLV_FALSE;

  *iter = i;
  return MDB_SUCCESS;
}

int dtlv_list_val_full_iter_seek(dtlv_list_val_full_iter *iter, MDB_val *k) {
  val_in(iter->key, k);
  iter->dup_vals = NULL;
  iter->fast_path = DTLV_FALSE;

  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET);
  if (rc == MDB_SUCCESS) {

    rc = mdb_cursor_count(iter->cur, &iter->n);
    if (rc != MDB_SUCCESS) return rc;

    iter->c = 1;
    const MDB_val *vals = NULL;
    mdb_size_t total = 0;
    int frc = mdb_cursor_list_dup(iter->cur, &vals, &total);
    if (frc == MDB_SUCCESS && total > 0) {
      iter->dup_vals = vals;
      iter->fast_path = DTLV_TRUE;
      iter->n = (size_t)total;
    }
    return DTLV_TRUE;

  }
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  return rc;
}

int dtlv_list_val_full_iter_has_next(dtlv_list_val_full_iter *iter) {
  if (iter->c < iter->n) {
    if (iter->fast_path == DTLV_TRUE && iter->dup_vals) {
      *iter->val = iter->dup_vals[iter->c];
      iter->c++;
      return DTLV_TRUE;
    }
    iter->c++;
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_NEXT_DUP);
    if (rc == MDB_SUCCESS) return DTLV_TRUE;
    return rc;
  }
  return DTLV_FALSE;
}

void dtlv_list_val_full_iter_destroy(dtlv_list_val_full_iter *iter) {
  if (iter) free(iter);
}

size_t dtlv_list_val_count(MDB_cursor *cur, MDB_val *key, MDB_val *val) {
  size_t n;

  int rc = mdb_cursor_get(cur, key, val, MDB_SET);
  if (rc == MDB_SUCCESS) {
    rc = mdb_cursor_count(cur, &n);
    if (rc == MDB_SUCCESS) return n;
    return rc;
  }
  if (rc == MDB_NOTFOUND) return 0;
  return rc;
}


struct dtlv_key_rank_sample_iter {
  MDB_cursor *cur;
  MDB_txn *txn;
  MDB_dbi dbi;
  MDB_val *key;
  MDB_val *val;
  size_t *indices;
  int samples;
  int current;
  uint64_t lower_rank;
  uint64_t upper_rank;
  int range_empty;
};

static int dtlv_key_rank_sample_iter_within_end(
    dtlv_key_rank_sample_iter *iter, MDB_val *end_key) {
  if (!end_key) return DTLV_TRUE;
  int cmp = mdb_cmp(iter->txn, iter->dbi, iter->key, end_key);
  if (cmp > 0) return DTLV_FALSE;
  return DTLV_TRUE;
}

static int dtlv_key_rank_sample_iter_seek_start(
    dtlv_key_rank_sample_iter *iter, MDB_val *start_key) {
  int rc;
  if (start_key && start_key->mv_size > 0) {
    val_in(iter->key, start_key);
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
  } else {
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_FIRST);
  }

  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  if (rc != MDB_SUCCESS) return rc;
  return DTLV_TRUE;
}

static int dtlv_key_rank_sample_iter_compute_lower(
    dtlv_key_rank_sample_iter *iter, MDB_val *start_key, MDB_val *end_key,
    uint64_t *rank_out) {
  int rc = dtlv_key_rank_sample_iter_seek_start(iter, start_key);
  if (rc == DTLV_FALSE) return DTLV_FALSE;
  if (rc != DTLV_TRUE) return rc;
  if (dtlv_key_rank_sample_iter_within_end(iter, end_key) == DTLV_FALSE)
    return DTLV_FALSE;

  if (!start_key) {
    *rank_out = 0;
    return MDB_SUCCESS;
  }

  uint64_t rank = 0;
  rc = mdb_cursor_key_rank(iter->cur, iter->key, NULL, 0, &rank);
  if (rc != MDB_SUCCESS) return rc;
  *rank_out = rank;
  return MDB_SUCCESS;
}

static int dtlv_key_rank_sample_iter_compute_upper(
    dtlv_key_rank_sample_iter *iter, MDB_val *end_key, uint64_t *rank_out) {
  if (!end_key) return mdb_count_all(iter->txn, iter->dbi, 0, rank_out);

  val_in(iter->key, end_key);
  int rc;
  if (end_key->mv_size > 0) {
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
  } else {
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_FIRST);
  }
  if (rc == MDB_NOTFOUND) return mdb_count_all(iter->txn, iter->dbi, 0, rank_out);
  if (rc != MDB_SUCCESS) return rc;

  int cmp = mdb_cmp(iter->txn, iter->dbi, iter->key, end_key);
  if (cmp > 0)
    return mdb_cursor_key_rank(iter->cur, iter->key, NULL, 0, rank_out);

  /* cmp == 0: step past the inclusive upper bound. */
  rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_NEXT_NODUP);
  if (rc == MDB_NOTFOUND) return mdb_count_all(iter->txn, iter->dbi, 0, rank_out);
  if (rc != MDB_SUCCESS) return rc;
  return mdb_cursor_key_rank(iter->cur, iter->key, NULL, 0, rank_out);
}

int dtlv_key_rank_sample_iter_create(dtlv_key_rank_sample_iter **iter,
                                     size_t *indices, int samples,
                                     MDB_cursor *cur, MDB_val *key,
                                     MDB_val *val, MDB_val *start_key,
                                     MDB_val *end_key) {
  dtlv_key_rank_sample_iter *s;
  s = calloc(1, sizeof(struct dtlv_key_rank_sample_iter));
  if (!s) return ENOMEM;

  int rc_copy = dtlv_copy_indices(&s->indices, indices, samples);
  if (rc_copy != MDB_SUCCESS) {
    free(s);
    return rc_copy;
  }

  s->cur = cur;
  s->txn = mdb_cursor_txn(cur);
  s->dbi = mdb_cursor_dbi(cur);
  s->key = key;
  s->val = val;
  s->samples = samples;
  s->current = 0;
  s->lower_rank = 0;
  s->upper_rank = 0;
  s->range_empty = DTLV_FALSE;

  uint64_t lower = 0;
  int rc = dtlv_key_rank_sample_iter_compute_lower(s, start_key, end_key, &lower);
  if (rc == DTLV_FALSE) {
    s->range_empty = DTLV_TRUE;
  } else if (rc != MDB_SUCCESS) {
    free(s->indices);
    free(s);
    return rc;
  } else {
    s->lower_rank = lower;
    uint64_t upper = 0;
    rc = dtlv_key_rank_sample_iter_compute_upper(s, end_key, &upper);
    if (rc != MDB_SUCCESS) {
      free(s->indices);
      free(s);
      return rc;
    }
    s->upper_rank = upper;
    if (s->lower_rank >= s->upper_rank) s->range_empty = DTLV_TRUE;
  }

  *iter = s;
  return MDB_SUCCESS;
}

static int dtlv_key_rank_sample_iter_resync(
    dtlv_key_rank_sample_iter *iter, uint64_t target) {
  uint64_t total = 0;
  int rc = mdb_count_all(iter->txn, iter->dbi, 0, &total);
  if (rc != MDB_SUCCESS) return rc;

  if (total < iter->upper_rank) iter->upper_rank = total;
  if (target >= iter->upper_rank) return MDB_NOTFOUND;

  rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_FIRST);
  if (rc != MDB_SUCCESS) return rc;

  uint64_t i = 0;
  while (i < target) {
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_NEXT);
    if (rc != MDB_SUCCESS) return rc;
    i++;
  }
  return MDB_SUCCESS;
}

int dtlv_key_rank_sample_iter_has_next(dtlv_key_rank_sample_iter *iter) {
  if (!iter) return EINVAL;
  if (iter->range_empty == DTLV_TRUE) return DTLV_FALSE;
  if (iter->current == iter->samples) return DTLV_FALSE;

  uint64_t offset = (uint64_t)iter->indices[iter->current];
  uint64_t target = iter->lower_rank + offset;
  if (target >= iter->upper_rank) {
    iter->range_empty = DTLV_TRUE;
    return DTLV_FALSE;
  }

  int rc = mdb_cursor_get_rank(iter->cur, target, iter->key, iter->val, 0);
  if (rc == MDB_SUCCESS) {
    iter->current++;
    return DTLV_TRUE;
  }
  if (rc == MDB_NOTFOUND) {
    iter->range_empty = DTLV_TRUE;
    return DTLV_FALSE;
  }
  if (rc == MDB_BAD_VALSIZE) {
    /* Some LMDB states can surface BAD_VALSIZE; fall back to a manual walk. */
    rc = dtlv_key_rank_sample_iter_resync(iter, target);
    if (rc == MDB_SUCCESS) {
      iter->current++;
      return DTLV_TRUE;
    }
    if (rc == MDB_NOTFOUND) {
      iter->range_empty = DTLV_TRUE;
      return DTLV_FALSE;
    }
    /* Any error during resync (including BAD_VALSIZE) means we can't continue reliably. */
    iter->range_empty = DTLV_TRUE;
    return DTLV_FALSE;
  }
  /* For any other unexpected error, mark the range as empty and stop iteration gracefully. */
  iter->range_empty = DTLV_TRUE;
  return DTLV_FALSE;
}

void dtlv_key_rank_sample_iter_destroy(dtlv_key_rank_sample_iter *iter) {
  if (iter) {
    free(iter->indices);
    free(iter);
  }
}

struct dtlv_list_key_range_full_val_iter {
  MDB_cursor *cur;
  MDB_txn *txn;
  MDB_dbi dbi;
  MDB_val *key;
  MDB_val *val;
  int kstart;
  int kend;
  MDB_val *start_key;
  MDB_val *end_key;
  int started;
  int range_done;
  size_t dup_total;
  size_t dup_index;
  const MDB_val *dup_vals;
  int fast_path;
};

static int dtlv_list_key_range_full_val_iter_within_end(
    dtlv_list_key_range_full_val_iter *iter) {
  if (!iter->end_key) return DTLV_TRUE;
  int cmp = mdb_cmp(iter->txn, iter->dbi, iter->key, iter->end_key);
  if (cmp > 0) return DTLV_FALSE;
  if ((cmp == 0) && (iter->kend == DTLV_FALSE)) return DTLV_FALSE;
  return DTLV_TRUE;
}

static int dtlv_list_key_range_full_val_iter_advance_key(
    dtlv_list_key_range_full_val_iter *iter);

static int dtlv_list_key_range_full_val_iter_finish(
    dtlv_list_key_range_full_val_iter *iter) {
  iter->range_done = DTLV_TRUE;
  iter->dup_total = 0;
  iter->dup_index = 0;
  iter->dup_vals = NULL;
  iter->fast_path = DTLV_FALSE;
  return DTLV_FALSE;
}

static int dtlv_list_key_range_full_val_iter_accept_current(
    dtlv_list_key_range_full_val_iter *iter) {
  if (dtlv_list_key_range_full_val_iter_within_end(iter) == DTLV_FALSE)
    return dtlv_list_key_range_full_val_iter_finish(iter);
  iter->dup_vals = NULL;
  iter->fast_path = DTLV_FALSE;
  int rc = mdb_cursor_count(iter->cur, &iter->dup_total);
  if (rc != MDB_SUCCESS) return rc;
  iter->dup_index = 1;
  const MDB_val *vals = NULL;
  mdb_size_t total = 0;
  int frc = mdb_cursor_list_dup(iter->cur, &vals, &total);
  if (frc == MDB_SUCCESS && total > 0) {
    iter->dup_vals = vals;
    iter->fast_path = DTLV_TRUE;
    iter->dup_total = (size_t)total;
  }
  return DTLV_TRUE;
}

static int dtlv_list_key_range_full_val_iter_check_key(
    dtlv_list_key_range_full_val_iter *iter, int op) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, op);
  if (rc == MDB_SUCCESS)
    return dtlv_list_key_range_full_val_iter_accept_current(iter);
  if (rc == MDB_NOTFOUND) return dtlv_list_key_range_full_val_iter_finish(iter);
  return rc;
}

static int dtlv_list_key_range_full_val_iter_init_key(
    dtlv_list_key_range_full_val_iter *iter) {
  if (iter->start_key) {
    val_in(iter->key, iter->start_key);
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
    if (rc == MDB_NOTFOUND) return dtlv_list_key_range_full_val_iter_finish(iter);
    if (rc != MDB_SUCCESS) return rc;

    int cmp = mdb_cmp(iter->txn, iter->dbi, iter->key, iter->start_key);
    if ((cmp == 0) && (iter->kstart == DTLV_FALSE))
      return dtlv_list_key_range_full_val_iter_check_key(iter, MDB_NEXT_NODUP);
    return dtlv_list_key_range_full_val_iter_accept_current(iter);
  }
  return dtlv_list_key_range_full_val_iter_check_key(iter, MDB_FIRST);
}

static int dtlv_list_key_range_full_val_iter_advance_key(
    dtlv_list_key_range_full_val_iter *iter) {
  return dtlv_list_key_range_full_val_iter_check_key(iter, MDB_NEXT_NODUP);
}

int dtlv_list_key_range_full_val_iter_create(
    dtlv_list_key_range_full_val_iter **iter, MDB_cursor *cur,
    MDB_val *key, MDB_val *val, int kstart, int kend,
    MDB_val *start_key, MDB_val *end_key) {
  dtlv_list_key_range_full_val_iter *s;
  s = calloc(1, sizeof(struct dtlv_list_key_range_full_val_iter));
  if (!s) return ENOMEM;

  s->cur = cur;
  s->txn = mdb_cursor_txn(cur);
  s->dbi = mdb_cursor_dbi(cur);
  s->key = key;
  s->val = val;
  s->kstart = kstart;
  s->kend = kend;
  s->start_key = start_key;
  s->end_key = end_key;
  s->started = DTLV_FALSE;
  s->range_done = DTLV_FALSE;
  s->dup_vals = NULL;
  s->fast_path = DTLV_FALSE;

  *iter = s;
  return MDB_SUCCESS;
}

int dtlv_list_key_range_full_val_iter_has_next(
    dtlv_list_key_range_full_val_iter *iter) {
  if (!iter) return EINVAL;
  if (iter->range_done == DTLV_TRUE) return DTLV_FALSE;

  if (iter->started == DTLV_FALSE) {
    int rc = dtlv_list_key_range_full_val_iter_init_key(iter);
    if (rc != DTLV_TRUE) {
      if (rc == DTLV_FALSE) iter->range_done = DTLV_TRUE;
      return rc;
    }
    iter->started = DTLV_TRUE;
    return DTLV_TRUE;
  }

  if (iter->dup_index < iter->dup_total) {
    if (iter->fast_path == DTLV_TRUE && iter->dup_vals) {
      *iter->val = iter->dup_vals[iter->dup_index];
      iter->dup_index++;
      return DTLV_TRUE;
    }
    int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_NEXT_DUP);
    if (rc == MDB_SUCCESS) {
      iter->dup_index++;
      return DTLV_TRUE;
    }
    return rc;
  }

  int rc = dtlv_list_key_range_full_val_iter_advance_key(iter);
  if (rc == DTLV_TRUE) return DTLV_TRUE;
  if (rc == DTLV_FALSE) iter->range_done = DTLV_TRUE;
  return rc;
}

void dtlv_list_key_range_full_val_iter_destroy(
    dtlv_list_key_range_full_val_iter *iter) {
  if (iter) free(iter);
}

struct dtlv_list_rank_sample_iter {
  MDB_cursor *cur;
  MDB_txn *txn;
  MDB_dbi dbi;
  MDB_val *key;
  MDB_val *val;
  size_t *indices;
  int samples;
  int current;
  uint64_t lower_rank;
  uint64_t upper_rank;
  int range_empty;
};

static int dtlv_list_rank_sample_iter_within_end(
    dtlv_list_rank_sample_iter *iter, MDB_val *end_key) {
  if (!end_key) return DTLV_TRUE;
  int rc = mdb_cmp(iter->txn, iter->dbi, iter->key, end_key);
  if (rc > 0) return DTLV_FALSE;
  return DTLV_TRUE;
}

static int dtlv_list_rank_sample_iter_seek_start(
    dtlv_list_rank_sample_iter *iter, MDB_val *start_key) {
  int rc;
  if (start_key) {
    val_in(iter->key, start_key);
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
  } else {
    rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_FIRST);
  }
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  if (rc != MDB_SUCCESS) return rc;
  return DTLV_TRUE;
}

static int dtlv_list_rank_sample_iter_seek_after_end(
    dtlv_list_rank_sample_iter *iter, MDB_val *end_key) {
  if (!end_key) return DTLV_FALSE;
  val_in(iter->key, end_key);
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_SET_RANGE);
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  if (rc != MDB_SUCCESS) return rc;

  int cmp = mdb_cmp(iter->txn, iter->dbi, iter->key, end_key);
  if (cmp > 0) return DTLV_TRUE;

  rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_NEXT_NODUP);
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  if (rc != MDB_SUCCESS) return rc;
  return DTLV_TRUE;
}

static int dtlv_list_rank_sample_iter_compute_lower(
    dtlv_list_rank_sample_iter *iter, MDB_val *start_key, MDB_val *end_key,
    uint64_t *rank_out) {
  int rc = dtlv_list_rank_sample_iter_seek_start(iter, start_key);
  if (rc == DTLV_FALSE) return DTLV_FALSE;
  if (rc != DTLV_TRUE) return rc;
  if (dtlv_list_rank_sample_iter_within_end(iter, end_key) == DTLV_FALSE)
    return DTLV_FALSE;
  if (!start_key) {
    *rank_out = 0;
    return MDB_SUCCESS;
  }
  uint64_t rank = 0;
  rc = mdb_cursor_key_rank(iter->cur, iter->key, iter->val, 0, &rank);
  if (rc != MDB_SUCCESS) return rc;
  *rank_out = rank;
  return MDB_SUCCESS;
}

static int dtlv_list_rank_sample_iter_compute_upper(
    dtlv_list_rank_sample_iter *iter, MDB_val *end_key, uint64_t *rank_out) {
  if (!end_key) return DTLV_FALSE;
  int rc = dtlv_list_rank_sample_iter_seek_after_end(iter, end_key);
  if (rc == DTLV_FALSE) return DTLV_FALSE;
  if (rc != DTLV_TRUE) return rc;
  uint64_t rank = 0;
  rc = mdb_cursor_key_rank(iter->cur, iter->key, iter->val, 0, &rank);
  if (rc != MDB_SUCCESS) return rc;
  *rank_out = rank;
  return MDB_SUCCESS;
}

static int dtlv_list_rank_sample_iter_compute_tail(
    dtlv_list_rank_sample_iter *iter, uint64_t *rank_out) {
  int rc = mdb_cursor_get(iter->cur, iter->key, iter->val, MDB_LAST);
  if (rc == MDB_NOTFOUND) {
    *rank_out = 0;
    return MDB_SUCCESS;
  }
  if (rc != MDB_SUCCESS) return rc;

  uint64_t rank = 0;
  rc = mdb_cursor_key_rank(iter->cur, iter->key, iter->val, 0, &rank);
  if (rc != MDB_SUCCESS) return rc;
  *rank_out = rank + 1;
  return MDB_SUCCESS;
}

int dtlv_list_rank_sample_iter_create(dtlv_list_rank_sample_iter **iter,
                                      size_t *indices, int samples,
                                      MDB_cursor *cur, MDB_val *key,
                                      MDB_val *val, MDB_val *start_key,
                                      MDB_val *end_key) {
  dtlv_list_rank_sample_iter *s;
  s = calloc(1, sizeof(struct dtlv_list_rank_sample_iter));
  if (!s) return ENOMEM;

  int rc_copy = dtlv_copy_indices(&s->indices, indices, samples);
  if (rc_copy != MDB_SUCCESS) {
    free(s);
    return rc_copy;
  }

  s->cur = cur;
  s->txn = mdb_cursor_txn(cur);
  s->dbi = mdb_cursor_dbi(cur);
  s->key = key;
  s->val = val;
  s->samples = samples;
  s->current = 0;
  s->lower_rank = 0;
  s->upper_rank = 0;
  s->range_empty = DTLV_FALSE;

  uint64_t lower = 0;
  int rc = dtlv_list_rank_sample_iter_compute_lower(
      s, start_key, end_key, &lower);
  if (rc == DTLV_FALSE) {
    s->range_empty = DTLV_TRUE;
  } else if (rc != MDB_SUCCESS) {
    free(s->indices);
    free(s);
    return rc;
  } else {
    s->lower_rank = lower;
    uint64_t upper = 0;
    rc = dtlv_list_rank_sample_iter_compute_upper(s, end_key, &upper);
    if (rc == DTLV_FALSE) {
      rc = dtlv_list_rank_sample_iter_compute_tail(s, &upper);
      if (rc != MDB_SUCCESS) {
        free(s->indices);
        free(s);
        return rc;
      }
    } else if (rc != MDB_SUCCESS) {
      free(s->indices);
      free(s);
      return rc;
    }
    s->upper_rank = upper;
    if (s->lower_rank >= s->upper_rank) s->range_empty = DTLV_TRUE;
  }

  *iter = s;
  return MDB_SUCCESS;
}

int dtlv_list_rank_sample_iter_has_next(dtlv_list_rank_sample_iter *iter) {
  if (!iter) return EINVAL;
  if (iter->range_empty == DTLV_TRUE) return DTLV_FALSE;
  if (iter->current == iter->samples) return DTLV_FALSE;

  uint64_t offset = (uint64_t)iter->indices[iter->current];
  uint64_t target = iter->lower_rank + offset;
  if (target >= iter->upper_rank) return DTLV_FALSE;

  int rc = mdb_cursor_get_rank(iter->cur, target, iter->key, iter->val, 0);
  if (rc == MDB_SUCCESS) {
    iter->current++;
    return DTLV_TRUE;
  }
  if (rc == MDB_NOTFOUND) return DTLV_FALSE;
  if (rc == MDB_BAD_VALSIZE) {
    iter->range_empty = DTLV_TRUE;
    return DTLV_FALSE;
  }
  return rc;
}

void dtlv_list_rank_sample_iter_destroy(dtlv_list_rank_sample_iter *iter) {
  if (iter) {
    free(iter->indices);
    free(iter);
  }
}
