/** @file dtlv.h
 *	Native supporting functions for Datalevin: a simple, fast and
 *  versatile Datalog database.
 *
 *  Datalevin works with LMDB, a Btree based key value store. This library
 *  provides an iterator interface to LMDB.
 *
 *  Datalevin uses usearch to provide vector similarity search.
 *
 *	@author	Huahai Yang
 *
 *	@copyright Copyright 2020-2025. Huahai Yang. All rights reserved.
 *
 *  This code is released under Eclipse Public License 2.0.
 */

#include "lmdb/libraries/liblmdb/lmdb.h"
#include "usearch/c/usearch.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DTLV_TRUE	  255
#define DTLV_FALSE	256

  /**
   * Opaque structure for a iterator that iterates by keys only.
   */
  typedef struct dtlv_key_iter dtlv_key_iter;

  /**
   * A function to create a key iterator.
   *
   * @param iter The address where the iterator will be stored.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param forward iterate forward (DTLV_TRUE) or backward (DTLV_FALSE).
   * @param start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
   * @param end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
   * @param start_key The start key, could be null
   * @param end_key The end key, could be null.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_key_iter_create(dtlv_key_iter **iter,
                           MDB_cursor *cur, MDB_val *key, MDB_val *val,
                           int forward, int start, int end,
                           MDB_val *start_key, MDB_val *end_key);

  /**
   * A function to indicate if the key iterator has the next item. If it does,
   * the key will be in the key argument passed to dtlv_key_iter_create, same
   * with value.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE on true,  DTLV_FALSE on false, or an error code.
   */
  int dtlv_key_iter_has_next(dtlv_key_iter *iter);

  /**
   * A function to release memory of the iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_key_iter_destroy(dtlv_key_iter *iter);

  /**
   * Opaque structure for a list iterator that iterates both key and values (list)
   * for a dupsort DBI.
   */
  typedef struct dtlv_list_iter dtlv_list_iter;

  /**
   * A function to create a list iterator.
   *
   * @param iter The address where the iterator will be stored.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param kforward iterate keys forward (DTLV_TRUE) or not.
   * @param kstart if to include (DTLV_TRUE) or not the start_key.
   * @param kend if to include (DTLV_TRUE) or not the end_key.
   * @param start_key The start key.
   * @param end_key The end key..
   * @param vforward iterate values forward (DTLV_TRUE) or not.
   * @param vstart if to include (DTLV_TRUE) or not the start_val.
   * @param vend if to include (DTLV_TRUE) or not the end_val.
   * @param start_val The start value.
   * @param end_val The end value.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_list_iter_create(dtlv_list_iter **iter,
                            MDB_cursor *cur, MDB_val *key, MDB_val *val,
                            int kforward, int kstart, int kend,
                            MDB_val *start_key, MDB_val *end_key,
                            int vforward, int vstart, int vend,
                            MDB_val *start_val, MDB_val *end_val);

  /**
   * A function to indicate if the list iterator has the next item. If it
   * does, the key will be in the key argument passed to dtlv_list_iter_create,
   * the same with value.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE on true and DTLV_FALSE on false.
   */
  int dtlv_list_iter_has_next(dtlv_list_iter *iter);

  /**
   * A function to destroy the list iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_list_iter_destroy(dtlv_list_iter *iter);

  /**
   * Opaque structure for a list value iterator that iterates values
   * (forward only currently) of keys for a dupsort DBI.
   */
  typedef struct dtlv_list_val_iter dtlv_list_val_iter;

  /**
   * A function to create a list values iterator.
   *
   * @param iter The address where the iterator will be stored.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param vstart if to include (DTLV_TRUE) or not the start_val.
   * @param vend if to include (DTLV_TRUE) or not the end_val.
   * @param start_val The start value..
   * @param end_val The end value.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_list_val_iter_create(dtlv_list_val_iter **iter,
                                MDB_cursor *cur, MDB_val *key, MDB_val *val,
                                int vstart, int vend,
                                MDB_val *start_val, MDB_val *end_val);

  /**
   * A function to seek to a key.
   *
   * @param iter The iterator handle.
   * @param k The key to seek.
   * @return DTLV_TRUE on found and DTLV_FALSE when not found.
   */
  int dtlv_list_val_iter_seek(dtlv_list_val_iter *iter, MDB_val *k);

  /**
   * A function to indicate if the iterator has the next item. If it does,
   * the value will be in val argument passed to dtlv_list_val_iter_create.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE on true and DTLV_FALSE on false.
   */
  int dtlv_list_val_iter_has_next(dtlv_list_val_iter *iter);

  /**
   * A function to destroy the list val iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_list_val_iter_destroy(dtlv_list_val_iter *iter);

  /**
   * Opaque structure for a list value iterator that iterates all values
   * of the key, without value comparisons, for a dupsort DBI.
   */
  typedef struct dtlv_list_val_full_iter dtlv_list_val_full_iter;

  /**
   * A function to create a list values full iterator.
   *
   * @param iter The address where the iterator will be stored.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_list_val_full_iter_create(dtlv_list_val_full_iter **iter,
                                     MDB_cursor *cur,
                                     MDB_val *key, MDB_val *val);

  /**
   * A function to seek to a key.
   *
   * @param iter The iterator handle.
   * @param k The key to seek.
   * @return DTLV_TRUE on found and DTLV_FALSE when not found.
   */
  int dtlv_list_val_full_iter_seek(dtlv_list_val_full_iter *iter, MDB_val *k);

  /**
   * A function to indicate if the iterator has the next item. If it does,
   * the value will be in val argument passed to dtlv_list_val_full_iter_create.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE on true and DTLV_FALSE on false.
   */
  int dtlv_list_val_full_iter_has_next(dtlv_list_val_full_iter *iter);

  /**
   * A function to destroy the list val full iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_list_val_full_iter_destroy(dtlv_list_val_full_iter *iter);

  /**
   * A function to return the count of values for a key.
   *
   * @param iter The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @return The value count.
   */
  size_t dtlv_list_val_count(MDB_cursor *cur, MDB_val *key, MDB_val *val);

  /**
   * A function to return the number of key-values in the specified value range of
   * the specified key range, for a dupsort DBI.
   *
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param kforward iterate keys forward (DTLV_TRUE) or not.
   * @param kstart if to include (DTLV_TRUE) or not the start_key.
   * @param kend if to include (DTLV_TRUE) or not the end_key.
   * @param start_key The start key.
   * @param end_key The end key..
   * @param vforward iterate vals forward (DTLV_TRUE) or not.
   * @param vstart if to include (DTLV_TRUE) or not the start_val.
   * @param vend if to include (DTLV_TRUE) or not the end_val.
   * @param start_val The start value.
   * @param end_val The end value.
   * @return The count
   */
  size_t dtlv_list_range_count(MDB_cursor *cur,
                               MDB_val *key, MDB_val *val,
                               int kforward, int kstart, int kend,
                               MDB_val *start_key, MDB_val *end_key,
                               int vforward, int vstart, int vend,
                               MDB_val *start_val, MDB_val *end_val);

  /**
   * A function to return the number of key-values in the specified value range of
   * the specified key range, for a dupsort DBI. Capped. When cap is reached, stop
   * counting and return cap;
   *
   * @param cur The cursor.
   * @param cap The cap.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param kforward iterate keys forward (DTLV_TRUE) or not.
   * @param kstart if to include (DTLV_TRUE) or not the start_key.
   * @param kend if to include (DTLV_TRUE) or not the end_key.
   * @param start_key The start key.
   * @param end_key The end key..
   * @param vforward iterate vals forward (DTLV_TRUE) or not.
   * @param vstart if to include (DTLV_TRUE) or not the start_val.
   * @param vend if to include (DTLV_TRUE) or not the end_val.
   * @param start_val The start value.
   * @param end_val The end value.
   * @return The count
   */
  size_t dtlv_list_range_count_cap(MDB_cursor *cur, size_t cap,
                                   MDB_val *key, MDB_val *val,
                                   int kforward, int kstart, int kend,
                                   MDB_val *start_key, MDB_val *end_key,
                                   int vforward, int vstart, int vend,
                                   MDB_val *start_val, MDB_val *end_val);

  /**
   * A function to return the number of keys in a key range.
   *
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param forward iterate keys forward (DTLV_TRUE) or not.
   * @param start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
   * @param end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
   * @param start_key The start key, could be null
   * @param end_key The end key, could be null.
   * @return The count
   */
  size_t dtlv_key_range_count(MDB_cursor *cur,
                              MDB_val *key, MDB_val *val,
                              int forward, int start, int end,
                              MDB_val *start_key, MDB_val *end_key);

  /**
   * A function to return the number of keys in a key range. Capped. When cap is
   * reached, stop counting and return cap;
   *
   * @param cur The cursor.
   * @param cap The cap.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param forward iterate keys forward (DTLV_TRUE) or not.
   * @param start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
   * @param end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
   * @param start_key The start key, could be null
   * @param end_key The end key, could be null.
   * @return The count
   */
  size_t dtlv_key_range_count_cap(MDB_cursor *cur, size_t cap,
                                  MDB_val *key, MDB_val *val,
                                  int forward, int start, int end,
                                  MDB_val *start_key, MDB_val *end_key);

  /**
   * A function to return the total number of values in a key range, for a
   * dupsort DBI.
   *
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param forward iterate keys forward (DTLV_TRUE) or not.
   * @param start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
   * @param end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
   * @param start_key The start key, could be null
   * @param end_key The end key, could be null.
   * @return The count
   */
  size_t dtlv_key_range_list_count(MDB_cursor *cur,
                                   MDB_val *key, MDB_val *val,
                                   int forward, int start, int end,
                                   MDB_val *start_key, MDB_val *end_key);

  /**
   * A function to return the total number of values in a key range, for a
   * dupsort DBI. Capped. When cap is reached, stop counting and return
   * cap;
   *
   * @param cur The cursor.
   * @param cap The cap.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param forward iterate keys forward (DTLV_TRUE) or not.
   * @param start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
   * @param end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
   * @param start_key The start key, could be null
   * @param end_key The end key, could be null.
   * @return The count
   */
  size_t dtlv_key_range_list_count_cap(MDB_cursor *cur, size_t cap,
                                       MDB_val *key, MDB_val *val,
                                       int forward, int start, int end,
                                       MDB_val *start_key, MDB_val *end_key);

  /**
   * A function to return the total number of values in a key range, for a
   * dupsort DBI. Capped. When cap is reached, stop counting and return
   * cap;
   *
   * @param cur The cursor.
   * @param cap The cap.
   * @param budget The budget.
   * @param step The step.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param forward iterate keys forward (DTLV_TRUE) or not.
   * @param start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
   * @param end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
   * @param start_key The start key, could be null
   * @param end_key The end key, could be null.
   * @return The count
   */
  size_t dtlv_key_range_list_count_cap_budget(MDB_cursor *cur, size_t cap,
                                              size_t budget, size_t step,
                                              MDB_val *key, MDB_val *val,
                                              int forward, int start, int end,
                                              MDB_val *start_key, MDB_val *end_key);

  /**
   * Opaque structure for a list sample iterator that return samples
   * for a dupsort DBI.
   */
  typedef struct dtlv_list_sample_iter dtlv_list_sample_iter;

  /**
   * A function to create a list sample iterator.
   *
   * @param iter The address where the iterator will be stored.
   * @param indices The array of sample indices..
   * @param samples The number of samples.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param kforward iterate keys forward (DTLV_TRUE) or not.
   * @param kstart if to include (DTLV_TRUE) or not the start_key.
   * @param kend if to include (DTLV_TRUE) or not the end_key.
   * @param start_key The start key.
   * @param end_key The end key..
   * @param vforward iterate vals forward (DTLV_TRUE) or not.
   * @param vstart if to include (DTLV_TRUE) or not the start_val.
   * @param vend if to include (DTLV_TRUE) or not the end_val.
   * @param start_val The start value.
   * @param end_val The end value.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_list_sample_iter_create(dtlv_list_sample_iter **iter,
                                   size_t *indices, int samples,
                                   size_t budget, size_t step,
                                   MDB_cursor *cur, MDB_val *key, MDB_val *val,
                                   int kforward, int kstart, int kend,
                                   MDB_val *start_key, MDB_val *end_key,
                                   int vforward, int vstart, int vend,
                                   MDB_val *start_val, MDB_val *end_val);

  /**
   * A function to indicate if the list sample iterator has next sample. If
   * it does, the sample will be in the key/val argument passed to
   * dtlv_list_sample_iter_create.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE on true and DTLV_FALSE on false.
   */
  int dtlv_list_sample_iter_has_next(dtlv_list_sample_iter *iter);

  /**
   * A function to destroy the list sample iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_list_sample_iter_destroy(dtlv_list_sample_iter *iter);


#ifdef __cplusplus
}
#endif
