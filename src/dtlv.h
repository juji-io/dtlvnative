/** @file dtlv.h
 *	@brief Native supporting functions for Datalevin: a simple, fast and
 *  versatile Datalog database.
 *
 *  Datalevin works with LMDB, a Btree based key value store. This library
 *  provides an iterator interface to LMDB.
 *
 *	@author	Huahai Yang
 *
 *	@copyright Copyright 2020-2024. Huahai Yang. All rights reserved.
 *
 *  This code is released under Eclipse Public License 2.0.
 */

#include "lmdb/libraries/liblmdb/lmdb.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DTLV_TRUE	  255
#define DTLV_FALSE	256

 /** @brief The main comparator.
  */
  int dtlv_cmp_memn(const MDB_val *a, const MDB_val *b);

 /** @brief Set the comparator for a DBI to the main comparator.
  */
  int dtlv_set_comparator(MDB_txn *txn, int dbi);

 /** @brief Set the comparator for a list DBI.
  */
  int dtlv_set_dupsort_comparator(MDB_txn *txn, int dbi);

 /** @brief Opaque structure for a key iterator
  *
  * Iterate by keys only.
  */
  typedef struct dtlv_key_iter dtlv_key_iter;

 /** @brief A function to create a key iterator.
  *
  * @param[out] iter The address where the iterator will be stored.
  * @param[in] cur The cursor.
  * @param[in] key Holder for the key.
  * @param[in] val Holder for the value.
  * @param[in] forward iterate forward (DTLV_TRUE) or backward (DTLV_FALSE).
  * @param[in] start if to include (DTLV_TRUE) or not (DTLV_FALSE) start_key.
  * @param[in] end if to include (DTLV_TRUE) or not (DTLV_FALSE) end_key.
  * @param[in] start_key The start key, could be null
  * @param[in] end_key The end key, could be null.
  * @return A non-zero error value on failure and 0 on success.
  */
  int dtlv_key_iter_create(dtlv_key_iter **iter,
                           MDB_cursor *cur, MDB_val *key, MDB_val *val,
                           int forward, int start, int end,
                           MDB_val *start_key, MDB_val *end_key);

  /** @brief A function to indicate if the iterator has the next item.
   *
   * @param[in] iter The iterator handle.
   * @return DTLV_TRUE on true,  DTLV_FALSE on false, or other error code.
   */
  int dtlv_key_iter_has_next(dtlv_key_iter *iter);

  /** @brief A function to return the next item.
   *
   * @param[in] iter The iterator handle.
   * @param[out] key The key of the next item.
   * @param[out] val The value of the next item.
   */
  void dtlv_key_iter_next(dtlv_key_iter *iter, MDB_val *key, MDB_val *val);

 /** @brief A function to destroy the iterator.
  *
  * @param[in] iter The iterator handle.
  */
  void dtlv_key_iter_destroy(dtlv_key_iter *iter);

  /** @brief Opaque structure for a list iterator
   *
   * Iterate both key and values (list) for a dupsort DBI.
   */
  typedef struct dtlv_list_iter dtlv_list_iter;

 /** @brief A function to create a list iterator.
  *
  * @param[out] iter The address where the iterator will be stored.
  * @param[in] cur The cursor.
  * @param[in] key Holder for the key.
  * @param[in] val Holder for the value.
  * @param[in] kforward iterate keys forward (DTLV_TRUE) or not.
  * @param[in] kstart if to include (DTLV_TRUE) or not the start_key.
  * @param[in] kend if to include (DTLV_TRUE) or not the end_key.
  * @param[in] start_key The start key.
  * @param[in] end_key The end key..
  * @param[in] vforward iterate values forward (DTLV_TRUE) or not.
  * @param[in] vstart if to include (DTLV_TRUE) or not the start_val.
  * @param[in] vend if to include (DTLV_TRUE) or not the end_val.
  * @param[in] start_val The start value.
  * @param[in] end_val The end value.
  * @return A non-zero error value on failure and 0 on success.
  */
  int dtlv_list_iter_create(dtlv_list_iter **iter,
                            MDB_cursor *cur, MDB_val *key, MDB_val *val,
                            int kforward, int kstart, int kend,
                            MDB_val *start_key, MDB_val *end_key,
                            int vforward, int vstart, int vend,
                            MDB_val *start_val, MDB_val *end_val);

 /** @brief A function to indicate if the iterator has the next item.
  *
  * @param[in] iter The iterator handle.
  * @return DTLV_TRUE on true and DTLV_FALSE on false.
  */
  int dtlv_list_iter_has_next(dtlv_list_iter *iter);

 /** @brief A function to return the next item.
  *
  * @param[in] iter The iterator handle.
  * @param[out] key The key of the next item.
  * @param[out] val The value of the next item.
  */
  void dtlv_list_iter_next(dtlv_list_iter *iter, MDB_val *key, MDB_val *val);

 /** @brief A function to destroy the list iterator.
  *
  * @param[in] iter The iterator handle.
  */
  void dtlv_list_iter_destroy(dtlv_list_iter *iter);

 /** @brief Opaque structure for a list value iterator
  *
  * Iterate values (forward only currently) of keys for a dupsort DBI.
  */
  typedef struct dtlv_list_val_iter dtlv_list_val_iter;

 /** @brief A function to create a list values iterator.
  *
  * @param[out] iter The address where the iterator will be stored.
  * @param[in] cur The cursor.
  * @param[in] key Holder for the key.
  * @param[in] val Holder for the value.
  * @param[in] vstart if to include (DTLV_TRUE) or not the start_val.
  * @param[in] vend if to include (DTLV_TRUE) or not the end_val.
  * @param[in] start_val The start value..
  * @param[in] end_val The end value.
  * @return A non-zero error value on failure and 0 on success.
  */
  int dtlv_list_val_iter_create(dtlv_list_val_iter **iter,
                                MDB_cursor *cur, MDB_val *key, MDB_val *val,
                                int vstart, int vend,
                                MDB_val *start_val, MDB_val *end_val);

 /** @brief A function to seek to a key.
  *
  * @param[in] iter The iterator handle.
  * @param[in] k The key to seek.
  * @return DTLV_TRUE on found and DTLV_FALSE when not found.
  */
  int dtlv_list_val_iter_seek(dtlv_list_val_iter *iter, MDB_val *k);

 /** @brief A function to indicate if the iterator has the next item.
  *
  * @param[in] iter The iterator handle.
  * @return DTLV_TRUE on true and DTLV_FALSE on false.
  */
  int dtlv_list_val_iter_has_next(dtlv_list_val_iter *iter);

 /** @brief A function to return the next item.
  *
  * @param[in] iter The iterator handle.
  * @param[out] val The next value.
  */
  void dtlv_list_val_iter_next(dtlv_list_val_iter *iter, MDB_val *val);

 /** @brief A function to destroy the list iterator.
  *
  * @param[in] iter The iterator handle.
  */
  void dtlv_list_val_iter_destroy(dtlv_list_val_iter *iter);

#ifdef __cplusplus
}
#endif
