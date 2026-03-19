/** @file dtlv.h
 *	Native supporting functions for Datalevin: a simple, fast and
 *  versatile Datalog database.
 *
 *  Datalevin works with LMDB, a Btree based key value store. This library
 *  provides an iterator interface to LMDB.
 *
 *	@author	Huahai Yang
 *
 *	@copyright Copyright 2020-2025. Huahai Yang. All rights reserved.
 *
 *  This code is released under Eclipse Public License 2.0.
 */

#include "lmdb/libraries/liblmdb/dlmdb.h"
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
   * Opaque structure for a rank based key sample iterator.
   */
  typedef struct dtlv_key_rank_sample_iter dtlv_key_rank_sample_iter;

  /**
   * Create a rank based key sample iterator. Iteration is forward only with
   * inclusive start/end key boundaries and requires a counted DB. On dupsort
   * DBs ranks are over key/value pairs (duplicate keys will surface multiple
   * times).
   *
   * @param iter The address where the iterator will be stored.
   * @param indices The array of strictly increasing sample indices relative to
   *                the range start.
   * @param samples The number of indices.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param start_key Optional inclusive start key, may be NULL.
   * @param end_key Optional inclusive end key, may be NULL.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_key_rank_sample_iter_create(dtlv_key_rank_sample_iter **iter,
                                       size_t *indices, int samples,
                                       MDB_cursor *cur, MDB_val *key,
                                       MDB_val *val, MDB_val *start_key,
                                       MDB_val *end_key);

  /**
   * Advance the rank based key sample iterator.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE when a sample has been materialized into key/val.
   */
  int dtlv_key_rank_sample_iter_has_next(dtlv_key_rank_sample_iter *iter);

  /**
   * Destroy the rank based key sample iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_key_rank_sample_iter_destroy(dtlv_key_rank_sample_iter *iter);

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
   * @param start_val Inclusive start value bound (optional).
   * @param end_val Inclusive end value bound (optional).
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_list_val_iter_create(dtlv_list_val_iter **iter,
                                MDB_cursor *cur, MDB_val *key, MDB_val *val,
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
   * Opaque structure for iterating a key range while visiting every value per
   * key using full duplicate spans.
   */
  typedef struct dtlv_list_key_range_full_val_iter
      dtlv_list_key_range_full_val_iter;

  /**
   * Create a key-range iterator that walks all values for each key without
   * performing value range checks.
   *
   * @param iter The address where the iterator will be stored.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param kstart if to include (DTLV_TRUE) or not the start_key.
   * @param kend if to include (DTLV_TRUE) or not the end_key.
   * @param start_key Optional start key.
   * @param end_key Optional end key.
   * @return MDB_SUCCESS or an error code.
   */
  int dtlv_list_key_range_full_val_iter_create(
      dtlv_list_key_range_full_val_iter **iter, MDB_cursor *cur,
      MDB_val *key, MDB_val *val, int kstart, int kend,
      MDB_val *start_key, MDB_val *end_key);

  /**
   * Advance the full-value key range iterator.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE when positioned at a new entry, DTLV_FALSE when exhausted.
   */
  int dtlv_list_key_range_full_val_iter_has_next(
      dtlv_list_key_range_full_val_iter *iter);

  /**
   * Destroy the full-value key range iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_list_key_range_full_val_iter_destroy(
      dtlv_list_key_range_full_val_iter *iter);

  /**
   * Opaque structure for a rank based list sample iterator.
   */
  typedef struct dtlv_list_rank_sample_iter dtlv_list_rank_sample_iter;

  /**
   * Create a rank based list sample iterator. This iterator assumes forward
   * iteration with inclusive start/end boundaries and leverages cursor ranks.
   *
   * @param iter The address where the iterator will be stored.
   * @param indices The array of strictly increasing sample indices relative to
   *                the range start.
   * @param samples The number of sample indices.
   * @param cur The cursor.
   * @param key Holder for the key.
   * @param val Holder for the value.
   * @param start_key Optional inclusive start key.
   * @param end_key Optional inclusive end key.
   * @return A non-zero error value on failure and 0 on success.
   */
  int dtlv_list_rank_sample_iter_create(dtlv_list_rank_sample_iter **iter,
                                        size_t *indices, int samples,
                                        MDB_cursor *cur, MDB_val *key,
                                        MDB_val *val, MDB_val *start_key,
                                        MDB_val *end_key);

  /**
   * Advance the rank based sample iterator.
   *
   * @param iter The iterator handle.
   * @return DTLV_TRUE when a sample has been materialized into key/val.
   */
  int dtlv_list_rank_sample_iter_has_next(dtlv_list_rank_sample_iter *iter);

  /**
   * Destroy the rank based sample iterator.
   *
   * @param iter The iterator handle.
   */
  void dtlv_list_rank_sample_iter_destroy(dtlv_list_rank_sample_iter *iter);

  /**
   * Opaque llama.cpp embedding handle.
   */
  typedef struct dtlv_llama_embedder dtlv_llama_embedder;

  /**
   * Create a CPU-only llama.cpp embedder backed by a GGUF model.
   *
   * @param embedder The address where the embedder will be stored.
   * @param model_path Path to a GGUF embedding model.
   * @param n_ctx Context size. Use 0 to default to the model training context.
   * @param n_batch Max tokens accepted per embedding request. Use 0 to mirror
   *                the context size.
   * @param n_threads CPU thread count. Use 0 to keep llama.cpp defaults.
   * @param normalize Non-zero to L2 normalize returned embeddings.
   * @return MDB_SUCCESS or an error code.
   */
  int dtlv_llama_embedder_create(dtlv_llama_embedder **embedder,
                                 const char *model_path,
                                 int n_ctx,
                                 int n_batch,
                                 int n_threads,
                                 int normalize);

  /**
   * Return the embedding width for this embedder.
   *
   * @param embedder The embedder handle.
   * @return The number of floats in each embedding, or -1 on invalid input.
   */
  int dtlv_llama_embedder_n_embd(dtlv_llama_embedder *embedder);

  /**
   * Return the number of tokens for a UTF-8 string.
   *
   * @param embedder The embedder handle.
   * @param text The UTF-8 text to tokenize.
   * @return The token count, or a negative errno on error.
   */
  int dtlv_llama_token_count(dtlv_llama_embedder *embedder,
                              const char *text);

  /**
   * Return the context size (max tokens) for this embedder.
   *
   * @param embedder The embedder handle.
   * @return The context size, or -1 on invalid input.
   */
  int dtlv_llama_embedder_n_ctx(dtlv_llama_embedder *embedder);

  /**
   * Tokenize a UTF-8 string into a caller-owned token buffer.
   *
   * @param embedder The embedder handle.
   * @param text The UTF-8 text to tokenize.
   * @param tokens Caller-owned token buffer.
   * @param n_tokens_max Capacity of the token buffer.
   * @return Number of tokens written, or a negative errno on error.
   *         -EMSGSIZE if the buffer is too small (absolute value is
   *         the required size).
   */
  int dtlv_llama_tokenize(dtlv_llama_embedder *embedder,
                           const char *text,
                           int *tokens,
                           int n_tokens_max);

  /**
   * Convert tokens back to a UTF-8 string.
   *
   * @param embedder The embedder handle.
   * @param tokens Array of token ids.
   * @param n_tokens Number of tokens.
   * @param text Caller-owned output buffer.
   * @param text_len_max Capacity of the text buffer in bytes.
   * @return Number of bytes written, or a negative errno on error.
   *         -EMSGSIZE if the buffer is too small (absolute value is
   *         the required size).
   */
  int dtlv_llama_detokenize(dtlv_llama_embedder *embedder,
                             const int *tokens,
                             int n_tokens,
                             char *text,
                             int text_len_max);

  /**
   * Compute an embedding for a single UTF-8 string.
   *
   * Text that exceeds the context/batch token limit is automatically
   * truncated (only the leading tokens are kept).
   *
   * @param embedder The embedder handle.
   * @param text The UTF-8 text to embed.
   * @param output Caller-owned float buffer.
   * @param output_len Number of floats available in output.
   * @return MDB_SUCCESS or an error code.
   */
  int dtlv_llama_embed(dtlv_llama_embedder *embedder,
                       const char *text,
                       float *output,
                       size_t output_len);

  /**
   * Compute embeddings for multiple UTF-8 strings in a single batch.
   *
   * Individual texts that exceed the context/batch token limit are
   * automatically truncated (only the leading tokens are kept).
   * EMSGSIZE is returned only when the combined token count of all
   * (possibly truncated) texts still exceeds the batch/context capacity.
   *
   * @param embedder The embedder handle.
   * @param texts Array of UTF-8 strings to embed.
   * @param n_texts Number of strings.
   * @param output Caller-owned float buffer of size n_texts * n_embd.
   * @param output_len Total number of floats available in output.
   * @return MDB_SUCCESS or an error code.
   */
  int dtlv_llama_embed_batch(dtlv_llama_embedder *embedder,
                              const char **texts,
                              int n_texts,
                              float *output,
                              size_t output_len);

  /**
   * Destroy a llama.cpp embedder.
   *
   * @param embedder The embedder handle.
   */
  void dtlv_llama_embedder_destroy(dtlv_llama_embedder *embedder);

  /**
   * Opaque llama.cpp text-generation handle.
   */
  typedef struct dtlv_llama_generator dtlv_llama_generator;

  /**
   * Create a CPU-only llama.cpp generator backed by a decoder-only GGUF model.
   *
   * This API is intended for prompt-based text generation such as document
   * summarization. Encoder-only embedding models are not supported here.
   *
   * @param generator The address where the generator will be stored.
   * @param model_path Path to a GGUF decoder-only text model.
   * @param n_ctx Context size. Use 0 to default to the model training context.
   * @param n_batch Max prompt tokens processed per decode call. Use 0 to mirror
   *                the context size.
   * @param n_threads CPU thread count. Use 0 to keep llama.cpp defaults.
   * @return MDB_SUCCESS or an error code.
   */
  int dtlv_llama_generator_create(dtlv_llama_generator **generator,
                                  const char *model_path,
                                  int n_ctx,
                                  int n_batch,
                                  int n_threads);

  /**
   * Return the context size (max tokens) for this generator.
   *
   * @param generator The generator handle.
   * @return The context size, or -1 on invalid input.
   */
  int dtlv_llama_generator_n_ctx(dtlv_llama_generator *generator);

  /**
   * Return the number of tokens for a UTF-8 string.
   *
   * @param generator The generator handle.
   * @param text The UTF-8 text to tokenize.
   * @return The token count, or a negative errno on error.
   */
  int dtlv_llama_generator_token_count(dtlv_llama_generator *generator,
                                       const char *text);

  /**
   * Generate text for a raw prompt.
   *
   * Prompt text that exceeds the context size is automatically truncated
   * (only the leading tokens are kept). When n_predict <= 0, a default budget
   * of 128 generated tokens is used.
   *
   * @param generator The generator handle.
   * @param prompt The UTF-8 prompt text.
   * @param n_predict Maximum number of tokens to generate.
   * @param output Caller-owned output buffer.
   * @param output_len Capacity of the output buffer in bytes.
   * @return Number of bytes written, or a negative errno on error.
   *         -EMSGSIZE if the output buffer is too small.
   */
  int dtlv_llama_generate(dtlv_llama_generator *generator,
                          const char *prompt,
                          int n_predict,
                          char *output,
                          size_t output_len);

  /**
   * Generate a summary for a UTF-8 document.
   *
   * This helper formats a concise summarization prompt, using the model's
   * built-in chat template when one is present.
   *
   * Text that exceeds the context size is automatically truncated
   * (only the leading tokens are kept). When n_predict <= 0, a default budget
   * of 128 generated tokens is used.
   *
   * @param generator The generator handle.
   * @param text The UTF-8 source document.
   * @param n_predict Maximum number of tokens to generate.
   * @param output Caller-owned output buffer.
   * @param output_len Capacity of the output buffer in bytes.
   * @return Number of bytes written, or a negative errno on error.
   *         -EMSGSIZE if the output buffer is too small.
   */
  int dtlv_llama_summarize(dtlv_llama_generator *generator,
                           const char *text,
                           int n_predict,
                           char *output,
                           size_t output_len);

  /**
   * Destroy a llama.cpp generator.
   *
   * @param generator The generator handle.
   */
  void dtlv_llama_generator_destroy(dtlv_llama_generator *generator);


#ifdef __cplusplus
}
#endif
