/** @file dtlv_embed.h
 *  Native embedding support for Datalevin backed by llama.cpp.
 */

#ifndef DTLV_EMBED_H
#define DTLV_EMBED_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct dtlv_llama_model dtlv_llama_model;
typedef struct dtlv_llama_context dtlv_llama_context;

dtlv_llama_model * dtlv_llama_model_load(const char * model_path,
                                         int use_mmap,
                                         int use_mlock);

void dtlv_llama_model_destroy(dtlv_llama_model * model);

int dtlv_llama_model_n_ctx_train(const dtlv_llama_model * model);
int dtlv_llama_model_n_embd_out(const dtlv_llama_model * model);
int dtlv_llama_model_has_encoder(const dtlv_llama_model * model);
int dtlv_llama_model_has_decoder(const dtlv_llama_model * model);

dtlv_llama_context * dtlv_llama_context_create(dtlv_llama_model * model,
                                               uint32_t n_ctx,
                                               uint32_t n_batch,
                                               int32_t n_threads,
                                               int32_t n_threads_batch);

dtlv_llama_context * dtlv_llama_context_create_nseq(dtlv_llama_model * model,
                                                    uint32_t n_ctx,
                                                    uint32_t n_batch,
                                                    uint32_t n_seq_max,
                                                    int32_t n_threads,
                                                    int32_t n_threads_batch);

void dtlv_llama_context_destroy(dtlv_llama_context * ctx);

int dtlv_llama_context_pooling_type(const dtlv_llama_context * ctx);
int dtlv_llama_context_n_embd_out(const dtlv_llama_context * ctx);

int dtlv_llama_embed_text(dtlv_llama_context * ctx,
                          const char * text,
                          float * output,
                          size_t output_len);

int dtlv_llama_embed_texts(dtlv_llama_context * ctx,
                           const char * const * texts,
                           size_t text_count,
                           float * output,
                           size_t output_len);

const char * dtlv_llama_last_error(void);

#ifdef __cplusplus
}
#endif

#endif
