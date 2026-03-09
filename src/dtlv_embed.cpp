#include "dtlv_embed.h"

#include <algorithm>
#include <cstring>
#include <mutex>
#include <new>
#include <string>
#include <vector>

#include "llama.h"

namespace {

thread_local std::string g_last_error;
std::once_flag g_backend_once;

void set_last_error(const char * msg) {
  g_last_error = msg ? msg : "";
}

void clear_last_error() {
  g_last_error.clear();
}

void ensure_backend_init() {
  std::call_once(g_backend_once, []() {
    llama_backend_init();
  });
}

int tokenize_text(const llama_vocab * vocab,
                  const char * text,
                  std::vector<llama_token> & tokens) {
  int32_t needed = llama_tokenize(vocab, text, (int32_t) std::strlen(text),
                                  nullptr, 0, true, true);
  if (needed == INT32_MIN) {
    set_last_error("llama tokenization overflow");
    return -1;
  }

  tokens.resize((size_t) (needed >= 0 ? needed : -needed));
  if (tokens.empty()) {
    set_last_error("llama tokenization produced no tokens");
    return -1;
  }

  int32_t count = llama_tokenize(vocab, text, (int32_t) std::strlen(text),
                                 tokens.data(), (int32_t) tokens.size(),
                                 true, true);
  if (count < 0 || count > (int32_t) tokens.size()) {
    set_last_error("llama tokenization failed");
    return -1;
  }

  tokens.resize((size_t) count);
  return count;
}

}  // namespace

struct dtlv_llama_model {
  llama_model * handle;
};

struct dtlv_llama_context {
  dtlv_llama_model * model;
  llama_context * handle;
  int32_t n_embd_out;
  enum llama_pooling_type pooling_type;
};

namespace {

int run_embedding_batch(dtlv_llama_context * ctx,
                        const std::vector<std::vector<llama_token>> & tokenized,
                        size_t start,
                        size_t count,
                        float * output) {
  int total_tokens = 0;
  for (size_t i = 0; i < count; ++i) {
    total_tokens += (int) tokenized[start + i].size();
  }

  llama_batch batch = llama_batch_init(total_tokens, 0, (int32_t) count);
  batch.n_tokens = total_tokens;

  int batch_idx = 0;
  for (size_t seq = 0; seq < count; ++seq) {
    const auto & tokens = tokenized[start + seq];
    for (size_t pos = 0; pos < tokens.size(); ++pos, ++batch_idx) {
      batch.token[batch_idx] = tokens[pos];
      batch.pos[batch_idx] = (llama_pos) pos;
      batch.n_seq_id[batch_idx] = 1;
      batch.seq_id[batch_idx][0] = (llama_seq_id) seq;
      batch.logits[batch_idx] = 1;
    }
  }

  if (llama_memory_t memory = llama_get_memory(ctx->handle)) {
    llama_memory_clear(memory, true);
  }

  const int rc = llama_decode(ctx->handle, batch);
  if (rc != 0) {
    llama_batch_free(batch);
    set_last_error("failed to run llama embedding graph");
    return rc;
  }

  llama_synchronize(ctx->handle);

  for (size_t seq = 0; seq < count; ++seq) {
    float * embd = llama_get_embeddings_seq(ctx->handle, (llama_seq_id) seq);
    if (!embd) {
      llama_batch_free(batch);
      set_last_error("failed to fetch llama sequence embedding");
      return -1;
    }

    std::memcpy(output + seq * (size_t) ctx->n_embd_out,
                embd,
                (size_t) ctx->n_embd_out * sizeof(float));
  }

  llama_batch_free(batch);
  return 0;
}

}  // namespace

extern "C" {

static dtlv_llama_context * dtlv_llama_context_create_impl(dtlv_llama_model * model,
                                                           uint32_t n_ctx,
                                                           uint32_t n_batch,
                                                           uint32_t n_seq_max,
                                                           int32_t n_threads,
                                                           int32_t n_threads_batch);

dtlv_llama_model * dtlv_llama_model_load(const char * model_path,
                                         int use_mmap,
                                         int use_mlock) {
  clear_last_error();
  if (!model_path) {
    set_last_error("model path is required");
    return nullptr;
  }

  try {
    ensure_backend_init();

    llama_model_params params = llama_model_default_params();
    params.use_mmap = use_mmap != 0;
    params.use_mlock = use_mlock != 0;

    llama_model * model = llama_model_load_from_file(model_path, params);
    if (!model) {
      set_last_error("failed to load llama model");
      return nullptr;
    }

    dtlv_llama_model * wrapped = new (std::nothrow) dtlv_llama_model();
    if (!wrapped) {
      llama_model_free(model);
      set_last_error("failed to allocate model wrapper");
      return nullptr;
    }

    wrapped->handle = model;
    return wrapped;
  } catch (const std::exception & e) {
    set_last_error(e.what());
    return nullptr;
  } catch (...) {
    set_last_error("unexpected llama model load failure");
    return nullptr;
  }
}

void dtlv_llama_model_destroy(dtlv_llama_model * model) {
  if (!model) return;
  if (model->handle) {
    llama_model_free(model->handle);
  }
  delete model;
}

int dtlv_llama_model_n_ctx_train(const dtlv_llama_model * model) {
  if (!model || !model->handle) return 0;
  return llama_model_n_ctx_train(model->handle);
}

int dtlv_llama_model_n_embd_out(const dtlv_llama_model * model) {
  if (!model || !model->handle) return 0;
  return llama_model_n_embd_out(model->handle);
}

int dtlv_llama_model_has_encoder(const dtlv_llama_model * model) {
  if (!model || !model->handle) return 0;
  return llama_model_has_encoder(model->handle) ? 1 : 0;
}

int dtlv_llama_model_has_decoder(const dtlv_llama_model * model) {
  if (!model || !model->handle) return 0;
  return llama_model_has_decoder(model->handle) ? 1 : 0;
}

static dtlv_llama_context * dtlv_llama_context_create_impl(dtlv_llama_model * model,
                                                           uint32_t n_ctx,
                                                           uint32_t n_batch,
                                                           uint32_t n_seq_max,
                                                           int32_t n_threads,
                                                           int32_t n_threads_batch) {
  clear_last_error();
  if (!model || !model->handle) {
    set_last_error("model is required");
    return nullptr;
  }

  try {
    llama_context_params params = llama_context_default_params();
    if (n_ctx > 0) {
      params.n_ctx = n_ctx;
    } else {
      const uint32_t n_ctx_train = (uint32_t) llama_model_n_ctx_train(model->handle);
      if (n_ctx_train > 0) {
        params.n_ctx = n_ctx_train;
      }
    }
    if (n_batch > 0) {
      params.n_batch = n_batch;
      params.n_ubatch = n_batch;
    }
    if (n_threads > 0) {
      params.n_threads = n_threads;
    }
    if (n_threads_batch > 0) {
      params.n_threads_batch = n_threads_batch;
    } else if (n_threads > 0) {
      params.n_threads_batch = n_threads;
    }
    const uint32_t max_parallel =
      (uint32_t) std::max<size_t>(1, llama_max_parallel_sequences());
    const uint32_t requested_seq_max = n_seq_max > 0 ? n_seq_max : 1;
    if (params.n_batch > 0) {
      params.n_seq_max =
        std::min<uint32_t>(std::min<uint32_t>(requested_seq_max, params.n_batch),
                           max_parallel);
    } else {
      params.n_seq_max = std::min<uint32_t>(requested_seq_max, max_parallel);
    }
    params.embeddings = true;
    params.pooling_type = LLAMA_POOLING_TYPE_UNSPECIFIED;

    llama_context * ctx = llama_init_from_model(model->handle, params);
    if (!ctx) {
      set_last_error("failed to initialize llama context");
      return nullptr;
    }

    enum llama_pooling_type pooling_type = llama_pooling_type(ctx);
    if (pooling_type == LLAMA_POOLING_TYPE_NONE) {
      llama_free(ctx);
      set_last_error("token-level embeddings are not supported by this wrapper");
      return nullptr;
    }

    dtlv_llama_context * wrapped = new (std::nothrow) dtlv_llama_context();
    if (!wrapped) {
      llama_free(ctx);
      set_last_error("failed to allocate context wrapper");
      return nullptr;
    }

    wrapped->model = model;
    wrapped->handle = ctx;
    wrapped->n_embd_out = llama_model_n_embd_out(model->handle);
    wrapped->pooling_type = pooling_type;
    return wrapped;
  } catch (const std::exception & e) {
    set_last_error(e.what());
    return nullptr;
  } catch (...) {
    set_last_error("unexpected llama context initialization failure");
    return nullptr;
  }
}

dtlv_llama_context * dtlv_llama_context_create(dtlv_llama_model * model,
                                               uint32_t n_ctx,
                                               uint32_t n_batch,
                                               int32_t n_threads,
                                               int32_t n_threads_batch) {
  return dtlv_llama_context_create_impl(model, n_ctx, n_batch, 1,
                                        n_threads, n_threads_batch);
}

dtlv_llama_context * dtlv_llama_context_create_nseq(dtlv_llama_model * model,
                                                    uint32_t n_ctx,
                                                    uint32_t n_batch,
                                                    uint32_t n_seq_max,
                                                    int32_t n_threads,
                                                    int32_t n_threads_batch) {
  return dtlv_llama_context_create_impl(model, n_ctx, n_batch, n_seq_max,
                                        n_threads, n_threads_batch);
}

void dtlv_llama_context_destroy(dtlv_llama_context * ctx) {
  if (!ctx) return;
  if (ctx->handle) {
    llama_free(ctx->handle);
  }
  delete ctx;
}

int dtlv_llama_context_pooling_type(const dtlv_llama_context * ctx) {
  if (!ctx) return LLAMA_POOLING_TYPE_UNSPECIFIED;
  return (int) ctx->pooling_type;
}

int dtlv_llama_context_n_embd_out(const dtlv_llama_context * ctx) {
  if (!ctx) return 0;
  return ctx->n_embd_out;
}

int dtlv_llama_embed_text(dtlv_llama_context * ctx,
                          const char * text,
                          float * output,
                          size_t output_len) {
  clear_last_error();
  if (!ctx || !ctx->handle) {
    set_last_error("context is required");
    return -1;
  }
  if (!text) {
    set_last_error("text is required");
    return -1;
  }
  if (!output) {
    set_last_error("output buffer is required");
    return -1;
  }
  if (output_len < (size_t) ctx->n_embd_out) {
    set_last_error("output buffer is too small");
    return -1;
  }

  try {
    const llama_model * model = llama_get_model(ctx->handle);
    if (!model) {
      set_last_error("context has no model");
      return -1;
    }

    if (llama_model_has_encoder(model) && llama_model_has_decoder(model)) {
      set_last_error("encoder-decoder embedding models are not supported");
      return -1;
    }

    const llama_vocab * vocab = llama_model_get_vocab(model);
    if (!vocab) {
      set_last_error("model has no vocabulary");
      return -1;
    }

    std::vector<llama_token> tokens;
    const int count = tokenize_text(vocab, text, tokens);
    if (count <= 0) {
      return -1;
    }

    const std::vector<std::vector<llama_token>> tokenized = { std::move(tokens) };
    const int rc = run_embedding_batch(ctx, tokenized, 0, 1, output);
    if (rc != 0) {
      return rc;
    }
    return 0;
  } catch (const std::exception & e) {
    set_last_error(e.what());
    return -1;
  } catch (...) {
    set_last_error("unexpected llama embedding failure");
    return -1;
  }
}

int dtlv_llama_embed_texts(dtlv_llama_context * ctx,
                           const char * const * texts,
                           size_t text_count,
                           float * output,
                           size_t output_len) {
  clear_last_error();
  if (!ctx || !ctx->handle) {
    set_last_error("context is required");
    return -1;
  }
  if (!texts && text_count > 0) {
    set_last_error("texts are required");
    return -1;
  }
  if (!output && text_count > 0) {
    set_last_error("output buffer is required");
    return -1;
  }

  const size_t needed = text_count * (size_t) ctx->n_embd_out;
  if (output_len < needed) {
    set_last_error("output buffer is too small");
    return -1;
  }

  if (text_count == 0) {
    return 0;
  }

  try {
    const llama_model * model = llama_get_model(ctx->handle);
    if (!model) {
      set_last_error("context has no model");
      return -1;
    }

    if (llama_model_has_encoder(model) && llama_model_has_decoder(model)) {
      set_last_error("encoder-decoder embedding models are not supported");
      return -1;
    }

    const llama_vocab * vocab = llama_model_get_vocab(model);
    if (!vocab) {
      set_last_error("model has no vocabulary");
      return -1;
    }

    const size_t max_tokens = (size_t) llama_n_batch(ctx->handle);
    const size_t max_seqs = (size_t) llama_n_seq_max(ctx->handle);
    if (max_tokens == 0 || max_seqs == 0) {
      set_last_error("context batching parameters are invalid");
      return -1;
    }

    std::vector<std::vector<llama_token>> tokenized(text_count);
    for (size_t i = 0; i < text_count; ++i) {
      if (!texts[i]) {
        set_last_error("text is required");
        return -1;
      }
      const int count = tokenize_text(vocab, texts[i], tokenized[i]);
      if (count <= 0) {
        return -1;
      }
      if ((size_t) count > max_tokens) {
        set_last_error("text token count exceeds context batch size");
        return -1;
      }
    }

    size_t start = 0;
    while (start < text_count) {
      size_t chunk_count = 0;
      size_t chunk_tokens = 0;

      while (start + chunk_count < text_count && chunk_count < max_seqs) {
        const size_t next_tokens = tokenized[start + chunk_count].size();
        if (chunk_count > 0 && chunk_tokens + next_tokens > max_tokens) {
          break;
        }
        chunk_tokens += next_tokens;
        chunk_count += 1;
      }

      if (chunk_count == 0) {
        set_last_error("failed to build embedding batch");
        return -1;
      }

      const int rc =
        run_embedding_batch(ctx, tokenized, start, chunk_count,
                            output + start * (size_t) ctx->n_embd_out);
      if (rc != 0) {
        return rc;
      }

      start += chunk_count;
    }

    return 0;
  } catch (const std::exception & e) {
    set_last_error(e.what());
    return -1;
  } catch (...) {
    set_last_error("unexpected llama batched embedding failure");
    return -1;
  }
}

const char * dtlv_llama_last_error(void) {
  return g_last_error.c_str();
}

}  // extern "C"
