#pragma once

#include <thread>

#include <usearch/index_dense.hpp>

extern "C" {
#include "usearch.h"
}

/* Helpers mirroring the mappings in the C API but exposed for JavaCPP-only
 * bindings, so we can construct the index directly from C++ without going
 * through the C wrapper that crashes on some Windows hosts. */
static inline unum::usearch::metric_kind_t dtlv_metric_kind_to_cpp(usearch_metric_kind_t kind) {
  using namespace unum::usearch;
  switch (kind) {
  case usearch_metric_ip_k: return metric_kind_t::ip_k;
  case usearch_metric_l2sq_k: return metric_kind_t::l2sq_k;
  case usearch_metric_cos_k: return metric_kind_t::cos_k;
  case usearch_metric_haversine_k: return metric_kind_t::haversine_k;
  case usearch_metric_divergence_k: return metric_kind_t::divergence_k;
  case usearch_metric_pearson_k: return metric_kind_t::pearson_k;
  case usearch_metric_jaccard_k: return metric_kind_t::jaccard_k;
  case usearch_metric_hamming_k: return metric_kind_t::hamming_k;
  case usearch_metric_tanimoto_k: return metric_kind_t::tanimoto_k;
  case usearch_metric_sorensen_k: return metric_kind_t::sorensen_k;
  default: return metric_kind_t::unknown_k;
  }
}

static inline unum::usearch::scalar_kind_t dtlv_scalar_kind_to_cpp(usearch_scalar_kind_t kind) {
  using namespace unum::usearch;
  switch (kind) {
  case usearch_scalar_f32_k: return scalar_kind_t::f32_k;
  case usearch_scalar_f64_k: return scalar_kind_t::f64_k;
  case usearch_scalar_f16_k: return scalar_kind_t::f16_k;
  case usearch_scalar_bf16_k: return scalar_kind_t::bf16_k;
  case usearch_scalar_i8_k: return scalar_kind_t::i8_k;
  case usearch_scalar_b1_k: return scalar_kind_t::b1x8_k;
  default: return scalar_kind_t::unknown_k;
  }
}

static inline usearch_index_t dtlv_usearch_init_cpp(usearch_init_options_t* options, usearch_error_t* error) {
  using namespace unum::usearch;

  if (!options) {
    if (error) *error = "Missing init options!";
    return NULL;
  }

  index_dense_config_t config(static_cast<std::size_t>(options->connectivity),
                              static_cast<std::size_t>(options->expansion_add),
                              static_cast<std::size_t>(options->expansion_search));
  config.multi = options->multi;
  if (config.connectivity_base < config.connectivity)
    config.connectivity_base = config.connectivity * 2;

  metric_kind_t metric_kind = dtlv_metric_kind_to_cpp(options->metric_kind);
  scalar_kind_t scalar_kind = dtlv_scalar_kind_to_cpp(options->quantization);
  metric_punned_t metric = !options->metric
                               ? metric_punned_t::builtin(options->dimensions, metric_kind, scalar_kind)
                               : metric_punned_t::stateless(
                                     options->dimensions, reinterpret_cast<std::uintptr_t>(options->metric),
                                     metric_punned_signature_t::array_array_k, metric_kind, scalar_kind);
  if (metric.missing()) {
    if (error) *error = "Unknown metric kind!";
    return NULL;
  }

  index_dense_t::state_result_t state = index_dense_t::make(metric, config);
  if (!state) {
    if (error) *error = state.error.release();
    return NULL;
  }

  index_dense_t* result_ptr = new index_dense_t(std::move(state.index));
  if (!result_ptr) {
    if (error) *error = "Out of memory!";
    return NULL;
  }

  std::size_t threads = std::thread::hardware_concurrency();
  if (threads == 0)
    threads = 1;
  index_limits_t limits{0};
  limits.threads_add = threads;
  limits.threads_search = threads;
  if (!result_ptr->try_reserve(limits)) {
    if (error) *error = "Failed to prepare contexts!";
    delete result_ptr;
    return NULL;
  }

  return result_ptr;
}
