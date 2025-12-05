#include "dtlv_usearch_jni_bridge.h"

extern "C" usearch_index_t dtlv_usearch_init_cpp(usearch_init_options_t* options, usearch_error_t* error) {
  return dtlv_usearch_init_cpp_impl(options, error);
}

