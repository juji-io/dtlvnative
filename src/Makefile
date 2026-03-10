lib_lmdb    := lmdb/libraries/liblmdb
lib_usearch := usearch
lib_llama   := llama.cpp
lib_dtlv    := libdtlv
libraries := $(lib_lmdb) $(lib_usearch) $(lib_llama) $(lib_dtlv)$(SOEXT)

CPPFLAGS += -Illama.cpp/include -Illama.cpp/ggml/include
LLAMA_LIB ?= -lllama -lggml -lggml-cpu -lggml-base

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ XCFLAGS="-fPIC" libdlmdb.a
		cp $(lib_lmdb)/libdlmdb.a .

$(lib_usearch):
		cp $(lib_usearch)/build_release/libusearch_static_c.a ./libusearch.a

$(lib_llama):
		cp $(lib_llama)/build_release/src/libllama.a ./libllama.a
		cp $(lib_llama)/build_release/ggml/src/libggml.a ./libggml.a
		cp $(lib_llama)/build_release/ggml/src/libggml-base.a ./libggml-base.a
		cp $(lib_llama)/build_release/ggml/src/libggml-cpu.a ./libggml-cpu.a

$(lib_dtlv)$(SOEXT): $(lib_lmdb) $(lib_usearch) $(lib_llama)
		$(CC) $(CPPFLAGS) -pthread -fPIC -O2 -Wall -g -c dtlv.c -o dtlv.o
		$(CXX) $(LDFLAGS) -pthread -shared -fPIC -o $@ dtlv.o -L. -ldlmdb $(OMP_LIB) $(USEARCH_LIB) $(LLAMA_LIB)

clean:
		$(RM) *.o *.a *.so *.dylib
		$(MAKE) --directory=$(lib_lmdb) clean
