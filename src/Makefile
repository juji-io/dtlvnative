lib_lmdb       := lmdb/libraries/liblmdb
lib_usearch    := usearch
lib_llama      := llama.cpp
lib_dtlv       := libdtlv
lib_dtlv_embed := libdtlv_embed

usearch_build := $(lib_usearch)/build_release
llama_build   := $(lib_llama)/build_release

libraries := $(lib_lmdb) $(lib_usearch) $(lib_llama) \
             $(lib_dtlv)$(SOEXT) $(lib_dtlv_embed)$(SOEXT)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ XCFLAGS="-fPIC" libdlmdb.a
		cp $(lib_lmdb)/libdlmdb.a .

$(lib_usearch):
		cp $(usearch_build)/libusearch_static_c.a ./libusearch.a

$(lib_llama):
		cp $(llama_build)/src/libllama.a .
		cp $(llama_build)/ggml/src/libggml.a .
		cp $(llama_build)/ggml/src/libggml-base.a .
		cp $(llama_build)/ggml/src/libggml-cpu.a .

$(lib_dtlv)$(SOEXT): $(lib_lmdb) $(lib_usearch)
		$(CC) -pthread -fPIC -O2 -Wall -g -c dtlv.c -o dtlv.o
	  $(CXX) $(LDFLAGS) -pthread -shared -fPIC -o $@ dtlv.o -L. -ldlmdb $(OMP_LIB) $(USEARCH_LIB)

$(lib_dtlv_embed)$(SOEXT): $(lib_llama)
		$(CXX) $(CPPFLAGS) -std=c++17 -pthread -fPIC -O2 -Wall -g \
			-I$(lib_llama)/include -I$(lib_llama)/ggml/include \
			-c dtlv_embed.cpp -o dtlv_embed.o
		$(CXX) $(LDFLAGS) $(LLAMA_LINK_FLAGS) -pthread -shared -fPIC -o $@ \
			dtlv_embed.o libllama.a libggml.a libggml-cpu.a libggml-base.a \
			$(LLAMA_EXTRA_LIBS)

clean:
		$(RM) *.o *.a *.so *.dylib
		$(MAKE) --directory=$(lib_lmdb) clean
