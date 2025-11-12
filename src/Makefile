lib_lmdb    := lmdb/libraries/liblmdb
lib_usearch := usearch
lib_dtlv  := libdtlv
libraries := $(lib_lmdb) $(lib_usearch) $(lib_dtlv)$(SOEXT)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ XCFLAGS="-fPIC" liblmdb.a
		cp $(lib_lmdb)/liblmdb.a .

$(lib_usearch):
		cp $(lib_usearch)/build_release/libusearch_static_c.a ./libusearch.a

$(lib_dtlv)$(SOEXT): $(lib_lmdb) $(lib_usearch)
		$(CC) -pthread -fPIC -O2 -Wall -g -c dtlv.c -o dtlv.o
	  $(CXX) $(LDFLAGS) -pthread -shared -fPIC -o $@ dtlv.o -L. -llmdb $(OMP_LIB) $(USEARCH_LIB)

clean:
		$(RM) *.o *.a *.so *.dylib
		$(MAKE) --directory=$(lib_lmdb) clean
