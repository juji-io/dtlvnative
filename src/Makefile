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

DTLV_OBJS := dtlv.o dtlv_usearch_wal.o dtlv_usearch.o dtlv_crc32c.o

dtlv.o: dtlv.c
		$(CC) -pthread -fPIC -O2 -Wall -g -c $< -o $@

dtlv_usearch_wal.o: dtlv_usearch_wal.c dtlv_usearch_wal.h
		$(CC) -pthread -fPIC -O2 -Wall -g -c $< -o $@

dtlv_usearch.o: dtlv_usearch.c dtlv_usearch.h dtlv_usearch_wal.h dtlv_crc32c.h dtlv_bytes.h
		$(CC) -pthread -fPIC -O2 -Wall -g -c $< -o $@

dtlv_crc32c.o: dtlv_crc32c.c dtlv_crc32c.h
		$(CC) -pthread -fPIC -O2 -Wall -g -c $< -o $@

$(lib_dtlv)$(SOEXT): $(lib_lmdb) $(lib_usearch) $(DTLV_OBJS)
		$(CXX) $(LDFLAGS) -pthread -shared -fPIC -o $@ $(DTLV_OBJS) -L. -llmdb $(OMP_LIB) $(USEARCH_LIB)

clean:
		$(RM) *.o *.a *.so *.dylib
		$(MAKE) --directory=$(lib_lmdb) clean
