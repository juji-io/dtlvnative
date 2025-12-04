lib_lmdb    := lmdb/libraries/liblmdb
lib_usearch := usearch
lib_dtlv    := libdtlv

DTLV_SRCS := dtlv.c dtlv_crc32c.c dtlv_usearch.c dtlv_usearch_wal.c
DTLV_OBJS := $(DTLV_SRCS:.c=.o)

libraries := $(lib_lmdb) $(lib_usearch) $(lib_dtlv)$(SOEXT)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ XCFLAGS="-fPIC" liblmdb.a
		cp $(lib_lmdb)/liblmdb.a .

$(lib_usearch):
		cp $(lib_usearch)/build_release/libusearch_static_c.a ./libusearch.a

$(lib_dtlv)$(SOEXT): $(lib_lmdb) $(lib_usearch) $(DTLV_OBJS)
		$(CXX) $(LDFLAGS) -pthread -shared -fPIC -o $@ $(DTLV_OBJS) -L. -llmdb $(OMP_LIB) $(USEARCH_LIB)

%.o: %.c
		$(CC) -pthread -fPIC -O2 -Wall -g -c $< -o $@

clean:
		$(RM) *.o *.a *.so *.dylib
		$(MAKE) --directory=$(lib_lmdb) clean
