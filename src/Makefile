CC	= gcc
SOEXT	= .so

lib_lmdb  := lmdb/libraries/liblmdb
lib_dtlv  := libdtlv
libraries := $(lib_lmdb) $(lib_dtlv)$(SOEXT)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ XCFLAGS="-fPIC" liblmdb.a
		cp $(lib_lmdb)/liblmdb.a .

$(lib_dtlv)$(SOEXT): $(lib_lmdb)
		$(CC) -pthread -fPIC -O2 -Wall -g -c dtlv.c -o dtlv.o
	  $(CC) $(LDFLAGS) -pthread -shared -fPIC -o $@ dtlv.o -L. -llmdb

clean:
		$(RM) *.o *.a *.so *$(SOEXT)
		$(MAKE) --directory=$(lib_lmdb) clean
