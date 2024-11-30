CC	= gcc
SOEXT	= .so

lib_lmdb  := lmdb/libraries/liblmdb
lib_dtlv  := libdtlv
libraries := $(lib_lmdb) $(lib_dtlv)$(SOEXT)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
# $(MAKE) --directory=$@ liblmdb.a liblmdb.so
		$(MAKE) --directory=$@ liblmdb$(SOEXT)
# cp $(lib_lmdb)/liblmdb.a $(lib_dtlv)
		cp $(lib_lmdb)/liblmdb$(SOEXT) .

$(lib_dtlv)$(SOEXT): $(lib_lmdb)
		$(CC) -pthread -fPIC -O2 -Wall -g -c dtlv.c -o dtlv.o
	  $(CC) $(LDFLAGS) -pthread -shared -o $@ dtlv.o liblmdb$(SOEXT)
# ar rcs libdtlv.a dtlv.o

clean:
		$(RM) *.o *.a *.so *$(SOEXT)
		$(MAKE) --directory=$(lib_lmdb) clean
