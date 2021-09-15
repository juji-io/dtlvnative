lib_lmdb  := lmdb/libraries/liblmdb
lib_dtlv  := .
libraries := $(lib_lmdb) $(lib_dtlv)

.PHONY: all $(libraries)
all: $(libraries)

$(lib_lmdb):
		$(MAKE) --directory=$@ liblmdb.a liblmdb.so
		cp $(lib_lmdb)/liblmdb.so $(lib_dtlv)
		cp $(lib_lmdb)/liblmdb.a $(lib_dtlv)

$(lib_dtlv): $(lib_lmdb)
		gcc -Wall -g -c dtlv.c -o dtlv.o
		ar rcs libdtlv.a dtlv.o
		gcc -shared -o libdtlv.so -fPIC dtlv.c

clean:
		rm *.o *.a
		$(MAKE) --directory=$(lib_lmdb) clean
