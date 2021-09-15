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
		gcc -c -fPIC -o dtlv.o dtlv.c
		gcc -shared -fPIC -L. -llmdb -o libdtlv.so 

clean:
		rm *.o *.a *.so
		$(MAKE) --directory=$(lib_lmdb) clean
