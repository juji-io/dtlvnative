CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(dtlv VERSION  0.9.30 LANGUAGES  C)

# Compilation switch to turn off compilation warning
OPTION(CLOSE_WARNING "close warning,default off" OFF)
# Compile switch, used to specify whether to compile test code
OPTION(BUILD_TEST "build test program,default off" OFF)

# for #include <unistd.h>
INCLUDE_DIRECTORIES( ${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/win32 ${PROJECT_SOURCE_DIR}/lmdb/libraries/liblmdb )

# option
IF( CLOSE_WARNING)
    #close warning
    ADD_DEFINITIONS(-D_CRT_SECURE_NO_WARNINGS)
ENDIF(CLOSE_WARNING)

# lmdb library (static)
ADD_LIBRARY(lmdb STATIC lmdb/libraries/liblmdb/mdb.c lmdb/libraries/liblmdb/midl.c)

SET_TARGET_PROPERTIES( lmdb
  PROPERTIES
  OUTPUT_NAME lmdb
  ARCHIVE_OUTPUT_DIRECTORY lmdb
  RUNTIME_OUTPUT_DIRECTORY lmdb
)

INSTALL(TARGETS lmdb
    RUNTIME DESTINATION .)

# dtlv library (shared)
ADD_LIBRARY(dtlv SHARED dtlv.c)

# Link the LMDB static library to DTLV
TARGET_LINK_LIBRARIES(dtlv PRIVATE lmdb)

INSTALL(TARGETS dtlv
    ARCHIVE DESTINATION .)
