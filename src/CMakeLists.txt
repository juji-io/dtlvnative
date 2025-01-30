cmake_minimum_required(VERSION 3.0)
project(dtlv VERSION 0.11.3 LANGUAGES C)

# Options for compilation
option(CLOSE_WARNING "Close warnings, default off" OFF)
option(BUILD_TEST "Build test program, default off" OFF)

# Include directories
include_directories(
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/win32
    ${PROJECT_SOURCE_DIR}/lmdb/libraries/liblmdb
)

# Option to close warnings
if(CLOSE_WARNING)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif()

# LMDB static
add_library(lmdb STATIC
    lmdb/libraries/liblmdb/mdb.c
    lmdb/libraries/liblmdb/midl.c
)

set_target_properties(lmdb PROPERTIES
    OUTPUT_NAME lmdb
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
)

# Install only the .lib file for the static library
install(TARGETS lmdb
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
)

# DTLV static
add_library(dtlv STATIC
    dtlv.c
)

# Link the LMDB static to DTLV
target_link_libraries(dtlv PRIVATE lmdb)

set_target_properties(dtlv PROPERTIES
    OUTPUT_NAME dtlv
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
)

# Install the static library
install(TARGETS dtlv
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
)

set(USEARCH_USE_FP16LIB        ON CACHE BOOL "" FORCE)
set(USEARCH_USE_OPENMP         ON CACHE BOOL "" FORCE)
set(USEARCH_USE_SIMSIMD        ON CACHE BOOL "" FORCE)
set(USEARCH_BUILD_STATIC_C     ON CACHE BOOL "" FORCE)
set(USEARCH_BUILD_SHARED_C     OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS          OFF CACHE BOOL "Build statically" FORCE)
set(USEARCH_BUILD_LIB_C        ON CACHE BOOL "" FORCE)

add_subdirectory(${PROJECT_SOURCE_DIR}/usearch usearch_static_c_build)

set_target_properties(usearch_static_c PROPERTIES
    OUTPUT_NAME usearch_static_c
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
)

target_link_options(dtlv PRIVATE "/WHOLEARCHIVE:usearch_static_c")
target_link_libraries(dtlv PRIVATE usearch_static_c)

install(TARGETS usearch_static_c
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
)
