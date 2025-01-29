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

# usearch static
add_library(usearch STATIC IMPORTED)
set_target_properties(usearch PROPERTIES
    IMPORTED_LOCATION "${PROJECT_SOURCE_DIR}"
)

# DTLV static
add_library(dtlv STATIC
    dtlv.c
)

# Link the LMDB static to DTLV
target_link_libraries(dtlv PRIVATE lmdb)

# Link the usearch static to DTLV
target_link_options(dtlv PRIVATE "/WHOLEARCHIVE:usearch.lib")
target_link_libraries(dtlv PRIVATE usearch)

set_target_properties(dtlv PROPERTIES
    OUTPUT_NAME dtlv
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
)

# Install the static library
install(TARGETS dtlv
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
)
