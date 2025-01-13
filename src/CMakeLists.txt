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

# LMDB library (static)
add_library(lmdb STATIC
    lmdb/libraries/liblmdb/mdb.c
    lmdb/libraries/liblmdb/midl.c
)

set_target_properties(lmdb PROPERTIES
    OUTPUT_NAME lmdb
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)

# Install only the .lib file for the static library
install(TARGETS lmdb
    ARCHIVE DESTINATION lib
)

# DTLV library (shared)
add_library(dtlv SHARED
    dtlv.c
)

# Link the LMDB static library to DTLV
target_link_libraries(dtlv PRIVATE lmdb)

set_target_properties(dtlv PROPERTIES
    OUTPUT_NAME dtlv
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)

# Install the shared library
install(TARGETS dtlv
    RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}
)
