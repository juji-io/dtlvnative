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
    ${PROJECT_SOURCE_DIR}/llama.cpp/include
    ${PROJECT_SOURCE_DIR}/llama.cpp/ggml/include
)

# Option to close warnings
if(CLOSE_WARNING)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif()

# DLMDB static
add_library(dlmdb STATIC
    lmdb/libraries/liblmdb/mdb.c
    lmdb/libraries/liblmdb/midl.c
)

set_target_properties(dlmdb PROPERTIES
    OUTPUT_NAME dlmdb
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
    ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${CMAKE_INSTALL_PREFIX}
)

# Install only the .lib file for the static library
install(TARGETS dlmdb
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
)

# DTLV static
add_library(dtlv STATIC
    dtlv.c
)

# Link the DLMDB static to DTLV
target_link_libraries(dtlv PRIVATE dlmdb)

set_target_properties(dtlv PROPERTIES
    OUTPUT_NAME dtlv
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
    ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${CMAKE_INSTALL_PREFIX}
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
set(USEARCH_BUILD_TEST_C       ON CACHE BOOL "" FORCE)
set(USEARCH_BUILD_TEST_CPP     ON CACHE BOOL "" FORCE)

add_subdirectory(${PROJECT_SOURCE_DIR}/usearch usearch_static_c_build)

set(LLAMA_BUILD_COMMON         OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TESTS          OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_TOOLS          OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_EXAMPLES       OFF CACHE BOOL "" FORCE)
set(LLAMA_BUILD_SERVER         OFF CACHE BOOL "" FORCE)
set(LLAMA_OPENSSL              OFF CACHE BOOL "" FORCE)
set(GGML_CPU                   ON  CACHE BOOL "" FORCE)
set(GGML_OPENMP                ON  CACHE BOOL "" FORCE)
set(GGML_NATIVE                OFF CACHE BOOL "" FORCE)
set(GGML_LLAMAFILE             OFF CACHE BOOL "" FORCE)
set(GGML_CPU_REPACK            OFF CACHE BOOL "" FORCE)
set(GGML_BACKEND_DL            OFF CACHE BOOL "" FORCE)
set(GGML_ACCELERATE            OFF CACHE BOOL "" FORCE)
set(GGML_BLAS                  OFF CACHE BOOL "" FORCE)
set(GGML_CUDA                  OFF CACHE BOOL "" FORCE)
set(GGML_HIP                   OFF CACHE BOOL "" FORCE)
set(GGML_METAL                 OFF CACHE BOOL "" FORCE)
set(GGML_MUSA                  OFF CACHE BOOL "" FORCE)
set(GGML_OPENCL                OFF CACHE BOOL "" FORCE)
set(GGML_RPC                   OFF CACHE BOOL "" FORCE)
set(GGML_SYCL                  OFF CACHE BOOL "" FORCE)
set(GGML_VULKAN                OFF CACHE BOOL "" FORCE)
set(GGML_WEBGPU                OFF CACHE BOOL "" FORCE)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(${PROJECT_SOURCE_DIR}/llama.cpp llama_static_build)

if(WIN32)
    foreach(target usearch_c usearch_static_c test_c test_cpp)
        if(TARGET ${target})
            # USearch compares mmap return values against MAP_FAILED even on Windows.
            # Provide the POSIX sentinel without modifying the submodule sources.
            target_compile_definitions(${target} PRIVATE "MAP_FAILED=((void*)-1)")
        endif()
    endforeach()
endif()

set_target_properties(usearch_static_c PROPERTIES
    OUTPUT_NAME usearch_static_c
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
    ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${CMAKE_INSTALL_PREFIX}
)

target_link_options(dtlv PRIVATE "/WHOLEARCHIVE:usearch_static_c")
target_link_libraries(dtlv PRIVATE usearch_static_c llama ggml ggml-base ggml-cpu)

install(TARGETS usearch_static_c
    ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
)

foreach(target llama ggml ggml-base ggml-cpu)
    if(TARGET ${target})
        set_target_properties(${target} PROPERTIES
            ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_INSTALL_PREFIX}
            ARCHIVE_OUTPUT_DIRECTORY_RELEASE ${CMAKE_INSTALL_PREFIX}
        )
        install(TARGETS ${target}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_PREFIX}
            PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_PREFIX}
        )
    endif()
endforeach()
