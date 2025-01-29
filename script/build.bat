
set PWD=%cd%

set CPATH=%PWD%\src

cd %CPATH%\usearch

cmake ^
    -D CMAKE_BUILD_TYPE=Release ^
    -D USEARCH_USE_FP16LIB=1 ^
    -D USEARCH_USE_OPENMP=1 ^
    -D USEARCH_USE_SIMSIMD=1 ^
    -D USEARCH_BUILD_TEST_CPP=1 ^
    -D USEARCH_BUILD_TEST_C=1 ^
    -D USEARCH_BUILD_LIB_C=1 ^
    -B build_release

cmake --build build_release --config Release

build_release\test_cpp

build_release\test_c

dir build_release

copy build_release\libusearch_static_c.lib %CPATH%\usearch.lib

cd %PWD%

cd %CPATH%

mkdir build

cd build

cmake .. ^
    -G "NMake Makefiles" ^
    -DCMAKE_BUILD_TYPE:STRING=RELEASE ^
    -DCMAKE_INSTALL_PREFIX=%CPATH% ^
    -DCLOSE_WARNING=on ^
    -DBUILD_TEST=off

nmake install

dir %CPATH%

cd %PWD%