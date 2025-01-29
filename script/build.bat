
set PWD=%cd%

set CPATH=%PWD%\src

cd %CPATH%\usearch

cmake -G "Visual Studio 17 2022" ^
      -D USEARCH_USE_FP16LIB=1 ^
      -D USEARCH_USE_OPENMP=1 ^
      -D USEARCH_USE_SIMSIMD=1 ^
      -D USEARCH_BUILD_TEST_CPP=1 ^
      -D USEARCH_BUILD_TEST_C=1 ^
      -D USEARCH_BUILD_LIB_C=1 ^
      -B build_us

cmake --build build_us --config Debug

build_us\test_cpp

build_us\test_c

dir build_us

copy build_us\libusearch_static_c.lib %CPATH%\usearch.lib

cd %PWD%

cd %CPATH%

mkdir build_dtlv

cd build_dtlv

cmake .. ^
  -G "Visual Studio 17 2022" ^
  -DCLOSE_WARNING=on ^
  -DBUILD_TEST=off ^
  -DCMAKE_INSTALL_PREFIX=%CPATH% ^
  -B build_dtlv

cmake --build build_dtlv --config Debug --target install

cd %PWD%

cd %CPATH%

dumpbin /directives usearch.lib

dumpbin /directives dtlv.lib

dir %CPATH%

cd %PWD%