
set PWD=%cd%

set CPATH=%PWD%\src

cd %CPATH%\usearch

cmake -G "Visual Studio 17 2022" ^
      -D CMAKE_BUILD_TYPE=Debug ^
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

cmake -G "Visual Studio 17 2022" ^
      -D CMAKE_BUILD_TYPE=Debug ^
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

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.11\javacpp-1.5.11.jar" ^
    -Dcompiler=msvc ^
    -DcompilerOptions=/MDd ^
    -DlinkerOptions="/DEBUG /INCREMENTAL" ^
    -DdeleteJniFiles=false ^
    -Dorg.bytedeco.javacpp.logger.debug=true ^
    datalevin/dtlvnative/DTLV.java

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\*.dll windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\