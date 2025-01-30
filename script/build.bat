
set PWD=%cd%

set CPATH=%PWD%\src

cd %CPATH%\usearch

cmake -G "Visual Studio 17 2022" ^
      -D CMAKE_BUILD_TYPE=Release ^
      -D USEARCH_USE_FP16LIB=1 ^
      -D USEARCH_USE_OPENMP=1 ^
      -D USEARCH_USE_SIMSIMD=1 ^
      -D USEARCH_BUILD_TEST_CPP=1 ^
      -D USEARCH_BUILD_TEST_C=1 ^
      -D USEARCH_BUILD_LIB_C=1 ^
      -B build_us

cmake --build build_us --config Release

build_us\test_cpp

build_us\test_c

dir build_us

copy build_us\libusearch_static_c.lib %CPATH%\usearch.lib

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -D CMAKE_BUILD_TYPE=Release ^
      -DCLOSE_WARNING=on ^
      -DBUILD_TEST=off ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target install

cd %PWD%

cd %CPATH%

dir %CPATH%

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.11\javacpp-1.5.11.jar" ^
    datalevin/dtlvnative/DTLV.java

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\*.dll windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\