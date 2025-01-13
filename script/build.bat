
set PWD=%cd%
set CPATH=%PWD%\src
set DPATH=%PWD%\windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64

mkdir %DPATH% /p

mkdir %CPATH%\java\datalevin\dtlvnative\windows-x86_64 /p

cd %CPATH%

mkdir build

cd build

cmake .. ^
    -G "NMake Makefiles" ^
    -DCMAKE_BUILD_TYPE:STRING=RELEASE ^
    -DCMAKE_INSTALL_PREFIX=%DPATH% ^
    -DCLOSE_WARNING=on ^
    -DBUILD_TEST=off

nmake install

dir %DPATH%

cd %PWD%