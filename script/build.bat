
set PWD=%cd%
set CPATH=%PWD%\src

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

cd %CPATH%

copy *.dll ..\windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\
