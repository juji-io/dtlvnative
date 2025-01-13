
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

dir %CPATH%
