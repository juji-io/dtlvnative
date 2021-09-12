
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
    -DBUILD_TEST=off ^
    -DBUILD_SHARED_LIBS=off
nmake install

cd %CPATH%

copy *.lib ..\resources\
copy dtlv.h ..\resources\
copy lmdb\libraries\liblmdb\lmdb.h ..\resources\lmdb\libraries\liblmdb\