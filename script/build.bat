
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

copy *.lib ..\windows-amd64\resources\windows-amd64\
copy dtlv.h ..\windows-amd64\resources\windows-amd64\
copy lmdb\libraries\liblmdb\lmdb.h ..\windows-amd64\resources\windows-amd64\lmdb\libraries\liblmdb\