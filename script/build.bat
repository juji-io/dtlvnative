
set PWD=%cd%

set CPATH=%PWD%\src

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      -DBUILD_TEST=off ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -DCMAKE_BUILD_TYPE=Debug ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target install

dir build_dtlv

build_dtlv\test_cpp

build_dtlv\test_c

cd %PWD%

cd %CPATH%

dir %CPATH%

dumpbin /DIRECTIVES libusearch_static_c.lib

cd java

java -Djavacpp.debug=true -Djavacpp.deleteJniFiles=false -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.11\javacpp-1.5.11.jar" -nodelete ^
    datalevin/dtlvnative/DTLV.java

type C:\projects\dtlvnative\src\java\datalevin\dtlvnative\jniDTLV.cpp

dir datalevin\dtlvnative\windows-x86_64

dumpbin /linkermember:2 datalevin\dtlvnative\windows-x86_64\jniDTLV.dll

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\* windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

copy src\*.lib windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

dir windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64