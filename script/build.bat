
set PWD=%cd%

REM Back up the original file just in case
copy src\usearch\c\usearch.h src\usearch\c\usearch.h.bak

REM Insert "#pragma pack(push,8)" immediately before the struct definition
powershell -Command "(Get-Content src\usearch\c\usearch.h) -replace '(USEARCH_EXPORT\s+typedef\s+struct\s+usearch_init_options_t\s+\{)', '#pragma pack(push,8)\r\n$1' | Set-Content src\usearch\c\usearch.h"

REM Insert "#pragma pack(pop)" immediately after the struct closing brace
powershell -Command "(Get-Content src\usearch\c\usearch.h) -replace '(\}\s+usearch_init_options_t;)', '$1\r\n#pragma pack(pop)' | Set-Content src\usearch\c\usearch.h"

type src\usearch\c\usearch.h

set CPATH=%PWD%\src

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      -DBUILD_TEST=off ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target install

dir build_dtlv

build_dtlv\test_cpp

build_dtlv\test_c

cd %PWD%

cd %CPATH%

dir %CPATH%

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.11\javacpp-1.5.11.jar" ^
    datalevin/dtlvnative/DTLV.java

dir datalevin\dtlvnative\windows-x86_64

dumpbin /linkermember:2 datalevin\dtlvnative\windows-x86_64\jniDTLV.dll

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\* windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

copy src\*.lib windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

dir windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64