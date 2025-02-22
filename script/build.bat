
set PWD=%cd%

copy src\usearch\c\usearch.h src\usearch\c\usearch.h.bak

powershell -Command "$regex='(?i)(USEARCH_EXPORT\s+typedef\s+struct\s+usearch_init_options_t\s*\{)'; $replacement='#pragma pack(push,1)' + [Environment]::NewLine + '$1'; (Get-Content -Raw 'src\usearch\c\usearch.h') -replace $regex, $replacement | Set-Content 'src\usearch\c\usearch.h'"

powershell -Command "$regex='(\}\s*usearch_init_options_t;)'; $replacement='$1' + [Environment]::NewLine + '#pragma pack(pop)'; (Get-Content -Raw 'src\usearch\c\usearch.h') -replace $regex, $replacement | Set-Content 'src\usearch\c\usearch.h'"

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