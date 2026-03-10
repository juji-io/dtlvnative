
set PWD=%cd%

set CPATH=%PWD%\src

del /q "%CPATH%\dtlv.lib" "%CPATH%\llama.lib" "%CPATH%\ggml.lib" "%CPATH%\ggml-base.lib" "%CPATH%\ggml-cpu.lib" "%CPATH%\dlmdb.lib" "%CPATH%\libusearch_static_c.lib" 2>nul

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      -DBUILD_TEST=off ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv
if errorlevel 1 exit /b %errorlevel%

call :build_and_stage dlmdb dlmdb.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage usearch_static_c libusearch_static_c.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage ggml-base ggml-base.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage ggml-cpu ggml-cpu.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage ggml ggml.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage llama llama.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage dtlv dtlv.lib
if errorlevel 1 exit /b %errorlevel%

cmake --build build_dtlv --config Release --target test_cpp test_c
if errorlevel 1 exit /b %errorlevel%

dir build_dtlv

call :run_built_exe test_cpp.exe
if errorlevel 1 exit /b %errorlevel%

call :run_built_exe test_c.exe
if errorlevel 1 exit /b %errorlevel%

cd %PWD%

cd %CPATH%

dir %CPATH%

for %%L in (dtlv.lib llama.lib ggml.lib ggml-base.lib ggml-cpu.lib dlmdb.lib libusearch_static_c.lib) do (
  if not exist "%CPATH%\%%L" (
    echo Missing %%L after build_dtlv completed.
    dir %CPATH%\*.lib
    exit /b 1
  )
)

dir %CPATH%\*.lib

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.13\javacpp-1.5.13.jar" ^
    datalevin/dtlvnative/DTLV.java
if errorlevel 1 exit /b %errorlevel%

dir datalevin\dtlvnative\windows-x86_64

dumpbin /dependents datalevin\dtlvnative\windows-x86_64\jniDTLV.dll
if errorlevel 1 exit /b %errorlevel%

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\* windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\
if errorlevel 1 exit /b %errorlevel%

copy src\*.lib windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\
if errorlevel 1 exit /b %errorlevel%

dir windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64

goto :eof

:build_and_stage
echo Building %~1 and staging %~2...
cmake --build build_dtlv --config Release --target %~1
if errorlevel 1 exit /b %errorlevel%
if exist "%CPATH%\%~2" goto :eof
for /r build_dtlv %%F in (%~2) do copy /Y "%%F" "%CPATH%\%~2" >nul
if exist "%CPATH%\%~2" goto :eof
echo Missing %~2 after building %~1.
dir %CPATH%\*.lib
dir /s build_dtlv\*%~1*.lib
exit /b 1

:run_built_exe
if exist "build_dtlv\Release\%~1" goto run_built_exe_release
if exist "build_dtlv\%~1" goto run_built_exe_root
echo Missing %~1 after build.
dir /s build_dtlv\%~1
exit /b 1

:run_built_exe_release
"build_dtlv\Release\%~1"
exit /b %errorlevel%

:run_built_exe_root
"build_dtlv\%~1"
exit /b %errorlevel%
