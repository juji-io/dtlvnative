
@echo off
setlocal

set PWD=%cd%

set CPATH=%PWD%\src

call :main
exit /b %errorlevel%

:main

del /q "%CPATH%\dtlv.lib" "%CPATH%\llama.lib" "%CPATH%\ggml.lib" "%CPATH%\ggml-base.lib" "%CPATH%\ggml-cpu.lib" "%CPATH%\dlmdb.lib" "%CPATH%\libusearch_static_c.lib" 2>nul

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      -DBUILD_TEST=off ^
      -DSIMSIMD_TARGET_HASWELL=1 ^
      -DSIMSIMD_TARGET_SKYLAKE=0 ^
      -DSIMSIMD_TARGET_ICE=0 ^
      -DSIMSIMD_TARGET_GENOA=0 ^
      -DSIMSIMD_TARGET_SAPPHIRE=0 ^
      -DSIMSIMD_TARGET_TURIN=0 ^
      -DSIMSIMD_TARGET_SIERRA=0 ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv
if errorlevel 1 exit /b %errorlevel%

call :build_and_stage dlmdb dlmdb.lib
if errorlevel 1 exit /b %errorlevel%
call :build_and_stage usearch_static_c libusearch_static_c.lib
if errorlevel 1 exit /b %errorlevel%
cmake --build build_dtlv --config Release --target test_cpp test_c
if errorlevel 1 exit /b %errorlevel%

dir build_dtlv

call :run_built_exe test_cpp.exe
if errorlevel 1 exit /b %errorlevel%

call :run_built_exe test_c.exe
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

set RESOURCE_DIR=windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64
if not exist "%RESOURCE_DIR%" mkdir "%RESOURCE_DIR%"
del /q "%RESOURCE_DIR%\*" 2>nul

REM Only ship runtime DLLs in the jar. Static .lib archives are link-time inputs.
copy src\java\datalevin\dtlvnative\windows-x86_64\*.dll "%RESOURCE_DIR%\"
if errorlevel 1 exit /b %errorlevel%

call :bundle_openmp_runtime "%RESOURCE_DIR%"
if errorlevel 1 exit /b %errorlevel%

dir "%RESOURCE_DIR%"

goto :eof

:bundle_openmp_runtime
set OMP_DLL=
if defined VCToolsRedistDir (
  for %%F in ("%VCToolsRedistDir%x64\Microsoft.VC*.OpenMP\vcomp*.dll") do (
    if exist "%%~fF" set OMP_DLL=%%~fF
  )
)
if not defined OMP_DLL (
  for /r "%ProgramFiles(x86)%\Microsoft Visual Studio" %%F in (vcomp140.dll) do (
    if not defined OMP_DLL set OMP_DLL=%%~fF
  )
)
if defined OMP_DLL (
  copy /Y "%OMP_DLL%" "%~1\" >nul
  exit /b %errorlevel%
)
echo OpenMP runtime vcomp140.dll not found; runtime must be present on target system.
exit /b 0

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
dir /s build_dtlv\test*.exe
exit /b 1

:run_built_exe_release
"build_dtlv\Release\%~1"
exit /b %errorlevel%

:run_built_exe_root
"build_dtlv\%~1"
exit /b %errorlevel%
