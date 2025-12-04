
set PWD=%cd%

set CPATH=%PWD%\src
set BUILD_TEST_FLAG=-DBUILD_TEST=ON

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      %BUILD_TEST_FLAG% ^
      -DUSEARCH_USE_FP16LIB=ON ^
      -DUSEARCH_USE_OPENMP=ON ^
      -DUSEARCH_USE_SIMSIMD=ON ^
      -DUSEARCH_BUILD_TEST_CPP=ON ^
      -DUSEARCH_BUILD_TEST_C=ON ^
      -DUSEARCH_BUILD_LIB_C=ON ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target install

dir build_dtlv

set TEST_CPP=build_dtlv\usearch_static_c_build\cpp\Release\test_cpp.exe
set TEST_C=build_dtlv\usearch_static_c_build\c\Release\test_c.exe
if exist "%TEST_CPP%" "%TEST_CPP%"
if exist "%TEST_C%" "%TEST_C%"
set TEST_DTLV=build_dtlv\Release\dtlv_usearch_checkpoint_test.exe
if exist "%TEST_DTLV%" "%TEST_DTLV%"

REM Copy built static libs where JavaCPP expects them
copy /Y build_dtlv\dtlv.lib %CPATH%\
copy /Y build_dtlv\lmdb.lib %CPATH%\
copy /Y build_dtlv\usearch_static_c.lib %CPATH%\

cd %PWD%

cd %CPATH%

dir %CPATH%

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.12\javacpp-1.5.12.jar" ^
    -Dorg.bytedeco.javacpp.buildtype=Release ^
    datalevin/dtlvnative/DTLV.java

dir datalevin\dtlvnative\windows-x86_64

dumpbin /linkermember:2 datalevin\dtlvnative\windows-x86_64\jniDTLV.dll

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\* windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

copy src\*.lib windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

dir windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64

REM Bundle OpenMP runtime (vcomp) so users do not need VC redist installed for OpenMP
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
  copy "%OMP_DLL%" windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\
) else (
  echo OpenMP runtime vcomp140.dll not found; runtime must be present on target system.
)
