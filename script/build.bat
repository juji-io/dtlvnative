
set PWD=%cd%

set CPATH=%PWD%\src

cd %PWD%

cd %CPATH%

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      -DBUILD_TEST=off ^
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

build_dtlv\test_cpp

build_dtlv\test_c

cd %PWD%

cd %CPATH%

dir %CPATH%

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.12\javacpp-1.5.12.jar" ^
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