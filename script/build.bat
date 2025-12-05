
set PWD=%cd%

set CPATH=%PWD%\src
set BUILD_TEST_FLAG=-DBUILD_TEST=ON

cd %PWD%

cd %CPATH%

if exist build_dtlv rmdir /S /Q build_dtlv

cmake -G "Visual Studio 17 2022" ^
      -DCLOSE_WARNING=on ^
      %BUILD_TEST_FLAG% ^
      -DUSEARCH_USE_FP16LIB=ON ^
      -DUSEARCH_USE_OPENMP=OFF ^
      -DUSEARCH_USE_SIMSIMD=ON ^
      -DUSEARCH_BUILD_TEST_CPP=ON ^
      -DUSEARCH_BUILD_TEST_C=ON ^
      -DUSEARCH_BUILD_LIB_C=ON ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreadedDLL ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target usearch_static_c lmdb dtlv install

dir build_dtlv

set TEST_CPP=build_dtlv\usearch_static_c_build\cpp\Release\test_cpp.exe
set TEST_C=build_dtlv\usearch_static_c_build\c\Release\test_c.exe
if exist "%TEST_CPP%" "%TEST_CPP%"
if exist "%TEST_C%" "%TEST_C%"
set TEST_DTLV=build_dtlv\Release\dtlv_usearch_checkpoint_test.exe
if exist "%TEST_DTLV%" "%TEST_DTLV%"

REM Copy built static libs into src for JavaCPP linking
if exist "build_dtlv\dtlv.lib" (
  copy /Y "build_dtlv\dtlv.lib" "%CPATH%\dtlv.lib"
) else (
  echo ERROR: dtlv.lib not found. && exit /b 1
)

if exist "build_dtlv\lmdb.lib" (
  copy /Y "build_dtlv\lmdb.lib" "%CPATH%\lmdb.lib"
) else (
  echo ERROR: lmdb.lib not found. && exit /b 1
)

if exist "build_dtlv\usearch_static_c.lib" (
  copy /Y "build_dtlv\usearch_static_c.lib" "%CPATH%\usearch_static_c.lib"
) else if exist "build_dtlv\libusearch_static_c.lib" (
  copy /Y "build_dtlv\libusearch_static_c.lib" "%CPATH%\usearch_static_c.lib"
) else (
  echo ERROR: usearch library not found. && exit /b 1
)

cd %PWD%

cd %CPATH%

dir %CPATH%

cd java

java -jar "%USERPROFILE%\.m2\repository\org\bytedeco\javacpp\1.5.12\javacpp-1.5.12.jar" ^
    -Dorg.bytedeco.javacpp.buildtype=Release ^
    -Dplatform.compiler.linkpath="%CPATH%" ^
    -Dplatform.linkpath="%CPATH%" ^
    datalevin/dtlvnative/DTLV.java

dir datalevin\dtlvnative\windows-x86_64

dumpbin /linkermember:2 datalevin\dtlvnative\windows-x86_64\jniDTLV.dll

cd ..\..

copy src\java\datalevin\dtlvnative\windows-x86_64\* windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

copy src\*.lib windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\

dir windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64

REM Bundle OpenMP runtime (vcomp) so users do not need VC redist installed for OpenMP
echo VCToolsRedistDir=%VCToolsRedistDir%
set OMP_DLL=
if defined VCToolsRedistDir (
  for /d %%D in ("%VCToolsRedistDir%x64\Microsoft.VC*.OpenMP") do (
    if exist "%%D\vcomp140.dll" set OMP_DLL=%%D\vcomp140.dll
  )
)

if not defined OMP_DLL (
  set "VS_DIR=%ProgramFiles(x86)%\Microsoft Visual Studio"
  if exist "!VS_DIR!" (
    for /r "!VS_DIR!" %%F in (vcomp140.dll) do (
      if not defined OMP_DLL set OMP_DLL=%%~fF
    )
  )
)

if not defined OMP_DLL (
  set "VS_DIR=%ProgramFiles%\Microsoft Visual Studio"
  if exist "!VS_DIR!" (
    for /r "!VS_DIR!" %%F in (vcomp140.dll) do (
      if not defined OMP_DLL set OMP_DLL=%%~fF
    )
  )
)

if defined OMP_DLL (
  echo Found OpenMP DLL at: %OMP_DLL%
  copy "%OMP_DLL%" windows-x86_64\resources\datalevin\dtlvnative\windows-x86_64\
) else (
  echo ERROR: OpenMP runtime vcomp140.dll not found.
  exit /b 1
)
