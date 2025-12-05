
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
      -DUSEARCH_USE_OPENMP=ON ^
      -DUSEARCH_USE_SIMSIMD=ON ^
      -DUSEARCH_BUILD_TEST_CPP=ON ^
      -DUSEARCH_BUILD_TEST_C=ON ^
      -DUSEARCH_BUILD_LIB_C=ON ^
      -DUSEARCH_BUILD_JNI=ON ^
      -DCMAKE_BUILD_TYPE=RelWithDebInfo ^
      -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target usearch_static_c lmdb dtlv usearch_jni test_cpp test_c dtlv_usearch_checkpoint_test install

REM Ensure the Usearch tests are built in the subproject tree (VS sometimes skips them on the parent build)
cmake --build build_dtlv\usearch_static_c_build --config Release --target test_cpp test_c

dir build_dtlv

set TEST_CPP=
for /r "build_dtlv" %%F in (test_cpp.exe) do (
  if not defined TEST_CPP set TEST_CPP=%%F
)
if not defined TEST_CPP (
  echo ERROR: usearch C++ test binary not found. && exit /b 1
)
"%TEST_CPP%"
if errorlevel 1 exit /b 1

set TEST_C=
for /r "build_dtlv" %%F in (test_c.exe) do (
  if not defined TEST_C set TEST_C=%%F
)
if not defined TEST_C (
  echo ERROR: usearch C test binary not found. && exit /b 1
)
"%TEST_C%"
if errorlevel 1 exit /b 1

set TEST_DTLV=
for /r "build_dtlv" %%F in (dtlv_usearch_checkpoint_test.exe) do (
  if not defined TEST_DTLV set TEST_DTLV=%%F
)
if not defined TEST_DTLV (
  echo ERROR: dtlv checkpoint test binary not found. && exit /b 1
)
"%TEST_DTLV%"
if errorlevel 1 exit /b 1

REM Build and run USearch Java binding smoke test to verify JNI wiring
for /r "build_dtlv" %%F in (*usearch_jni*.dll) do (
  if not defined USEARCH_JNI_DLL set USEARCH_JNI_DLL=%%F
)
if not defined USEARCH_JNI_DLL (
  echo ERROR: usearch JNI library not found. && exit /b 1
)

set USEARCH_JAVA_TEST_DIR=build_dtlv\java_test
if exist "%USEARCH_JAVA_TEST_DIR%" rmdir /S /Q "%USEARCH_JAVA_TEST_DIR%"
mkdir "%USEARCH_JAVA_TEST_DIR%"

copy /Y "%USEARCH_JNI_DLL%" "%USEARCH_JAVA_TEST_DIR%\usearch.dll"
copy /Y "%USEARCH_JNI_DLL%" "%USEARCH_JAVA_TEST_DIR%\libusearch_jni.dll"

javac -d "%USEARCH_JAVA_TEST_DIR%" src\usearch\java\cloud\unum\usearch\*.java tests\usearch_java\IndexSmoke.java
if errorlevel 1 (
  echo ERROR: Failed to compile USearch Java bindings. && exit /b 1
)

set "JAVA_TEST_CP=%USEARCH_JAVA_TEST_DIR%"
set "JAVA_TEST_LIB_PATH=%USEARCH_JAVA_TEST_DIR%"
set PATH=%JAVA_TEST_LIB_PATH%;%PATH%

java -Djava.library.path=%JAVA_TEST_LIB_PATH% -cp "%JAVA_TEST_CP%" IndexSmoke
if errorlevel 1 (
  echo ERROR: Usearch Java smoke test failed. && exit /b 1
)

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
    -Dplatform.compiler.options=/MT ^
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
