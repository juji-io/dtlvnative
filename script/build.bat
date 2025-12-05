
set PWD=%cd%

set CPATH=%PWD%\src
set BUILD_TEST_FLAG=-DBUILD_TEST=ON
set GRADLE_VERSION=9.2.1
set GRADLE_BASE=%CPATH%\usearch\tools\gradle-%GRADLE_VERSION%
set GRADLE_ZIP=%GRADLE_BASE%.zip

REM Locate or install Gradle early so we fail fast if unavailable
set GRADLE_BIN=
if exist "%GRADLE_HOME%\bin\gradle.bat" set GRADLE_BIN=%GRADLE_HOME%\bin\gradle.bat
if not defined GRADLE_BIN (
  for %%G in (gradle.bat gradle) do (
    for /f "delims=" %%P in ('where %%G 2^>NUL') do (
      if not defined GRADLE_BIN set GRADLE_BIN=%%P
    )
  )
)
if not defined GRADLE_BIN (
  if not exist "%GRADLE_BASE%\bin\gradle.bat" (
    echo Gradle not found; downloading %GRADLE_VERSION% to %GRADLE_BASE% ...
    if not exist "%CPATH%\usearch\tools" mkdir "%CPATH%\usearch\tools"
    powershell -Command "$ProgressPreference='SilentlyContinue'; Invoke-WebRequest -Uri \"https://services.gradle.org/distributions/gradle-%GRADLE_VERSION%-bin.zip\" -OutFile \"%GRADLE_ZIP%\" -MaximumRedirection 5 -UseBasicParsing"
    if errorlevel 1 (
      echo ERROR: Failed to download Gradle %GRADLE_VERSION%.
      exit /b 1
    )
    if not exist "%GRADLE_ZIP%" (
      echo ERROR: Gradle archive not found at %GRADLE_ZIP%.
      exit /b 1
    )
    powershell -Command "Expand-Archive -Force '%GRADLE_ZIP%' '%CPATH%\usearch\tools'"
    if errorlevel 1 (
      echo ERROR: Failed to extract Gradle %GRADLE_VERSION%.
      exit /b 1
    )
  )
  if exist "%GRADLE_BASE%\bin\gradle.bat" set GRADLE_BIN=%GRADLE_BASE%\bin\gradle.bat
)
if not defined GRADLE_BIN (
  echo ERROR: Gradle not found in PATH/GRADLE_HOME and download failed.
  exit /b 1
)

REM Build and run USearch C/C++ tests standalone to validate JNI-independent bits
set USEARCH_TEST_BUILD=%CPATH%\usearch\build_tests
if exist "%USEARCH_TEST_BUILD%" rmdir /S /Q "%USEARCH_TEST_BUILD%"

cmake -G "Visual Studio 17 2022" ^
      -A x64 ^
      -D CMAKE_BUILD_TYPE=Release ^
      -D USEARCH_USE_FP16LIB=ON ^
      -D USEARCH_USE_OPENMP=ON ^
      -D USEARCH_USE_SIMSIMD=ON ^
      -D USEARCH_BUILD_TEST_CPP=ON ^
      -D USEARCH_BUILD_TEST_C=ON ^
      -D USEARCH_BUILD_LIB_C=ON ^
      -B "%USEARCH_TEST_BUILD%" ^
      -S "%CPATH%\usearch"
if errorlevel 1 (
  echo ERROR: Failed to configure USearch standalone build.
  exit /b 1
)

cmake --build "%USEARCH_TEST_BUILD%" --config Release --target test_cpp test_c
if errorlevel 1 (
  echo ERROR: Failed to build USearch C/C++ tests.
  exit /b 1
)

pushd "%USEARCH_TEST_BUILD%"
ctest -C Release --output-on-failure
if errorlevel 1 (
  echo Usearch standalone C/C++ tests failed.
  popd
  exit /b 1
)
popd

REM Run Usearch Java tests via Gradle (fetches its own dependencies)
pushd "%CPATH%\usearch"
"%GRADLE_BIN%" --version
"%GRADLE_BIN%" --no-daemon test
if errorlevel 1 (
  echo ERROR: Usearch Java tests failed (Gradle).
  popd
  exit /b 1
)
popd

cd %PWD%

cd %CPATH%

if exist build_dtlv rmdir /S /Q build_dtlv

cmake -G "Visual Studio 17 2022" ^
      -A x64 ^
      -DCLOSE_WARNING=on ^
      %BUILD_TEST_FLAG% ^
      -DUSEARCH_USE_FP16LIB=ON ^
      -DUSEARCH_USE_OPENMP=ON ^
      -DUSEARCH_USE_SIMSIMD=ON ^
      -DUSEARCH_BUILD_TEST_CPP=ON ^
      -DUSEARCH_BUILD_TEST_C=ON ^
      -DUSEARCH_BUILD_LIB_C=ON ^
      -DCMAKE_BUILD_TYPE=RelWithDebInfo ^
      -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded ^
      -DCMAKE_INSTALL_PREFIX=%CPATH% ^
      -B build_dtlv

cmake --build build_dtlv --config Release --target usearch_static_c lmdb dtlv dtlv_usearch_checkpoint_test install

dir build_dtlv

set TEST_DTLV=
if exist "build_dtlv\Release\dtlv_usearch_checkpoint_test.exe" (
  set TEST_DTLV=build_dtlv\Release\dtlv_usearch_checkpoint_test.exe
) else (
  for /r "build_dtlv" %%F in (dtlv_usearch_checkpoint_test.exe) do (
    if not defined TEST_DTLV set TEST_DTLV=%%F
  )
)
if not defined TEST_DTLV (
  echo ERROR: dtlv checkpoint test binary not found. && exit /b 1
)
"%TEST_DTLV%"
if errorlevel 1 exit /b 1

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
