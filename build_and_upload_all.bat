@echo off
setlocal enabledelayedexpansion
set ROOT_DIR=%~dp0
goto :main

:process_module
set MODULE=%~1
set MODULE_PATH=%ROOT_DIR%%MODULE%

echo.
echo ========================================
echo Checking module: %MODULE%
echo Module path: %MODULE_PATH%
echo ========================================

if not exist "%MODULE_PATH%" (
    echo [SKIP] %MODULE% - directory not found: %MODULE_PATH%
    goto :eof
)

if not exist "%MODULE_PATH%\setup.py" (
    echo [SKIP] %MODULE% - setup.py not found
    goto :eof
)

echo.
echo --- Processing %MODULE% ---

pushd "%MODULE_PATH%" >nul

echo Cleaning old builds...
if exist "dist" rmdir /s /q "dist" >nul 2>&1
if exist "build" rmdir /s /q "build" >nul 2>&1
for /d %%D in (*.egg-info) do rmdir /s /q "%%D" >nul 2>&1

echo Building package...
python setup.py sdist bdist_wheel 2>&1
if errorlevel 1 (
    echo [FAIL] build failed for %MODULE%
    set /a FAIL_COUNT+=1
    popd >nul
    goto :eof
)

echo Uploading to devpi...
if not exist "dist" (
    echo [FAIL] dist directory not found for %MODULE%
    set /a FAIL_COUNT+=1
    popd >nul
    goto :eof
)

REM Save upload output to temp file
set TEMP_FILE=%TEMP%\devpi_upload_%MODULE%.txt
devpi upload --from-dir dist --only-latest > "%TEMP_FILE%" 2>&1

REM Check for 409 conflict error
findstr /C:"409" /C:"already exists" "%TEMP_FILE%" >nul
if not errorlevel 1 (
    echo [SKIP] %MODULE% - already exists on server (non-volatile index)
    del "%TEMP_FILE%" 2>nul
    set /a SUCCESS_COUNT+=1
    popd >nul
    goto :eof
)

REM Check for other errors
findstr /C:"FAIL" /C:"ERROR" "%TEMP_FILE%" >nul
if not errorlevel 1 (
    echo [FAIL] Upload failed for %MODULE%
    type "%TEMP_FILE%"
    del "%TEMP_FILE%" 2>nul
    set /a FAIL_COUNT+=1
    popd >nul
    goto :eof
)

del "%TEMP_FILE%" 2>nul

echo [OK] %MODULE%
set /a SUCCESS_COUNT+=1
popd >nul
goto :eof

:main

REM Package & upload all sub-packages to devpi

set DEVPI_URL=http://localhost:18080/root/dev
set DEVPI_PASSWORD=
if not "%~1"=="" set DEVPI_URL=%~1
if not "%~2"=="" set DEVPI_PASSWORD=%~2

echo ========================================
echo Packaging and uploading modules to devpi
echo Root directory: %ROOT_DIR%
echo Devpi URL: %DEVPI_URL%
echo ========================================
echo.

echo Checking devpi connection...
devpi use >nul 2>&1
if errorlevel 1 (
    echo ERROR: devpi is not reachable. Start devpi-server first.
    pause
    exit /b 1
)

echo Switching to target index...
devpi use %DEVPI_URL%
if errorlevel 1 (
    echo ERROR: failed to select devpi index %DEVPI_URL%.
    pause
    exit /b 1
)

echo Logging in to devpi...
devpi login root --password=%DEVPI_PASSWORD%
if errorlevel 1 (
    echo ERROR: devpi login failed. Please check credentials.
    echo Try running: build_and_upload_all.bat "URL" "PASSWORD"
    pause
    exit /b 1
)

echo Successfully logged in and switched to %DEVPI_URL%
echo.

set SUCCESS_COUNT=0
set FAIL_COUNT=0

call :process_module panda_common
call :process_module panda_data
call :process_module panda_data_hub
call :process_module panda_factor
call :process_module panda_factor_server
call :process_module panda_llm
call :process_module panda_web

echo.
echo ========================================
echo Completed
echo ========================================
echo Success: %SUCCESS_COUNT%
echo Failed: %FAIL_COUNT%
echo.
echo Devpi index: %DEVPI_URL%
echo Install command: uv pip install -i %DEVPI_URL% ^<package_name^>
echo.
pause
exit /b 0
