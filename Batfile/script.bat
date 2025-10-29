@echo off
setlocal enabledelayedexpansion

REM === Set your source folder here ===
set "src=C:\Users\DhivakarJeganathan\Desktop\Work\Quick\Cursor\TNCP R\SAP Tables\corning_aupf_acpf\ACPF-AUPF\"

REM === Create destination folders if they don't exist ===
if not exist "%src%\GNB" mkdir "%src%\GNB"
if not exist "%src%\VCU" mkdir "%src%\VCU"
if not exist "%src%\VM" mkdir "%src%\VM"

REM === Loop through all files in the folder ===
for %%f in ("%src%\*") do (
    set "filename=%%~nxf"

    REM Skip moving this script itself
    if /I "!filename!"=="script.bat" (
        echo Skipping script file: !filename!
    ) else if /I "!filename!" neq "" (
        echo Processing: !filename!

        REM Check for GNB files
        echo !filename! | findstr /I "_GNB_" >nul
        if !errorlevel! == 0 (
            move "%%f" "%src%\GNB\" >nul
            echo Moved to GNB
        ) else (
            REM Check for VCU files
            echo !filename! | findstr /I "_VCU_" >nul
            if !errorlevel! == 0 (
                move "%%f" "%src%\VCU\" >nul
                echo Moved to VCU
            ) else (
                REM Move remaining files to VM
                move "%%f" "%src%\VM\" >nul
                echo Moved to VM
            )
        )
    )
)

echo.
echo === File sorting completed ===
pause
