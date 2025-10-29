@echo off
setlocal enabledelayedexpansion

REM ===============================================
REM TNCP R Data Processing Pipeline
REM ===============================================

echo Starting TNCP R Data Processing Pipeline...
echo ===============================================

REM Set base directory
set "BASE_DIR=C:\Users\DhivakarJeganathan\Desktop\Work\Quick\Cursor\TNCP R"

REM ===============================================
REM Step 1: Copy files from S3 to Ingestion/csvfiles
REM ===============================================
echo.
echo Step 1: Copying files from S3 to Ingestion/csvfiles...
if not exist "%BASE_DIR%\S3" (
    echo ERROR: Datafiles directory not found!
    pause
    exit /b 1
)

timeout /t 5 /nobreak

if not exist "%BASE_DIR%\Ingestion\csvfiles" (
    echo Creating Ingestion\csvfiles directory...
    mkdir "%BASE_DIR%\Ingestion\csvfiles"
)

copy "%BASE_DIR%\S3\*.csv" "%BASE_DIR%\Ingestion\csvfiles\" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Failed to copy files to Ingestion\csvfiles
    pause
    exit /b 1
)
echo Successfully copied files to Ingestion\csvfiles

REM ===============================================
REM Step 2: Copy files from S3 to HierarchyCode/csv_files
REM ===============================================
echo.
echo Step 2: Copying files from S3 to HierarchyCode/csv_files...
if not exist "%BASE_DIR%\HierarchyCode\csv_files" (
    echo Creating HierarchyCode\csv_files directory...
    mkdir "%BASE_DIR%\HierarchyCode\csv_files"
)

copy "%BASE_DIR%\S3\*.csv" "%BASE_DIR%\HierarchyCode\csv_files\" >nul 2>&1
if errorlevel 1 (
    echo ERROR: Failed to copy files to HierarchyCode\csv_files
    pause
    exit /b 1
)
echo Successfully copied files to HierarchyCode\csv_files

timeout /t 10 /nobreak

REM ===============================================
REM Step 3: Run ETL Processor
REM ===============================================
echo.
echo Step 3: Running ETL Processor...
cd /d "%BASE_DIR%\Ingestion\etl_env\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to Ingestion\etl_env\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\Ingestion"
python etl_processor.py
if errorlevel 1 (
    echo ERROR: ETL processor failed
    pause
    exit /b 1
)
echo ETL Processor completed successfully

timeout /t 15 /nobreak

REM ===============================================
REM Step 5: Copy _output.csv files to Analytics/output
REM ===============================================
echo.
echo Step 5: Copying _output.csv files to Analytics/output...
if not exist "%BASE_DIR%\Analytics\output" (
    echo Creating Analytics\output directory...
    mkdir "%BASE_DIR%\Analytics\output"
)

copy "%BASE_DIR%\Ingestion\csvfiles\*_output.csv" "%BASE_DIR%\Analytics\output\" >nul 2>&1
if errorlevel 1 (
    echo WARNING: No _output.csv files found or copy failed
) else (
    echo Successfully copied _output.csv files to Analytics\output
)

timeout /t 3 /nobreak

REM ===============================================
REM Step 5a: Run Analytics ETL
REM ===============================================
echo.
echo Step 5a: Running Analytics ETL...
cd /d "%BASE_DIR%\Analytics\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to Analytics\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\Analytics"
python main_etl.py
if errorlevel 1 (
    echo ERROR: Analytics ETL failed
    pause
    exit /b 1
)
echo Analytics ETL completed successfully

REM ===============================================
REM Step 5b: Execute SQL (Metrics and Tables)
REM ===============================================
echo.
echo Step 5b: Executing SQL (Metrics and Tables)...
cd /d "%BASE_DIR%\MetricsandTables\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to MetricsandTables\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\MetricsandTables"
python execute_sql.py
if errorlevel 1 (
    echo ERROR: SQL execution failed
    pause
    exit /b 1
)
echo SQL execution completed successfully

timeout /t 5 /nobreak

REM ===============================================
REM Step 6: Run Performance Rules
REM ===============================================
echo.
echo Step 6: Running Performance Rules...
cd /d "%BASE_DIR%\Analytics\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to Analytics\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\Analytics"
python performance_rules.py
if errorlevel 1 (
    echo ERROR: Performance rules failed
    pause
    exit /b 1
)
echo Streaming Analytics rules completed successfully

timeout /t 1 /nobreak

REM ===============================================
REM Step 6a: Execute SQL again (Metrics and Tables)
REM ===============================================
echo.
echo Step 6a: Executing SQL again (Metrics and Tables)...
cd /d "%BASE_DIR%\MetricsandTables\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to MetricsandTables\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\MetricsandTables"
python execute_sql.py
if errorlevel 1 (
    echo ERROR: SQL execution failed
    pause
    exit /b 1
)
echo Metrics and Tables execution completed successfully

timeout /t 5 /nobreak

REM ===============================================
REM Step 7: Run Batch Analytics
REM ===============================================
echo.
echo Step 7: Running Batch Analytics...
cd /d "%BASE_DIR%\BatchAnalytics\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to BatchAnalytics\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\BatchAnalytics"
python batch_processor.py
if errorlevel 1 (
    echo ERROR: Batch analytics failed
    pause
    exit /b 1
)
echo Batch analytics completed successfully

timeout /t 2 /nobreak

REM ===============================================
REM Step 7a: Execute SQL again (Metrics and Tables)
REM ===============================================
echo.
echo Step 7a: Executing SQL again (Metrics and Tables)...
cd /d "%BASE_DIR%\MetricsandTables\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to MetricsandTables\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\MetricsandTables"
python execute_sql.py
if errorlevel 1 (
    echo ERROR: SQL execution failed
    pause
    exit /b 1
)
echo Metrics and Tables execution completed successfully

REM ===============================================
REM Step 8: Run Threshold Execution
REM ===============================================
echo.
echo Step 8: Running Threshold Execution...
cd /d "%BASE_DIR%\ThresholdExecution\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to ThresholdExecution\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\ThresholdExecution"
python main.py
if errorlevel 1 (
    echo ERROR: Threshold execution failed
    pause
    exit /b 1
)
echo Threshold execution completed successfully

timeout /t 10 /nobreak

REM ===============================================
REM Step 9: Run Alarm Creation
REM ===============================================
echo.
echo Step 9: Running Alarm Creation...
cd /d "%BASE_DIR%\AlarmCreation\venv\Scripts"
if errorlevel 1 (
    echo ERROR: Cannot navigate to AlarmCreation\venv\Scripts
    pause
    exit /b 1
)

call activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment
    pause
    exit /b 1
)

cd /d "%BASE_DIR%\AlarmCreation"
python alarm_processor.py
if errorlevel 1 (
    echo ERROR: Alarm creation failed
    pause
    exit /b 1
)
echo Alarm creation completed successfully

timeout /t 10 /nobreak

REM ===============================================
REM Pipeline Complete
REM ===============================================
echo.
echo ===============================================
echo TNCP R Data Processing Pipeline completed successfully!
echo ===============================================
echo.
echo All steps have been executed successfully.
echo.
pause
