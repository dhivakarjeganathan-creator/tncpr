@echo off
REM Build script for Docker image (Windows)

set IMAGE_NAME=wxdata-pipeline
set IMAGE_TAG=latest
set FULL_IMAGE_NAME=%IMAGE_NAME%:%IMAGE_TAG%

echo Building Docker image: %FULL_IMAGE_NAME%

REM Build the Docker image
docker build -t %FULL_IMAGE_NAME% .

if %ERRORLEVEL% EQU 0 (
    echo Docker image built successfully: %FULL_IMAGE_NAME%
    echo.
    echo To run the container:
    echo   docker run --rm %FULL_IMAGE_NAME%
    echo.
    echo To run with custom config:
    echo   docker run --rm -v %cd%/config.yaml:/app/config.yaml %FULL_IMAGE_NAME% --config /app/config.yaml
    echo.
    echo To run in continuous mode:
    echo   docker run --rm %FULL_IMAGE_NAME% --mode continuous
) else (
    echo Docker build failed!
    exit /b %ERRORLEVEL%
)

