@echo off
REM Setup script for Airflow PostgreSQL connection (Windows)
REM Run this once after starting the environment with: docker compose up -d

echo Setting up Airflow PostgreSQL connection...

REM Wait for Airflow to be ready
echo Waiting for Airflow webserver to be ready...
:WAIT_LOOP
curl -f http://localhost:8080/health >nul 2>&1
if %errorlevel% neq 0 (
    echo Waiting for Airflow...
    timeout /t 5 /nobreak >nul
    goto WAIT_LOOP
)

REM Create the PostgreSQL connection
echo Creating PostgreSQL connection in Airflow...
docker exec data-engineering-sandbox-airflow-webserver-1 airflow connections add "postgres_default" --conn-type "postgres" --conn-host "postgres" --conn-login "dw_user" --conn-password "dw_password" --conn-port 5432 --conn-schema "datawarehouse" --conn-description "Data Warehouse PostgreSQL Connection"

if %errorlevel% neq 0 (
    echo Connection may already exist, which is fine.
)

echo.
echo ✅ Airflow connection setup completed!
echo You can now run DAGs in Airflow at http://localhost:8080
