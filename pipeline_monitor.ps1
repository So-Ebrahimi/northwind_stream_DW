# CDC Pipeline Monitor - PowerShell Script
# Quick commands to monitor and test the CDC pipeline

param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("insert-customer", "monitor-kafka", "check-staging", "check-dw", "track-all", "help")]
    [string]$Action = "help",
    
    [Parameter(Mandatory=$false)]
    [string]$CustomerId = "TEST1",
    
    [Parameter(Mandatory=$false)]
    [int]$OrderId = 0
)

function Write-Header {
    param([string]$Title)
    Write-Host "`n" + ("="*80) -ForegroundColor Cyan
    Write-Host $Title -ForegroundColor Cyan
    Write-Host ("="*80) -ForegroundColor Cyan
}

function Insert-Customer {
    param([string]$Id, [string]$CompanyName)
    Write-Header "INSERTING CUSTOMER INTO POSTGRESQL"
    
    $sql = "INSERT INTO customers (customer_id, company_name, contact_name, city, country) VALUES ('$Id', '$CompanyName', 'Contact $Id', 'Test City', 'Test Country');"
    $command = "psql -U postgres -d northwind -c `"$sql`""
    
    docker exec postgres $command
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Inserted customer: $Id - $CompanyName" -ForegroundColor Green
    } else {
        Write-Host "❌ Failed to insert customer" -ForegroundColor Red
    }
}

function Monitor-Kafka {
    Write-Header "KAFKA TOPICS MONITORING"
    
    Write-Host "`n📊 Listing Kafka topics..." -ForegroundColor Yellow
    docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092 | Select-String "northwind"
    
    Write-Host "`n📨 Checking recent messages from customers topic..." -ForegroundColor Yellow
    $timeout = "timeout /t 3 /nobreak >nul 2>&1; echo done"
    docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic northwind.public.customers --from-beginning --max-messages 3 --timeout-ms 3000 2>$null
}

function Check-Staging {
    Write-Header "CLICKHOUSE STAGING TABLES"
    
    $tables = @(
        "northwind.northwind_customers",
        "northwind.northwind_orders",
        "northwind.northwind_order_details",
        "northwind.northwind_products"
    )
    
    foreach ($table in $tables) {
        Write-Host "`n📊 $table" -ForegroundColor Yellow
        $count = docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM $table"
        Write-Host "   Row count: $count" -ForegroundColor White
        
        if ([int]$count -gt 0) {
            Write-Host "   Recent records:" -ForegroundColor Gray
            docker exec clickhouse1 clickhouse-client --query "SELECT * FROM $table ORDER BY updatedate DESC LIMIT 2 FORMAT PrettyCompact" | Select-Object -First 5
        }
    }
}

function Check-DataWarehouse {
    Write-Header "CLICKHOUSE DATA WAREHOUSE"
    
    $tables = @(
        "DimCustomer",
        "DimProducts",
        "DimEmployees",
        "FactOrders"
    )
    
    foreach ($table in $tables) {
        Write-Host "`n📊 $table" -ForegroundColor Yellow
        $count = docker exec clickhouse1 clickhouse-client --query "SELECT COUNT(*) FROM $table"
        Write-Host "   Row count: $count" -ForegroundColor White
        
        if ([int]$count -gt 0) {
            Write-Host "   Sample records:" -ForegroundColor Gray
            docker exec clickhouse1 clickhouse-client --query "SELECT * FROM $table ORDER BY updatedate DESC LIMIT 2 FORMAT PrettyCompact" | Select-Object -First 5
        }
    }
}

function Track-All {
    param([string]$CustomerId)
    
    Write-Header "TRACKING DATA FLOW THROUGH CDC PIPELINE"
    
    # Step 1: Insert
    Write-Host "`n[STEP 1] Inserting test customer into PostgreSQL..." -ForegroundColor Yellow
    Insert-Customer -Id $CustomerId -CompanyName "Test Company $CustomerId"
    
    Start-Sleep -Seconds 3
    
    # Step 2: Check Kafka
    Write-Host "`n[STEP 2] Checking Kafka (waiting 10 seconds for CDC capture)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
    Monitor-Kafka
    
    # Step 3: Check Staging
    Write-Host "`n[STEP 3] Checking ClickHouse Staging (waiting 5 seconds for Spark processing)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
    Check-Staging
    
    # Step 4: Check DW
    Write-Host "`n[STEP 4] Checking Data Warehouse (waiting 20 seconds for ETL job)..." -ForegroundColor Yellow
    Start-Sleep -Seconds 20
    Check-DataWarehouse
    
    Write-Host "`n" + ("="*80) -ForegroundColor Cyan
    Write-Host "DATA FLOW TRACKING COMPLETE" -ForegroundColor Cyan
    Write-Host ("="*80) -ForegroundColor Cyan
}

function Show-Help {
    Write-Header "CDC PIPELINE MONITOR - USAGE"
    Write-Host @"
Available Actions:

  insert-customer    Insert a test customer into PostgreSQL
  monitor-kafka      Monitor Kafka topics for CDC events
  check-staging      Check ClickHouse staging tables
  check-dw           Check ClickHouse data warehouse tables
  track-all          Complete data flow tracking (insert + monitor all stages)
  help               Show this help message

Examples:

  .\pipeline_monitor.ps1 -Action insert-customer -CustomerId TEST1
  .\pipeline_monitor.ps1 -Action monitor-kafka
  .\pipeline_monitor.ps1 -Action check-staging
  .\pipeline_monitor.ps1 -Action check-dw
  .\pipeline_monitor.ps1 -Action track-all -CustomerId TEST1

"@ -ForegroundColor White
}

# Main execution
switch ($Action) {
    "insert-customer" {
        Insert-Customer -Id $CustomerId -CompanyName "Test Company $CustomerId"
    }
    "monitor-kafka" {
        Monitor-Kafka
    }
    "check-staging" {
        Check-Staging
    }
    "check-dw" {
        Check-DataWarehouse
    }
    "track-all" {
        Track-All -CustomerId $CustomerId
    }
    default {
        Show-Help
    }
}

