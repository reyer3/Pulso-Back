# ========================================
# Database Setup Script for Pulso-Back (Windows PowerShell)
# Creates dedicated user and database with proper permissions
# ========================================

param(
    [string]$PostgresUser = "postgres",
    [string]$DbName = "pulso_db",
    [string]$DbUser = "pulso_user",
    [string]$DbPassword = "pulso_secure_pass_2025"
)

$ErrorActionPreference = "Stop"

Write-Host "🗄️ Setting up PostgreSQL for Pulso-Back..." -ForegroundColor Green
Write-Host "Database: $DbName" -ForegroundColor Cyan
Write-Host "User: $DbUser" -ForegroundColor Cyan

# Function to run SQL commands
function Invoke-SQL {
    param([string]$Query, [string]$Database = "postgres")
    
    if ($Database -eq "postgres") {
        $result = psql -U $PostgresUser -h localhost -c "$Query"
    } else {
        $result = psql -U $PostgresUser -h localhost -d $Database -c "$Query"
    }
    
    if ($LASTEXITCODE -ne 0) {
        throw "SQL command failed: $Query"
    }
    return $result
}

try {
    Write-Host ""
    Write-Host "📦 Step 1: Creating database..." -ForegroundColor Yellow
    
    $dbExists = Invoke-SQL "SELECT 1 FROM pg_database WHERE datname='$DbName'"
    if ($dbExists -match "1 row") {
        Write-Host "✅ Database $DbName already exists" -ForegroundColor Green
    } else {
        Invoke-SQL "CREATE DATABASE $DbName;"
        Write-Host "✅ Database $DbName created" -ForegroundColor Green
    }

    Write-Host ""
    Write-Host "👤 Step 2: Creating user..." -ForegroundColor Yellow
    
    $userExists = Invoke-SQL "SELECT 1 FROM pg_user WHERE usename='$DbUser'"
    if ($userExists -match "1 row") {
        Write-Host "⚠️ User $DbUser already exists, updating password..." -ForegroundColor Yellow
        Invoke-SQL "ALTER USER $DbUser WITH PASSWORD '$DbPassword';"
    } else {
        Invoke-SQL "CREATE USER $DbUser WITH PASSWORD '$DbPassword';"
        Write-Host "✅ User $DbUser created" -ForegroundColor Green
    }

    Write-Host ""
    Write-Host "🔑 Step 3: Granting permissions..." -ForegroundColor Yellow

    # Grant connection to database
    Invoke-SQL "GRANT CONNECT ON DATABASE $DbName TO $DbUser;"

    # Grant schema permissions
    Invoke-SQL "GRANT USAGE ON SCHEMA public TO $DbUser;" -Database $DbName
    Invoke-SQL "GRANT CREATE ON SCHEMA public TO $DbUser;" -Database $DbName

    # Grant table permissions (current and future)
    Invoke-SQL "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $DbUser;" -Database $DbName
    Invoke-SQL "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $DbUser;" -Database $DbName

    # Grant permissions on future objects
    Invoke-SQL "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DbUser;" -Database $DbName
    Invoke-SQL "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DbUser;" -Database $DbName

    Write-Host "✅ Permissions granted" -ForegroundColor Green

    Write-Host ""
    Write-Host "🔍 Step 4: Testing connection..." -ForegroundColor Yellow
    
    $null = psql -U $DbUser -h localhost -d $DbName -c "SELECT 'Connection successful' as status;" 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Connection test successful" -ForegroundColor Green
    } else {
        throw "Connection test failed"
    }

    Write-Host ""
    Write-Host "📋 Step 5: Connection details..." -ForegroundColor Yellow
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
    Write-Host "Database: $DbName" -ForegroundColor White
    Write-Host "User: $DbUser" -ForegroundColor White
    Write-Host "Password: $DbPassword" -ForegroundColor White
    Write-Host ""
    Write-Host "Connection URL:" -ForegroundColor White
    Write-Host "postgresql://${DbUser}:${DbPassword}@localhost:5432/$DbName" -ForegroundColor Green
    Write-Host ""
    Write-Host "Update your .env file with:" -ForegroundColor White
    Write-Host "POSTGRES_URL=postgresql://${DbUser}:${DbPassword}@localhost:5432/$DbName" -ForegroundColor Green
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan

    Write-Host ""
    Write-Host "🎯 Setup completed successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "Next steps:" -ForegroundColor Yellow
    Write-Host "1. Update your .env file with the connection URL above" -ForegroundColor White
    Write-Host "2. Run: yoyo apply" -ForegroundColor White
    Write-Host "3. Run: uvicorn app.main:app --reload" -ForegroundColor White

    # Automatically update .env file if it exists
    if (Test-Path ".env") {
        Write-Host ""
        Write-Host "🔧 Automatically updating .env file..." -ForegroundColor Yellow
        
        $envContent = Get-Content ".env" -Raw
        $newUrl = "postgresql://${DbUser}:${DbPassword}@localhost:5432/$DbName"
        
        if ($envContent -match "POSTGRES_URL=.*") {
            $envContent = $envContent -replace "POSTGRES_URL=.*", "POSTGRES_URL=$newUrl"
        } else {
            $envContent += "`nPOSTGRES_URL=$newUrl"
        }
        
        Set-Content ".env" $envContent
        Write-Host "✅ .env file updated automatically" -ForegroundColor Green
    }

} catch {
    Write-Host ""
    Write-Host "❌ Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
    Write-Host "Troubleshooting tips:" -ForegroundColor Yellow
    Write-Host "1. Make sure PostgreSQL is running" -ForegroundColor White
    Write-Host "2. Check if 'psql' command is available in PATH" -ForegroundColor White
    Write-Host "3. Verify postgres user credentials" -ForegroundColor White
    exit 1
}
