#!/bin/bash
# ========================================
# Database Setup Script for Pulso-Back
# Creates dedicated user and database with proper permissions
# ========================================

set -e  # Exit on any error

# Configuration
DB_NAME="pulso_db"
DB_USER="pulso_user"
DB_PASSWORD="pulso_secure_pass_2025"
POSTGRES_USER="postgres"

echo "🗄️ Setting up PostgreSQL for Pulso-Back..."
echo "Database: $DB_NAME"
echo "User: $DB_USER"

# Function to run SQL as postgres superuser
run_sql() {
    psql -U $POSTGRES_USER -h localhost -c "$1"
}

# Function to run SQL on specific database
run_sql_db() {
    psql -U $POSTGRES_USER -h localhost -d "$1" -c "$2"
}

echo ""
echo "📦 Step 1: Creating database..."
if run_sql "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" | grep -q "1 row"; then
    echo "✅ Database $DB_NAME already exists"
else
    run_sql "CREATE DATABASE $DB_NAME;"
    echo "✅ Database $DB_NAME created"
fi

echo ""
echo "👤 Step 2: Creating user..."
if run_sql "SELECT 1 FROM pg_user WHERE usename='$DB_USER'" | grep -q "1 row"; then
    echo "⚠️ User $DB_USER already exists, updating password..."
    run_sql "ALTER USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"
else
    run_sql "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"
    echo "✅ User $DB_USER created"
fi

echo ""
echo "🔑 Step 3: Granting permissions..."

# Grant connection to database
run_sql "GRANT CONNECT ON DATABASE $DB_NAME TO $DB_USER;"

# Grant schema permissions
run_sql_db "$DB_NAME" "GRANT USAGE ON SCHEMA public TO $DB_USER;"
run_sql_db "$DB_NAME" "GRANT CREATE ON SCHEMA public TO $DB_USER;"

# Grant table permissions (current and future)
run_sql_db "$DB_NAME" "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $DB_USER;"
run_sql_db "$DB_NAME" "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $DB_USER;"

# Grant permissions on future objects
run_sql_db "$DB_NAME" "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $DB_USER;"
run_sql_db "$DB_NAME" "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $DB_USER;"

echo "✅ Permissions granted"

echo ""
echo "🔍 Step 4: Testing connection..."
if psql -U $DB_USER -h localhost -d $DB_NAME -c "SELECT 'Connection successful' as status;" > /dev/null 2>&1; then
    echo "✅ Connection test successful"
else
    echo "❌ Connection test failed"
    exit 1
fi

echo ""
echo "📋 Step 5: Connection details..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo "Password: $DB_PASSWORD"
echo ""
echo "Connection URL:"
echo "postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME"
echo ""
echo "Update your .env file with:"
echo "POSTGRES_URL=postgresql://$DB_USER:$DB_PASSWORD@localhost:5432/$DB_NAME"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "🎯 Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Update your .env file with the connection URL above"
echo "2. Run: yoyo apply"
echo "3. Run: uvicorn app.main:app --reload"
