#!/bin/bash

# 🚀 Pulso-Back Setup Script
# Initial setup for development environment

set -e

echo "🔧 Setting up Pulso-Back development environment..."

# Check Python version
if ! command -v python3.11 &> /dev/null; then
    echo "❌ Python 3.11 required but not found"
    exit 1
fi

# Create virtual environment
echo "📦 Creating virtual environment..."
python3.11 -m venv .venv
source .venv/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Copy environment file
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file..."
    cp .env.example .env
    echo "✏️ Please edit .env with your configuration"
fi

# Create directories
echo "📁 Creating directories..."
mkdir -p logs
mkdir -p credentials
mkdir -p etl_outputs
mkdir -p data_exports

# Set up pre-commit hooks
echo "🪝 Setting up pre-commit hooks..."
pre-commit install

# Run initial tests
echo "🧪 Running initial tests..."
pytest tests/ -v --tb=short

# Check code quality
echo "🔍 Checking code quality..."
black --check app/
isort --check-only app/
flake8 app/
mypy app/

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env with your BigQuery credentials"
echo "2. Add your service account JSON to credentials/"
echo "3. Run: docker-compose up -d"
echo "4. Test API: curl http://localhost:8000/health"
echo ""
echo "Happy coding! 🚀"