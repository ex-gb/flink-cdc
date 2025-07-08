#!/bin/bash

set -e

echo "🧹 Cleaning up PostgreSQL CDC Local Environment"

# Stop and remove containers
echo "🐳 Stopping and removing Docker containers..."
docker-compose down -v

# Remove output directories
echo "🗑️  Removing output directories..."
rm -rf output/
rm -rf logs/

# Remove any SBT target directories
echo "🗑️  Removing SBT target directories..."
rm -rf target/
rm -rf project/target/

echo "✅ Cleanup complete!"
echo ""
echo "To start fresh:"
echo "./scripts/setup.sh" 