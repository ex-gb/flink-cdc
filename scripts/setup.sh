#!/bin/bash

set -e

echo "🚀 Setting up PostgreSQL CDC to S3 Local Environment"

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p logs
mkdir -p output/cdc-events

# Start PostgreSQL with Docker Compose
echo "🐘 Starting PostgreSQL with CDC configuration..."
docker-compose up -d

# Wait for PostgreSQL to be ready
echo "⏳ Waiting for PostgreSQL to be ready..."
timeout=30
count=0
while [ $count -lt $timeout ]; do
    if docker-compose exec postgres pg_isready -U cdc_user -d cdc_source -q; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo "⏳ PostgreSQL is starting... ($count/$timeout)"
    sleep 1
    ((count++))
done

if [ $count -eq $timeout ]; then
    echo "❌ PostgreSQL failed to start within $timeout seconds"
    exit 1
fi

# Show database status
echo "📊 Database status:"
docker-compose exec postgres psql -U cdc_user -d cdc_source -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Build the application: sbt compile"
echo "2. Run the CDC job: sbt \"runMain com.example.cdc.PostgresCdcToFileJob\""
echo "3. Test with sample data: ./scripts/test-cdc.sh"
echo ""
echo "To connect to PostgreSQL:"
echo "docker exec -it postgres-cdc psql -U cdc_user -d cdc_source" 