#!/bin/bash

# ICO Analytics Platform Setup Verification Script
# This script verifies that all components are properly configured and working

echo "🚀 Starting ICO Analytics Platform Setup Verification..."

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ .env file not found. Copying from .env.example..."
    cp .env.example .env
fi

echo "✅ Prerequisites check passed"

# Start the services
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services to start
echo "⏳ Waiting for services to initialize..."
sleep 30

# Check PostgreSQL
echo "🐘 Checking PostgreSQL connection..."
if docker exec ico-postgres pg_isready -U ico_user -d ico_analytics > /dev/null 2>&1; then
    echo "✅ PostgreSQL is running and accessible"
else
    echo "❌ PostgreSQL connection failed"
    exit 1
fi

# Check if database schema is properly initialized
echo "🔍 Verifying database schema..."
TABLES_COUNT=$(docker exec ico-postgres psql -U ico_user -d ico_analytics -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('raw_data', 'processed_data', 'analytics');")
if [ "${TABLES_COUNT// }" -ge "5" ]; then
    echo "✅ Database schema initialized with $TABLES_COUNT tables"
else
    echo "❌ Database schema not properly initialized"
    exit 1
fi

# Check ICO websites data
echo "📊 Checking reference data..."
ICO_COUNT=$(docker exec ico-postgres psql -U ico_user -d ico_analytics -t -c "SELECT COUNT(*) FROM processed_data.ico_websites;")
if [ "${ICO_COUNT// }" -ge "20" ]; then
    echo "✅ ICO reference data loaded ($ICO_COUNT websites)"
else
    echo "❌ ICO reference data not loaded"
    exit 1
fi

# Check analytics views
echo "📈 Verifying analytics views..."
VIEWS_COUNT=$(docker exec ico-postgres psql -U ico_user -d ico_analytics -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'analytics' AND table_type = 'VIEW';")
if [ "${VIEWS_COUNT// }" -ge "3" ]; then
    echo "✅ Analytics views created ($VIEWS_COUNT views)"
else
    echo "❌ Analytics views not properly created"
    exit 1
fi

# Check Kafka
echo "📨 Checking Kafka..."
if docker exec ico-kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    echo "✅ Kafka is running"
else
    echo "❌ Kafka connection failed"
    exit 1
fi

# Check Grafana
echo "📊 Checking Grafana..."
GRAFANA_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health)
if [ "$GRAFANA_STATUS" = "200" ]; then
    echo "✅ Grafana is running at http://localhost:3000"
    echo "   Login: admin / admin04"
else
    echo "❌ Grafana connection failed"
    exit 1
fi

# Check Grafana datasource
echo "🔌 Checking Grafana datasource..."
DATASOURCE_STATUS=$(curl -s -u admin:admin04 http://localhost:3000/api/datasources/1/health | grep -o '"status":"OK"' || echo "")
if [ ! -z "$DATASOURCE_STATUS" ]; then
    echo "✅ Grafana datasource connection is healthy"
else
    echo "⚠️  Grafana datasource might need configuration"
fi

# Check dashboards
echo "📋 Checking Grafana dashboards..."
DASHBOARDS_COUNT=$(curl -s -u admin:admin04 "http://localhost:3000/api/search?type=dash-db" | grep -o '"title"' | wc -l)
if [ "$DASHBOARDS_COUNT" -ge "2" ]; then
    echo "✅ Grafana dashboards loaded ($DASHBOARDS_COUNT dashboards)"
else
    echo "⚠️  Some Grafana dashboards might not be loaded"
fi

echo ""
echo "🎉 Setup verification completed!"
echo ""
echo "📊 Access Points:"
echo "   - Grafana: http://localhost:3000 (admin / admin04)"
echo "   - Kafka UI: http://localhost:8080"
echo "   - PostgreSQL: localhost:5432 (ico_user / ico_password)"
echo ""
echo "🚀 To start generating sample data:"
echo "   docker logs -f ico-data-generator"
echo ""
echo "📈 Available Dashboards:"
echo "   - ICO Real-time Analytics: Real-time metrics and active sessions"
echo "   - ICO Business Metrics: Revenue analysis and conversion funnels"