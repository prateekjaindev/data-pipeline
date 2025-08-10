#!/bin/bash

# ICO Analytics Pipeline Setup Verification Script
# This script verifies that all files are in place and the project is ready to run

set -e

echo "🔍 ICO Analytics Pipeline - Setup Verification"
echo "=============================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

check_file() {
    if [ -f "$1" ]; then
        echo -e "✅ ${GREEN}$1${NC}"
        return 0
    else
        echo -e "❌ ${RED}$1 - MISSING${NC}"
        return 1
    fi
}

check_directory() {
    if [ -d "$1" ]; then
        echo -e "✅ ${GREEN}$1/${NC}"
        return 0
    else
        echo -e "❌ ${RED}$1/ - MISSING${NC}"
        return 1
    fi
}

check_docker() {
    if command -v docker &> /dev/null; then
        echo -e "✅ ${GREEN}Docker is installed${NC}"
        if docker info &> /dev/null; then
            echo -e "✅ ${GREEN}Docker daemon is running${NC}"
            return 0
        else
            echo -e "❌ ${RED}Docker daemon is not running${NC}"
            return 1
        fi
    else
        echo -e "❌ ${RED}Docker is not installed${NC}"
        return 1
    fi
}

check_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        echo -e "✅ ${GREEN}Docker Compose is installed${NC}"
        return 0
    else
        echo -e "❌ ${RED}Docker Compose is not installed${NC}"
        return 1
    fi
}

echo -e "\n📋 Checking Prerequisites..."
echo "----------------------------"

prerequisites_ok=0
check_docker || ((prerequisites_ok++))
check_docker_compose || ((prerequisites_ok++))

echo -e "\n📁 Checking Project Structure..."
echo "--------------------------------"

structure_ok=0

# Root files
check_file "README.md" || ((structure_ok++))
check_file "docker-compose.yml" || ((structure_ok++))
check_file ".env.example" || ((structure_ok++))
check_file "Makefile" || ((structure_ok++))

# Directories
check_directory "data-generator" || ((structure_ok++))
check_directory "flink-processor" || ((structure_ok++))
check_directory "postgres" || ((structure_ok++))
check_directory "grafana" || ((structure_ok++))
check_directory "schemas" || ((structure_ok++))

# Data Generator
echo -e "\n🐍 Checking Data Generator..."
echo "----------------------------"
check_file "data-generator/app.py" || ((structure_ok++))
check_file "data-generator/requirements.txt" || ((structure_ok++))
check_file "data-generator/Dockerfile" || ((structure_ok++))
check_file "data-generator/config/ico_websites.py" || ((structure_ok++))

# Flink Processor
echo -e "\n⚡ Checking Flink Processor..."
echo "-----------------------------"
check_file "flink-processor/pom.xml" || ((structure_ok++))
check_file "flink-processor/requirements.txt" || ((structure_ok++))
check_file "flink-processor/Dockerfile" || ((structure_ok++))
check_file "flink-processor/python_processor.py" || ((structure_ok++))
check_file "flink-processor/src/main/java/com/ico/analytics/ICOAnalyticsJob.java" || ((structure_ok++))

# PostgreSQL
echo -e "\n🐘 Checking PostgreSQL Setup..."
echo "------------------------------"
check_file "postgres/Dockerfile" || ((structure_ok++))
check_file "postgres/init.sql" || ((structure_ok++))

# Grafana
echo -e "\n📊 Checking Grafana Configuration..."
echo "-----------------------------------"
check_file "grafana/provisioning/datasources/postgres.yml" || ((structure_ok++))
check_file "grafana/provisioning/dashboards/dashboard.yml" || ((structure_ok++))
check_file "grafana/dashboards/ico-realtime-dashboard.json" || ((structure_ok++))
check_file "grafana/dashboards/ico-business-metrics.json" || ((structure_ok++))

# Schemas
echo -e "\n📋 Checking Avro Schemas..."
echo "--------------------------"
check_file "schemas/clickstream-event.avsc" || ((structure_ok++))
check_file "schemas/user-registration.avsc" || ((structure_ok++))
check_file "schemas/conversion-event.avsc" || ((structure_ok++))
check_file "schemas/README.md" || ((structure_ok++))

# Tests
echo -e "\n🧪 Checking Test Suite..."
echo "------------------------"
check_file "tests/integration_test.py" || ((structure_ok++))

# Check available ports
echo -e "\n🔌 Checking Port Availability..."
echo "-------------------------------"
ports=(3000 5432 8080 8081 8082 9092)
ports_ok=0

for port in "${ports[@]}"; do
    if ! netstat -tuln 2>/dev/null | grep ":$port " > /dev/null; then
        echo -e "✅ ${GREEN}Port $port is available${NC}"
    else
        echo -e "⚠️  ${YELLOW}Port $port is in use${NC}"
        ((ports_ok++))
    fi
done

# Check available resources
echo -e "\n💾 Checking System Resources..."
echo "------------------------------"
resources_ok=0

# Check available memory (at least 6GB free)
if command -v free &> /dev/null; then
    available_mem=$(free -g | awk 'NR==2{printf "%.1f", $7}')
    if (( $(echo "$available_mem > 6" | bc -l) )); then
        echo -e "✅ ${GREEN}Available memory: ${available_mem}GB (sufficient)${NC}"
    else
        echo -e "⚠️  ${YELLOW}Available memory: ${available_mem}GB (minimum 6GB recommended)${NC}"
        ((resources_ok++))
    fi
else
    echo -e "⚠️  ${YELLOW}Cannot check memory availability${NC}"
fi

# Check available disk space (at least 10GB free)
if command -v df &> /dev/null; then
    available_disk=$(df -BG . | awk 'NR==2{print $4}' | sed 's/G//')
    if [ "$available_disk" -gt 10 ]; then
        echo -e "✅ ${GREEN}Available disk space: ${available_disk}GB (sufficient)${NC}"
    else
        echo -e "⚠️  ${YELLOW}Available disk space: ${available_disk}GB (minimum 10GB recommended)${NC}"
        ((resources_ok++))
    fi
else
    echo -e "⚠️  ${YELLOW}Cannot check disk space availability${NC}"
fi

# Summary
echo -e "\n📊 Verification Summary"
echo "======================"

if [ $prerequisites_ok -eq 0 ] && [ $structure_ok -eq 0 ]; then
    echo -e "🎉 ${GREEN}Setup verification PASSED!${NC}"
    echo ""
    echo -e "🚀 ${GREEN}Your ICO Analytics Pipeline is ready to start!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Copy environment file:    cp .env.example .env"
    echo "2. Start the pipeline:       make start"
    echo "3. Check status:             make status"
    echo "4. View logs:                make logs"
    echo "5. Access dashboards:        http://localhost:3000"
    echo ""
    echo "Or use the quick start:      make quick-start"
    
    if [ $ports_ok -gt 0 ]; then
        echo ""
        echo -e "⚠️  ${YELLOW}Warning: Some ports are in use. You may need to stop other services.${NC}"
    fi
    
    if [ $resources_ok -gt 0 ]; then
        echo ""
        echo -e "⚠️  ${YELLOW}Warning: System resources may be insufficient for optimal performance.${NC}"
    fi
    
    exit 0
else
    echo -e "❌ ${RED}Setup verification FAILED!${NC}"
    echo ""
    echo "Issues found:"
    [ $prerequisites_ok -gt 0 ] && echo "- $prerequisites_ok prerequisite issue(s)"
    [ $structure_ok -gt 0 ] && echo "- $structure_ok project structure issue(s)"
    echo ""
    echo "Please resolve the issues above before starting the pipeline."
    exit 1
fi