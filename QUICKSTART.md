# ICO Analytics Platform - Quick Start Guide

This guide will help you get the complete ICO Analytics Platform running with all dashboards and data configured.

## Prerequisites

- Docker and Docker Compose installed
- At least 4GB RAM available
- Ports 3000, 5432, 8080, 8081, 9092 available

## Quick Setup (New PC)

1. **Clone/Copy the project**
   ```bash
   # If using Git
   git clone <repository-url>
   cd data-pipeline
   
   # Or copy all files to your new PC
   ```

2. **One-command setup**
   ```bash
   ./scripts/setup-verification.sh
   ```

3. **Manual setup (if automated script fails)**
   ```bash
   # Copy environment file
   cp .env.example .env
   
   # Start all services
   docker-compose up -d
   
   # Wait 30 seconds for initialization
   sleep 30
   
   # Verify setup
   ./scripts/setup-verification.sh
   ```

## Access Points

After setup completes:

- **Grafana Dashboards**: http://localhost:3000
  - Username: `admin`
  - Password: `admin04`
  - Pre-configured dashboards: ICO Real-time Analytics, ICO Business Metrics

- **Kafka UI**: http://localhost:8080
  - Monitor Kafka topics and messages

- **PostgreSQL**: localhost:5432
  - Database: `ico_analytics`
  - Username: `ico_user`
  - Password: `ico_password`

## What's Included

### Pre-configured Dashboards
1. **ICO Real-time Analytics** (`/d/ico-realtime/ico-real-time-analytics`)
   - Active sessions by country
   - Current traffic metrics
   - Real-time event tracking

2. **ICO Business Metrics** (`/d/ico-business/ico-business-metrics`)
   - Revenue by ICO website
   - Conversion funnels
   - Registration and revenue trends

### Database Schema
- **Raw Data**: `clickstream_events`, `user_registrations`, `conversions`
- **Processed Data**: `ico_websites` (20 sample ICO websites)
- **Analytics**: Pre-built views for dashboard queries

### Sample Data
- 20 ICO websites with realistic data
- Continuous sample data generation
- Revenue and conversion tracking

## Troubleshooting

### If Grafana shows "No data"
```bash
# Check if data generator is running
docker logs ico-data-generator

# Restart data generator if needed
docker restart ico-data-generator
```

### If database queries fail
```bash
# Check PostgreSQL logs
docker logs ico-postgres

# Verify database schema
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "\dt raw_data.*"
```

### Reset everything
```bash
# Stop all services and remove volumes
docker-compose down -v

# Restart
docker-compose up -d
```

## Architecture

```
[Data Generator] → [Kafka] → [Kafka Consumer] → [PostgreSQL]
                                                      ↓
[Grafana Dashboards] ← [Analytics Views] ← [Raw Data Tables]
```

## Development

- Dashboards are automatically provisioned from `grafana/dashboards/`
- Database schema is initialized from `postgres/init_complete.sql`
- All configurations persist in the codebase for easy deployment

## Configuration Files

- `docker-compose.yml` - Container orchestration
- `.env` - Environment variables
- `grafana/provisioning/` - Grafana auto-configuration
- `postgres/init_complete.sql` - Database initialization
- `scripts/setup-verification.sh` - Automated setup verification