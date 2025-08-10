# Troubleshooting Guide - ICO CRM Analytics Pipeline

## Quick Health Check

### 1. Check All Services Status
```bash
docker-compose ps
```
**Expected output:** All services should show "Up" status
- ✅ ico-zookeeper, ico-kafka, ico-schema-registry, ico-postgres, ico-data-generator, ico-grafana, ico-kafka-ui, ico-kafka-consumer

### 2. Verify Data Flow
```bash
# Check if events are being generated
docker logs ico-data-generator --tail 10

# Check if events are being processed  
docker logs ico-kafka-consumer --tail 10

# Check database has data
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "SELECT COUNT(*) FROM raw_data.clickstream_events;"
```

---

## Common Issues and Solutions

### Issue 1: Container Won't Start

**Symptoms:**
- Service exits immediately
- "Exited (1)" status in `docker-compose ps`

**Diagnosis:**
```bash
# Check container logs
docker logs [container-name]

# Check system resources
docker system df
free -h
```

**Common Causes & Solutions:**

**A) Port Already in Use**
```bash
# Check what's using the port
sudo netstat -tulpn | grep :5432
sudo netstat -tulpn | grep :3000

# Kill the process or change port in docker-compose.yml
```

**B) Out of Memory**
```bash
# Check available memory
free -h

# Increase Docker memory limit or close other applications
```

**C) Permission Issues**
```bash
# Fix Docker permissions
sudo chmod 666 /var/run/docker.sock
```

---

### Issue 2: No Data in Grafana Dashboards

**Symptoms:**
- Dashboards show "No data"
- Database query errors

**Step-by-Step Diagnosis:**

**Step 1: Check Data Generator**
```bash
docker logs ico-data-generator --tail 20 -f
```
**Expected:** Should see messages like "Sent clickstream event"

**If no messages:**
```bash
# Restart data generator
docker restart ico-data-generator

# Check if Kafka is accessible
docker exec ico-data-generator ping kafka
```

**Step 2: Check Kafka Topics**
```bash
# List topics
docker exec ico-kafka kafka-topics --list --bootstrap-server localhost:9092

# Check topic has messages
docker exec ico-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic clickstream-events \
  --from-beginning --max-messages 5
```

**Step 3: Check Consumer Processing**
```bash
docker logs ico-kafka-consumer --tail 30
```
**Expected:** Should see "Inserted X events" messages

**If no processing:**
```bash
# Check consumer can connect to Kafka
docker exec ico-kafka-consumer ping kafka

# Check consumer can connect to PostgreSQL  
docker exec ico-kafka-consumer ping postgres
```

**Step 4: Check Database Content**
```bash
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "
  SELECT 
    'clickstream_events' as table_name, 
    COUNT(*) as record_count,
    MAX(created_at) as latest_record
  FROM raw_data.clickstream_events
  UNION ALL
  SELECT 'user_registrations', COUNT(*), MAX(created_at) FROM raw_data.user_registrations;
"
```

**Step 5: Check Materialized Views**
```bash
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "
  SELECT schemaname, matviewname, hasindexes 
  FROM pg_matviews 
  WHERE schemaname = 'analytics';
"
```

**If views don't exist, recreate them:**
```bash
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "
  CREATE MATERIALIZED VIEW analytics.current_active_sessions AS
  SELECT session_id, COUNT(*) as event_count, MAX(timestamp) as last_activity
  FROM raw_data.clickstream_events 
  WHERE timestamp >= NOW() - INTERVAL '30 minutes'
  GROUP BY session_id;
"
```

---

### Issue 3: Kafka Connection Errors

**Symptoms:**
- "Connection refused" errors
- Consumer can't connect to Kafka

**Solutions:**

**A) Wait for Kafka Startup**
Kafka takes 30-60 seconds to fully initialize.
```bash
# Check Kafka health
docker exec ico-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

**B) Network Issues**
```bash
# Test container networking
docker exec ico-kafka-consumer ping kafka
docker exec ico-kafka-consumer nslookup kafka

# Restart networking
docker-compose down && docker-compose up -d
```

**C) Kafka Configuration Issues**
Check `docker-compose.yml` advertised listeners:
```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
```

---

### Issue 4: Database Connection Errors

**Symptoms:**
- "Connection refused" to PostgreSQL
- Authentication failures

**Solutions:**

**A) Check PostgreSQL Status**
```bash
# Check if PostgreSQL is ready
docker exec ico-postgres pg_isready -U ico_user

# Check logs for initialization errors
docker logs ico-postgres --tail 20
```

**B) Connection String Issues**
Verify environment variables in consumer:
```yaml
environment:
  POSTGRES_HOST: postgres      # Must match service name
  POSTGRES_DB: ico_analytics
  POSTGRES_USER: ico_user
  POSTGRES_PASSWORD: ico_password
```

**C) Database Not Initialized**
```bash
# Connect manually and check schema
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "\dt raw_data.*"

# If tables don't exist, check init script
docker exec ico-postgres cat /docker-entrypoint-initdb.d/init.sql
```

---

### Issue 5: Grafana Dashboard Errors

**Symptoms:**
- "Database query error"
- "Table/column does not exist"

**Common Database Errors:**

**A) Missing Tables/Views**
```sql
-- Check what exists
\dt raw_data.*
\dt analytics.*
\dv analytics.*

-- Recreate missing view
CREATE MATERIALIZED VIEW analytics.missing_view AS ...;
```

**B) Permission Issues**
```sql
-- Grant permissions to Grafana user
GRANT USAGE ON SCHEMA raw_data TO grafana_user;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_data TO grafana_user;
GRANT SELECT ON analytics.current_active_sessions TO grafana_user;
```

**C) Data Type Issues**
Common fixes:
```sql
-- Use CAST instead of ROUND function
CAST(AVG(page_load_time) AS DECIMAL(10,3)) 
-- Instead of: ROUND(AVG(page_load_time), 3)

-- Handle NULL values
COALESCE(column_name, 0)
```

---

### Issue 6: Performance Issues

**Symptoms:**
- Slow dashboard loading
- High memory usage
- Consumer lag

**Solutions:**

**A) Database Performance**
```sql
-- Check slow queries
SELECT query, mean_time, calls 
FROM pg_stat_statements 
ORDER BY mean_time DESC LIMIT 5;

-- Add missing indexes
CREATE INDEX idx_events_timestamp ON raw_data.clickstream_events(timestamp);
```

**B) Memory Issues**
```bash
# Check memory usage
docker stats

# Increase consumer batch size (less frequent DB writes)
batch_size = 500  # instead of 100
```

**C) Consumer Lag**
```bash
# Check Kafka consumer group lag
docker exec ico-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group postgres-consumer-group
```

---

## Recovery Procedures

### Complete Pipeline Reset

**When to use:** Major corruption or configuration issues

```bash
# Stop everything
docker-compose down

# Remove volumes (WARNING: Deletes all data)
docker volume rm data-pipeline_postgres_data data-pipeline_grafana_data

# Remove containers
docker-compose rm -f

# Rebuild and restart
docker-compose build --no-cache
docker-compose up -d
```

### Partial Recovery

**Reset Only Database:**
```bash
docker-compose stop postgres kafka-consumer
docker volume rm data-pipeline_postgres_data
docker-compose up -d postgres
# Wait 30 seconds
docker-compose up -d kafka-consumer
```

**Reset Only Kafka:**
```bash
docker-compose stop kafka kafka-consumer data-generator
docker-compose up -d kafka
# Wait 60 seconds
docker-compose up -d data-generator kafka-consumer
```

---

## Monitoring Commands

### Real-time Monitoring

**Data Flow Monitoring:**
```bash
# Watch consumer processing
docker logs ico-kafka-consumer -f | grep "Inserted"

# Monitor database growth
watch "docker exec ico-postgres psql -U ico_user -d ico_analytics -c 'SELECT COUNT(*) FROM raw_data.clickstream_events;'"

# Monitor Kafka topics
docker exec ico-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic clickstream-events
```

**Resource Monitoring:**
```bash
# Monitor Docker resource usage
docker stats

# Monitor disk space
df -h
docker system df
```

### Health Check Script

Create a monitoring script:
```bash
#!/bin/bash
# health_check.sh

echo "=== Container Status ==="
docker-compose ps

echo -e "\n=== Data Counts ==="
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "
SELECT 'Clickstream' as type, COUNT(*) as count FROM raw_data.clickstream_events
UNION ALL SELECT 'Registrations', COUNT(*) FROM raw_data.user_registrations  
UNION ALL SELECT 'Conversions', COUNT(*) FROM raw_data.conversions;"

echo -e "\n=== Recent Activity ==="
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "
SELECT MAX(created_at) as latest_event FROM raw_data.clickstream_events;"

echo -e "\n=== Consumer Lag ==="
docker logs ico-kafka-consumer --tail 3 | grep "Inserted"
```

---

## Getting Help

### Log Collection

Before asking for help, collect these logs:
```bash
# Container status
docker-compose ps > container_status.txt

# All container logs
docker logs ico-kafka > kafka.log 2>&1
docker logs ico-postgres > postgres.log 2>&1  
docker logs ico-kafka-consumer > consumer.log 2>&1
docker logs ico-data-generator > generator.log 2>&1

# System information
docker version > system_info.txt
docker-compose version >> system_info.txt
free -h >> system_info.txt
```

### Common Error Messages

**"bind: address already in use"**
- Solution: Change ports in docker-compose.yml or kill existing process

**"no space left on device"**  
- Solution: Clean Docker: `docker system prune -a`

**"connection refused"**
- Solution: Check service startup order and networking

**"relation does not exist"**
- Solution: Check database initialization and schema creation

**"permission denied"**
- Solution: Check user permissions and Docker socket access

---

This troubleshooting guide covers the most common issues you'll encounter. For complex problems, collect logs and system information before seeking help.