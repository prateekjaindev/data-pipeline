# ICO CRM Analytics Pipeline - Complete Documentation

## Table of Contents

1. [Introduction to Data Pipelines](#introduction-to-data-pipelines)
2. [Architecture Overview](#architecture-overview)
3. [Component Deep Dive](#component-deep-dive)
4. [Data Flow Explanation](#data-flow-explanation)
5. [Configuration Details](#configuration-details)
6. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
7. [Scaling and Production Considerations](#scaling-and-production-considerations)

---

## Introduction to Data Pipelines

### What is a Data Pipeline?

A **data pipeline** is a series of data processing steps that collect, transform, and store data from various sources to a destination where it can be analyzed. Think of it like a factory assembly line, but for data:

1. **Data Ingestion**: Raw data enters the pipeline
2. **Data Processing**: Data is cleaned, transformed, and enriched
3. **Data Storage**: Processed data is stored in a database
4. **Data Analysis**: Stored data is queried and visualized

### Why Use Real-Time Data Pipelines?

- **Immediate Insights**: Get insights as events happen
- **Better User Experience**: React to user behavior instantly
- **Fraud Detection**: Identify suspicious patterns immediately
- **System Monitoring**: Track application health in real-time

### Key Concepts

- **Stream Processing**: Processing data as it flows (real-time)
- **Batch Processing**: Processing data in chunks (periodic)
- **ETL (Extract, Transform, Load)**: Traditional data processing pattern
- **Event-driven Architecture**: System reacts to events as they occur

---

## Architecture Overview

Our ICO CRM Analytics Pipeline follows a modern **Lambda Architecture** pattern:

```
Data Sources → Kafka → Stream Processor → Database → Visualization
     ↓           ↓           ↓             ↓            ↓
ICO Websites  Message      Apache         PostgreSQL   Grafana
              Queue        Flink          Analytics    Dashboards
```

### Architecture Components

1. **Data Generator** (`data-generator/`): Simulates real ICO website traffic
2. **Apache Kafka** (`kafka`): Message streaming platform
3. **Schema Registry** (`schema-registry`): Manages data schemas
4. **Stream Consumer** (`kafka-consumer/`): Processes streaming data
5. **PostgreSQL** (`postgres/`): Analytical database
6. **Grafana** (`grafana/`): Data visualization platform
7. **Kafka UI** (`kafka-ui`): Kafka management interface

---

## Component Deep Dive

### 1. Apache Kafka - The Message Backbone

**What is Kafka?**
Apache Kafka is a distributed streaming platform that acts as a message queue. Think of it as a high-speed postal service for data.

**Key Concepts:**
- **Topics**: Categories of messages (like mailboxes)
- **Producers**: Applications that send messages
- **Consumers**: Applications that read messages
- **Partitions**: Sub-divisions of topics for scalability

**Our Kafka Topics:**
```yaml
Topics in our pipeline:
├── clickstream-events    # User clicks, page views, interactions
├── user-registrations   # New user sign-ups
└── conversions          # Purchases, token acquisitions
```

**Configuration Details:**
```yaml
# docker-compose.yml - Kafka Service
kafka:
  image: confluentinc/cp-kafka:7.4.0
  environment:
    KAFKA_BROKER_ID: 1                    # Unique broker identifier
    KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'  # Coordination service
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'    # Automatically create topics
```

### 2. Data Generator - Simulating Real Traffic

**Purpose**: Creates realistic ICO website analytics data for testing and demonstration.

**What it generates:**
- **Clickstream Events**: Page views, clicks, form interactions
- **User Registrations**: Email sign-ups, KYC completions
- **Conversion Events**: Token purchases, whitelist joins

**Key Files:**
```
data-generator/
├── simple_app.py          # Main data generation logic
├── Dockerfile             # Container configuration
└── requirements.txt       # Python dependencies
```

**Data Generation Logic:**
```python
# Example clickstream event structure
{
    "event_id": "uuid-123",
    "session_id": "session-456",
    "user_id": "user-789",
    "timestamp": "2024-08-10T12:00:00Z",
    "event_type": "page_view",
    "page_type": "landing",
    "ico_website_id": 1,
    "country": "United States",
    "device_type": "desktop"
}
```

### 3. Stream Consumer - Data Processing Engine

**Purpose**: Consumes messages from Kafka and stores them in PostgreSQL.

**Architecture Pattern**: Micro-batch processing
- Collects events in memory buffers
- Processes in batches of 100 events or every 10 seconds
- Ensures data consistency and performance

**Key Features:**
```python
# Batch processing configuration
batch_size = 100        # Process 100 events at once
batch_timeout = 10      # Or every 10 seconds
```

**Threading Model:**
```
Main Process
├── Clickstream Consumer Thread
├── Registration Consumer Thread
├── Conversion Consumer Thread
└── Periodic Flush Thread
```

### 4. PostgreSQL - Analytical Database

**Schema Design:**
```
Database: ico_analytics
├── raw_data schema          # Raw event storage
│   ├── clickstream_events   # All user interactions
│   ├── user_registrations   # Sign-up data
│   └── conversions          # Purchase events
├── processed_data schema    # Reference data
│   └── ico_websites         # ICO project information
└── analytics schema         # Aggregated views
    ├── current_active_sessions
    ├── hourly_metrics
    ├── metrics_5min
    ├── conversion_funnel
    ├── daily_registrations
    └── revenue_metrics
```

**Materialized Views Explained:**
Materialized views are pre-computed query results stored as tables. They provide fast access to complex aggregations.

**Example - Active Sessions View:**
```sql
CREATE MATERIALIZED VIEW analytics.current_active_sessions AS
SELECT 
    session_id,
    COUNT(*) as event_count,
    MAX(timestamp) as last_activity,
    ico_website_id
FROM raw_data.clickstream_events 
WHERE timestamp >= NOW() - INTERVAL '30 minutes'
GROUP BY session_id, ico_website_id;
```

### 5. Grafana - Data Visualization

**Dashboard Categories:**
1. **Real-time Metrics**: Current active users, live events
2. **User Analytics**: Registration trends, user behavior
3. **Conversion Metrics**: Sales funnels, revenue tracking
4. **Technical Metrics**: System performance, data quality

**Data Source Configuration:**
```yaml
# Connects to PostgreSQL as 'grafana_user'
Host: postgres:5432
Database: ico_analytics
Username: grafana_user
SSL Mode: disable
```

---

## Data Flow Explanation

### Step-by-Step Data Journey

1. **Data Generation** (Every few seconds)
   ```
   Data Generator → Creates realistic ICO events → Sends to Kafka topics
   ```

2. **Message Queuing** (Immediate)
   ```
   Kafka → Receives events → Stores in topic partitions → Waits for consumers
   ```

3. **Stream Processing** (Batch of 100 or every 10 seconds)
   ```
   Kafka Consumer → Reads events → Buffers in memory → Batch inserts to PostgreSQL
   ```

4. **Data Storage** (Immediate after batch)
   ```
   PostgreSQL → Stores raw events → Updates materialized views → Ready for queries
   ```

5. **Visualization** (Real-time updates)
   ```
   Grafana → Queries PostgreSQL → Renders dashboards → Updates every 5-30 seconds
   ```

### Data Volume Example

**Typical Processing Rates:**
- **500 events/minute** generated
- **~30,000 events/hour** processed
- **~720,000 events/day** stored
- **Active sessions**: 25,000-30,000 concurrent

---

## Configuration Details

### Docker Compose Configuration

**Network Setup:**
```yaml
networks:
  ico-network:
    driver: bridge    # Allows containers to communicate
```

**Volume Management:**
```yaml
volumes:
  postgres_data:     # Persistent database storage
  grafana_data:      # Persistent dashboard configurations
```

### Environment Variables

**Kafka Consumer Settings:**
```yaml
environment:
  KAFKA_BOOTSTRAP_SERVERS: kafka:29092      # Kafka connection
  POSTGRES_HOST: postgres                   # Database host
  POSTGRES_DB: ico_analytics               # Database name
  POSTGRES_USER: ico_user                  # Database user
  POSTGRES_PASSWORD: ico_password          # Database password
```

**Data Generator Settings:**
```yaml
environment:
  EVENTS_PER_MINUTE: 500                   # Event generation rate
  ICO_WEBSITES_COUNT: 20                   # Number of ICO projects
```

### Database Configuration

**Connection Settings:**
```yaml
environment:
  POSTGRES_DB: ico_analytics
  POSTGRES_USER: ico_user
  POSTGRES_PASSWORD: ico_password
```

**Schema Initialization:**
- Tables created automatically on first startup
- Indexes added for query performance
- Users and permissions configured

---

## Monitoring and Troubleshooting

### Health Check Commands

**Check Kafka Topics:**
```bash
docker exec ico-kafka kafka-topics --list --bootstrap-server localhost:9092
```

**Check PostgreSQL Data:**
```bash
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "SELECT COUNT(*) FROM raw_data.clickstream_events;"
```

**Check Consumer Logs:**
```bash
docker logs ico-kafka-consumer --tail 100 -f
```

### Key Metrics to Monitor

**Data Pipeline Health:**
1. **Event Generation Rate**: Should be ~500/minute
2. **Consumer Lag**: Events waiting to be processed
3. **Database Growth**: Storage usage over time
4. **Active Sessions**: Real-time user activity

**Performance Indicators:**
```sql
-- Check recent data processing
SELECT 
    DATE_TRUNC('minute', created_at) as minute,
    COUNT(*) as events_processed
FROM raw_data.clickstream_events 
WHERE created_at >= NOW() - INTERVAL '10 minutes'
GROUP BY minute
ORDER BY minute DESC;
```

### Common Issues and Solutions

**Issue 1: No data in Grafana**
```bash
# Check if data is flowing to PostgreSQL
docker exec ico-postgres psql -U ico_user -d ico_analytics -c "SELECT COUNT(*) FROM raw_data.clickstream_events;"

# Check consumer logs
docker logs ico-kafka-consumer --tail 50
```

**Issue 2: Kafka connection errors**
```bash
# Restart Kafka consumer
docker restart ico-kafka-consumer

# Check Kafka health
docker exec ico-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

**Issue 3: Database connection issues**
```bash
# Check PostgreSQL status
docker exec ico-postgres pg_isready -U ico_user

# Restart database
docker restart ico-postgres
```

---

## Scaling and Production Considerations

### Performance Optimization

**Kafka Scaling:**
```yaml
# Increase partitions for better parallelism
KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_NUM_PARTITIONS: 6
```

**Database Optimization:**
```sql
-- Add more indexes for common queries
CREATE INDEX idx_events_timestamp_ico ON raw_data.clickstream_events (timestamp, ico_website_id);

-- Partition large tables by date
CREATE TABLE raw_data.clickstream_events_2024_08 PARTITION OF raw_data.clickstream_events
FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
```

**Consumer Scaling:**
```yaml
# Run multiple consumer instances
kafka-consumer:
  deploy:
    replicas: 3    # 3 parallel consumers
```

### Production Deployment Changes

**Security:**
```yaml
# Enable authentication
KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN

# PostgreSQL SSL
POSTGRES_SSL_MODE: require
```

**Monitoring:**
```yaml
# Add Prometheus metrics
KAFKA_JMX_PORT: 9999
POSTGRES_EXPORTER_ENABLED: true
```

**Data Retention:**
```sql
-- Implement data retention policies
DELETE FROM raw_data.clickstream_events 
WHERE timestamp < NOW() - INTERVAL '90 days';
```

---

## Getting Started Checklist

1. **Prerequisites Installed:**
   - [ ] Docker & Docker Compose
   - [ ] 8GB+ RAM available
   - [ ] Ports 3000, 5432, 8080, 9092 available

2. **Initial Setup:**
   - [ ] Clone/create project directory
   - [ ] Run `docker-compose up -d`
   - [ ] Wait 2-3 minutes for initialization

3. **Verification:**
   - [ ] Grafana accessible at http://localhost:3000 (admin/admin)
   - [ ] Kafka UI at http://localhost:8080
   - [ ] PostgreSQL contains data
   - [ ] No errors in container logs

4. **First Analysis:**
   - [ ] View real-time dashboards in Grafana
   - [ ] Explore Kafka topics in Kafka UI
   - [ ] Run sample PostgreSQL queries
   - [ ] Understand data flow patterns

---

This documentation provides a comprehensive foundation for understanding and operating the ICO CRM Analytics Pipeline. Start with the architecture overview, then dive deeper into specific components as needed.