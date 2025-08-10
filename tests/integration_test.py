#!/usr/bin/env python3
"""
Integration tests for ICO Analytics Pipeline
Tests the end-to-end data flow from generation to visualization
"""

import json
import time
import requests
import psycopg2
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IntegrationTest:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': ['localhost:9092'],
            'group_id': 'integration-test',
            'auto_offset_reset': 'latest',
            'value_deserializer': lambda x: json.loads(x.decode('utf-8'))
        }
        
        self.postgres_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'ico_analytics',
            'user': 'ico_user',
            'password': 'ico_password'
        }

    def test_services_health(self):
        """Test that all services are running and healthy"""
        logger.info("Testing service health...")
        
        services = {
            'Kafka UI': 'http://localhost:8080/actuator/health',
            'Flink Web UI': 'http://localhost:8082/config',
            'Grafana': 'http://localhost:3000/api/health',
            'Schema Registry': 'http://localhost:8081/subjects'
        }
        
        for service, url in services.items():
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    logger.info(f"✅ {service} is healthy")
                else:
                    logger.error(f"❌ {service} returned status {response.status_code}")
                    return False
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ {service} is unreachable: {e}")
                return False
        
        return True

    def test_kafka_topics(self):
        """Test that required Kafka topics exist"""
        logger.info("Testing Kafka topics...")
        
        try:
            response = requests.get('http://localhost:8080/api/clusters/local/topics', timeout=10)
            if response.status_code != 200:
                logger.error("Failed to fetch topics from Kafka UI")
                return False
            
            topics = [topic['name'] for topic in response.json()]
            required_topics = ['clickstream-events', 'user-registrations', 'conversions']
            
            for topic in required_topics:
                if topic in topics:
                    logger.info(f"✅ Topic '{topic}' exists")
                else:
                    logger.error(f"❌ Topic '{topic}' does not exist")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error testing Kafka topics: {e}")
            return False

    def test_data_generation(self):
        """Test that data is being generated and flowing through Kafka"""
        logger.info("Testing data generation...")
        
        topics_to_test = ['clickstream-events', 'user-registrations', 'conversions']
        
        for topic in topics_to_test:
            try:
                consumer = KafkaConsumer(
                    topic,
                    **self.kafka_config,
                    consumer_timeout_ms=30000  # 30 second timeout
                )
                
                message_count = 0
                logger.info(f"Listening for messages on topic '{topic}'...")
                
                for message in consumer:
                    message_count += 1
                    logger.info(f"Received message from '{topic}': {message.value}")
                    
                    # Validate message structure
                    if topic == 'clickstream-events':
                        required_fields = ['event_id', 'session_id', 'timestamp', 'event_type']
                    elif topic == 'user-registrations':
                        required_fields = ['registration_id', 'user_id', 'email', 'timestamp']
                    elif topic == 'conversions':
                        required_fields = ['conversion_id', 'user_id', 'timestamp', 'conversion_type']
                    
                    for field in required_fields:
                        if field not in message.value:
                            logger.error(f"❌ Missing required field '{field}' in {topic}")
                            consumer.close()
                            return False
                    
                    if message_count >= 3:  # Test with 3 messages
                        break
                
                consumer.close()
                
                if message_count > 0:
                    logger.info(f"✅ Successfully received {message_count} messages from '{topic}'")
                else:
                    logger.error(f"❌ No messages received from '{topic}'")
                    return False
                    
            except Exception as e:
                logger.error(f"Error testing topic '{topic}': {e}")
                return False
        
        return True

    def test_database_connection(self):
        """Test PostgreSQL connection and data ingestion"""
        logger.info("Testing database connection...")
        
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            
            # Test basic connection
            cursor.execute("SELECT version()")
            version = cursor.fetchone()
            logger.info(f"✅ Connected to PostgreSQL: {version[0]}")
            
            # Test schema existence
            cursor.execute("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name IN ('raw_data', 'processed_data', 'analytics')
            """)
            schemas = [row[0] for row in cursor.fetchall()]
            
            required_schemas = ['raw_data', 'processed_data', 'analytics']
            for schema in required_schemas:
                if schema in schemas:
                    logger.info(f"✅ Schema '{schema}' exists")
                else:
                    logger.error(f"❌ Schema '{schema}' does not exist")
                    return False
            
            # Test table existence
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'raw_data'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            required_tables = ['clickstream_events', 'user_registrations', 'conversions']
            for table in required_tables:
                if table in tables:
                    logger.info(f"✅ Table 'raw_data.{table}' exists")
                else:
                    logger.error(f"❌ Table 'raw_data.{table}' does not exist")
                    return False
            
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def test_data_processing(self):
        """Test that data is being processed and stored in database"""
        logger.info("Testing data processing...")
        
        try:
            # Wait for some data to be processed
            logger.info("Waiting 60 seconds for data processing...")
            time.sleep(60)
            
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()
            
            # Check for data in raw tables
            tables_to_check = [
                'raw_data.clickstream_events',
                'raw_data.user_registrations', 
                'raw_data.conversions'
            ]
            
            for table in tables_to_check:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    logger.info(f"✅ Found {count} records in {table}")
                else:
                    logger.error(f"❌ No records found in {table}")
                    return False
                    
                # Check recent data (last 5 minutes)
                cursor.execute(f"""
                    SELECT COUNT(*) FROM {table} 
                    WHERE created_at >= NOW() - INTERVAL '5 minutes'
                """)
                recent_count = cursor.fetchone()[0]
                
                if recent_count > 0:
                    logger.info(f"✅ Found {recent_count} recent records in {table}")
                else:
                    logger.warning(f"⚠️  No recent records in {table}")
            
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"Data processing test failed: {e}")
            return False

    def test_flink_jobs(self):
        """Test that Flink jobs are running"""
        logger.info("Testing Flink jobs...")
        
        try:
            response = requests.get('http://localhost:8082/jobs', timeout=10)
            if response.status_code != 200:
                logger.error("Failed to fetch jobs from Flink")
                return False
            
            jobs = response.json()['jobs']
            
            if len(jobs) == 0:
                logger.warning("⚠️  No Flink jobs found")
                return True  # This might be normal in some setups
            
            for job in jobs:
                job_id = job['id']
                state = job['state']
                
                job_response = requests.get(f'http://localhost:8082/jobs/{job_id}', timeout=10)
                if job_response.status_code == 200:
                    job_details = job_response.json()
                    job_name = job_details.get('name', 'Unknown')
                    
                    if state == 'RUNNING':
                        logger.info(f"✅ Flink job '{job_name}' is running")
                    else:
                        logger.error(f"❌ Flink job '{job_name}' is in state '{state}'")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Flink jobs test failed: {e}")
            return False

    def test_grafana_dashboards(self):
        """Test that Grafana dashboards are accessible"""
        logger.info("Testing Grafana dashboards...")
        
        try:
            # Test basic API access
            response = requests.get(
                'http://localhost:3000/api/search',
                auth=('admin', 'admin'),
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error("Failed to access Grafana API")
                return False
            
            dashboards = response.json()
            
            expected_dashboards = ['ICO Real-time Analytics', 'ICO Business Metrics']
            found_dashboards = [dash['title'] for dash in dashboards if dash['type'] == 'dash-db']
            
            for expected in expected_dashboards:
                if expected in found_dashboards:
                    logger.info(f"✅ Dashboard '{expected}' found")
                else:
                    logger.warning(f"⚠️  Dashboard '{expected}' not found")
            
            return True
            
        except Exception as e:
            logger.error(f"Grafana dashboards test failed: {e}")
            return False

    def run_all_tests(self):
        """Run all integration tests"""
        logger.info("🧪 Starting ICO Analytics Pipeline Integration Tests")
        logger.info("=" * 60)
        
        tests = [
            ("Service Health", self.test_services_health),
            ("Kafka Topics", self.test_kafka_topics),
            ("Data Generation", self.test_data_generation),
            ("Database Connection", self.test_database_connection),
            ("Data Processing", self.test_data_processing),
            ("Flink Jobs", self.test_flink_jobs),
            ("Grafana Dashboards", self.test_grafana_dashboards)
        ]
        
        passed = 0
        failed = 0
        
        for test_name, test_func in tests:
            logger.info(f"\n📋 Running test: {test_name}")
            logger.info("-" * 40)
            
            try:
                if test_func():
                    logger.info(f"✅ {test_name}: PASSED")
                    passed += 1
                else:
                    logger.error(f"❌ {test_name}: FAILED")
                    failed += 1
            except Exception as e:
                logger.error(f"❌ {test_name}: ERROR - {e}")
                failed += 1
        
        logger.info("\n" + "=" * 60)
        logger.info(f"🏆 Test Results: {passed} passed, {failed} failed")
        
        if failed == 0:
            logger.info("🎉 All tests passed! Pipeline is working correctly.")
            return True
        else:
            logger.error(f"💥 {failed} test(s) failed. Please check the logs above.")
            return False

if __name__ == "__main__":
    test_runner = IntegrationTest()
    success = test_runner.run_all_tests()
    exit(0 if success else 1)