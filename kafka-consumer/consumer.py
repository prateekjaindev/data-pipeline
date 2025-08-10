#!/usr/bin/env python3
"""
Kafka to PostgreSQL Consumer
Consumes events from Kafka topics and stores them in PostgreSQL
"""

import json
import time
import logging
import os
from datetime import datetime
from typing import Dict, Any, List
import threading

from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaToPostgresConsumer:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')],
            'group_id': 'postgres-consumer-group',
            'auto_offset_reset': 'latest',  # Start from latest messages
            'enable_auto_commit': True,
            'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        }
        
        self.postgres_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'ico_analytics'),
            'user': os.getenv('POSTGRES_USER', 'ico_user'),
            'password': os.getenv('POSTGRES_PASSWORD', 'ico_password')
        }
        
        # Batch processing settings
        self.batch_size = 100
        self.batch_timeout = 10  # seconds
        
        # Event buffers for batching
        self.clickstream_buffer = []
        self.registration_buffer = []
        self.conversion_buffer = []
        
        # Last flush times
        self.last_flush = {
            'clickstream': time.time(),
            'registration': time.time(),
            'conversion': time.time()
        }

    def get_postgres_connection(self):
        """Get PostgreSQL connection with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**self.postgres_config)
                conn.autocommit = True
                return conn
            except psycopg2.Error as e:
                logger.error(f"PostgreSQL connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    raise

    def insert_clickstream_events(self, events: List[Dict[str, Any]]):
        """Insert clickstream events into PostgreSQL"""
        if not events:
            return
            
        conn = None
        try:
            conn = self.get_postgres_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert
            insert_query = """
                INSERT INTO raw_data.clickstream_events (
                    event_id, session_id, user_id, timestamp, event_type, page_type,
                    page_url, referrer, user_agent, ip_address, country, city,
                    device_type, browser, session_duration, page_load_time,
                    ico_website_id, ico_website_name, button_clicked, form_filled,
                    scroll_depth, bounce
                ) VALUES %s
                ON CONFLICT (event_id) DO NOTHING
            """
            
            # Prepare data tuples
            data_tuples = []
            for event in events:
                data_tuples.append((
                    event.get('event_id'),
                    event.get('session_id'),
                    event.get('user_id'),
                    event.get('timestamp'),
                    event.get('event_type'),
                    event.get('page_type'),
                    event.get('page_url'),
                    event.get('referrer'),
                    event.get('user_agent'),
                    event.get('ip_address'),
                    event.get('country'),
                    event.get('city'),
                    event.get('device_type'),
                    event.get('browser'),
                    event.get('session_duration'),
                    event.get('page_load_time'),
                    event.get('ico_website_id'),
                    event.get('ico_website_name'),
                    event.get('button_clicked'),
                    event.get('form_filled'),
                    event.get('scroll_depth'),
                    event.get('bounce', False)
                ))
            
            # Execute batch insert
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, data_tuples)
            
            logger.info(f"Inserted {len(events)} clickstream events")
            
        except Exception as e:
            logger.error(f"Error inserting clickstream events: {e}")
        finally:
            if conn:
                conn.close()

    def insert_user_registrations(self, events: List[Dict[str, Any]]):
        """Insert user registration events into PostgreSQL"""
        if not events:
            return
            
        conn = None
        try:
            conn = self.get_postgres_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert
            insert_query = """
                INSERT INTO raw_data.user_registrations (
                    registration_id, user_id, timestamp, email, wallet_address,
                    social_handles, consent_marketing, consent_data, country,
                    ip_address, ico_website_id, referral_source, kyc_completed,
                    verification_tier
                ) VALUES %s
                ON CONFLICT (user_id) DO NOTHING
            """
            
            # Prepare data tuples
            data_tuples = []
            for event in events:
                data_tuples.append((
                    event.get('registration_id'),
                    event.get('user_id'),
                    event.get('timestamp'),
                    event.get('email'),
                    event.get('wallet_address'),
                    json.dumps(event.get('social_handles', {})),
                    event.get('consent_marketing', False),
                    event.get('consent_data', True),
                    event.get('country'),
                    event.get('ip_address'),
                    event.get('ico_website_id'),
                    event.get('referral_source'),
                    event.get('kyc_completed', False),
                    event.get('verification_tier', 'basic')
                ))
            
            # Execute batch insert
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, data_tuples)
            
            logger.info(f"Inserted {len(events)} user registrations")
            
        except Exception as e:
            logger.error(f"Error inserting user registrations: {e}")
        finally:
            if conn:
                conn.close()

    def insert_conversions(self, events: List[Dict[str, Any]]):
        """Insert conversion events into PostgreSQL"""
        if not events:
            return
            
        conn = None
        try:
            conn = self.get_postgres_connection()
            cursor = conn.cursor()
            
            # Prepare batch insert
            insert_query = """
                INSERT INTO raw_data.conversions (
                    conversion_id, user_id, session_id, timestamp, conversion_type,
                    ico_website_id, amount_usd, token_amount, payment_method,
                    funnel_stage, time_to_convert, previous_visits
                ) VALUES %s
                ON CONFLICT (conversion_id) DO NOTHING
            """
            
            # Prepare data tuples
            data_tuples = []
            for event in events:
                data_tuples.append((
                    event.get('conversion_id'),
                    event.get('user_id'),
                    event.get('session_id'),
                    event.get('timestamp'),
                    event.get('conversion_type'),
                    event.get('ico_website_id'),
                    event.get('amount_usd'),
                    event.get('token_amount'),
                    event.get('payment_method'),
                    event.get('funnel_stage'),
                    event.get('time_to_convert'),
                    event.get('previous_visits')
                ))
            
            # Execute batch insert
            from psycopg2.extras import execute_values
            execute_values(cursor, insert_query, data_tuples)
            
            logger.info(f"Inserted {len(events)} conversions")
            
        except Exception as e:
            logger.error(f"Error inserting conversions: {e}")
        finally:
            if conn:
                conn.close()

    def flush_buffers(self, force=False):
        """Flush all buffers to PostgreSQL"""
        current_time = time.time()
        
        # Flush clickstream events
        if (len(self.clickstream_buffer) >= self.batch_size or 
            current_time - self.last_flush['clickstream'] > self.batch_timeout or 
            force):
            if self.clickstream_buffer:
                self.insert_clickstream_events(self.clickstream_buffer.copy())
                self.clickstream_buffer.clear()
                self.last_flush['clickstream'] = current_time
        
        # Flush user registrations
        if (len(self.registration_buffer) >= self.batch_size or 
            current_time - self.last_flush['registration'] > self.batch_timeout or 
            force):
            if self.registration_buffer:
                self.insert_user_registrations(self.registration_buffer.copy())
                self.registration_buffer.clear()
                self.last_flush['registration'] = current_time
        
        # Flush conversions
        if (len(self.conversion_buffer) >= self.batch_size or 
            current_time - self.last_flush['conversion'] > self.batch_timeout or 
            force):
            if self.conversion_buffer:
                self.insert_conversions(self.conversion_buffer.copy())
                self.conversion_buffer.clear()
                self.last_flush['conversion'] = current_time

    def consume_clickstream_events(self):
        """Consume clickstream events from Kafka"""
        consumer = KafkaConsumer('clickstream-events', **self.kafka_config)
        logger.info("Started consuming clickstream events")
        
        try:
            for message in consumer:
                event = message.value
                self.clickstream_buffer.append(event)
                
                # Flush if buffer is full
                if len(self.clickstream_buffer) >= self.batch_size:
                    self.flush_buffers()
                    
        except KeyboardInterrupt:
            logger.info("Shutting down clickstream consumer")
        except Exception as e:
            logger.error(f"Error in clickstream consumer: {e}")
        finally:
            consumer.close()

    def consume_user_registrations(self):
        """Consume user registration events from Kafka"""
        consumer = KafkaConsumer('user-registrations', **self.kafka_config)
        logger.info("Started consuming user registrations")
        
        try:
            for message in consumer:
                event = message.value
                self.registration_buffer.append(event)
                
                # Flush if buffer is full
                if len(self.registration_buffer) >= self.batch_size:
                    self.flush_buffers()
                    
        except KeyboardInterrupt:
            logger.info("Shutting down registration consumer")
        except Exception as e:
            logger.error(f"Error in registration consumer: {e}")
        finally:
            consumer.close()

    def consume_conversions(self):
        """Consume conversion events from Kafka"""
        consumer = KafkaConsumer('conversions', **self.kafka_config)
        logger.info("Started consuming conversions")
        
        try:
            for message in consumer:
                event = message.value
                self.conversion_buffer.append(event)
                
                # Flush if buffer is full
                if len(self.conversion_buffer) >= self.batch_size:
                    self.flush_buffers()
                    
        except KeyboardInterrupt:
            logger.info("Shutting down conversion consumer")
        except Exception as e:
            logger.error(f"Error in conversion consumer: {e}")
        finally:
            consumer.close()

    def periodic_flush(self):
        """Periodically flush buffers"""
        while True:
            try:
                time.sleep(5)  # Check every 5 seconds
                self.flush_buffers()
            except KeyboardInterrupt:
                logger.info("Shutting down periodic flush")
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")

    def run(self):
        """Run all consumers in parallel"""
        logger.info("Starting Kafka to PostgreSQL consumer")
        
        # Wait for Kafka to be ready
        time.sleep(30)
        
        # Start consumer threads
        threads = [
            threading.Thread(target=self.consume_clickstream_events, daemon=True),
            threading.Thread(target=self.consume_user_registrations, daemon=True), 
            threading.Thread(target=self.consume_conversions, daemon=True),
            threading.Thread(target=self.periodic_flush, daemon=True)
        ]
        
        for thread in threads:
            thread.start()
        
        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer")
            # Final flush
            self.flush_buffers(force=True)

if __name__ == "__main__":
    consumer = KafkaToPostgresConsumer()
    consumer.run()