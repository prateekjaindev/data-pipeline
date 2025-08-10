"""
ICO Analytics Flink Processor - Python Implementation
Real-time stream processing for ICO website analytics
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, ProcessFunction, WindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickstreamEventProcessor(MapFunction):
    """Process clickstream events from Kafka"""
    
    def map(self, value: str) -> Dict[str, Any]:
        try:
            event = json.loads(value)
            # Add processing timestamp
            event['processed_at'] = datetime.now().isoformat()
            
            # Data quality checks
            if not event.get('event_id') or not event.get('session_id'):
                logger.warning(f"Invalid event missing required fields: {event}")
                return None
            
            # Enrich with additional metadata
            event['is_bot'] = self._detect_bot(event.get('user_agent', ''))
            event['session_length_category'] = self._categorize_session_length(
                event.get('session_duration', 0)
            )
            
            return event
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse clickstream event: {value}, error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing clickstream event: {e}")
            return None
    
    def _detect_bot(self, user_agent: str) -> bool:
        """Simple bot detection based on user agent"""
        bot_indicators = ['bot', 'spider', 'crawler', 'scraper']
        return any(indicator in user_agent.lower() for indicator in bot_indicators)
    
    def _categorize_session_length(self, duration: int) -> str:
        """Categorize session length"""
        if duration < 30:
            return 'quick_visit'
        elif duration < 300:  # 5 minutes
            return 'short_session'
        elif duration < 1800:  # 30 minutes
            return 'medium_session'
        else:
            return 'long_session'

class UserRegistrationProcessor(MapFunction):
    """Process user registration events from Kafka"""
    
    def map(self, value: str) -> Dict[str, Any]:
        try:
            registration = json.loads(value)
            registration['processed_at'] = datetime.now().isoformat()
            
            # Data quality checks
            if not registration.get('user_id') or not registration.get('email'):
                logger.warning(f"Invalid registration missing required fields: {registration}")
                return None
            
            # Email domain analysis
            email = registration.get('email', '')
            domain = email.split('@')[-1] if '@' in email else ''
            registration['email_domain'] = domain
            registration['is_corporate_email'] = self._is_corporate_email(domain)
            
            return registration
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse registration event: {value}, error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing registration event: {e}")
            return None
    
    def _is_corporate_email(self, domain: str) -> bool:
        """Check if email domain indicates corporate user"""
        personal_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
        return domain.lower() not in personal_domains

class ConversionProcessor(MapFunction):
    """Process conversion events from Kafka"""
    
    def map(self, value: str) -> Dict[str, Any]:
        try:
            conversion = json.loads(value)
            conversion['processed_at'] = datetime.now().isoformat()
            
            # Calculate conversion value category
            amount_usd = conversion.get('amount_usd', 0)
            conversion['conversion_category'] = self._categorize_conversion_amount(amount_usd)
            
            # Risk scoring for high-value transactions
            if amount_usd > 10000:
                conversion['requires_review'] = True
            
            return conversion
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse conversion event: {value}, error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing conversion event: {e}")
            return None
    
    def _categorize_conversion_amount(self, amount: float) -> str:
        """Categorize conversion amount"""
        if amount < 100:
            return 'micro'
        elif amount < 1000:
            return 'small'
        elif amount < 10000:
            return 'medium'
        else:
            return 'large'

class AnomalyDetector(ProcessFunction):
    """Detect anomalies in real-time data"""
    
    def __init__(self):
        self.event_counts = {}
        self.time_window = 300  # 5 minutes
        
    def process_element(self, value: Dict[str, Any], ctx, out):
        current_time = ctx.timestamp()
        ico_id = value.get('ico_website_id')
        
        if ico_id not in self.event_counts:
            self.event_counts[ico_id] = []
        
        # Add current event
        self.event_counts[ico_id].append(current_time)
        
        # Clean old events (outside time window)
        cutoff_time = current_time - (self.time_window * 1000)  # Convert to milliseconds
        self.event_counts[ico_id] = [
            ts for ts in self.event_counts[ico_id] if ts > cutoff_time
        ]
        
        # Check for anomalies
        event_count = len(self.event_counts[ico_id])
        if event_count > 1000:  # More than 1000 events in 5 minutes
            anomaly = {
                'timestamp': datetime.now().isoformat(),
                'type': 'high_traffic_spike',
                'ico_website_id': ico_id,
                'event_count': event_count,
                'window_minutes': self.time_window / 60,
                'severity': 'high' if event_count > 2000 else 'medium'
            }
            out.collect(anomaly)

class MetricsAggregator(WindowFunction):
    """Aggregate metrics for time windows"""
    
    def apply(self, key, window, inputs, out):
        events = list(inputs)
        
        # Basic aggregations
        total_events = len(events)
        unique_sessions = len(set(e.get('session_id') for e in events if e.get('session_id')))
        unique_users = len(set(e.get('user_id') for e in events if e.get('user_id')))
        
        # Page views
        page_views = sum(1 for e in events if e.get('event_type') == 'page_view')
        
        # Device type distribution
        device_types = {}
        for event in events:
            device = event.get('device_type', 'unknown')
            device_types[device] = device_types.get(device, 0) + 1
        
        # Country distribution
        countries = {}
        for event in events:
            country = event.get('country', 'unknown')
            countries[country] = countries.get(country, 0) + 1
        
        # Average page load time
        load_times = [e.get('page_load_time', 0) for e in events if e.get('page_load_time')]
        avg_load_time = sum(load_times) / len(load_times) if load_times else 0
        
        # Bounce rate (sessions with only one page view)
        session_page_counts = {}
        for event in events:
            if event.get('event_type') == 'page_view':
                session_id = event.get('session_id')
                if session_id:
                    session_page_counts[session_id] = session_page_counts.get(session_id, 0) + 1
        
        bounce_sessions = sum(1 for count in session_page_counts.values() if count == 1)
        bounce_rate = bounce_sessions / len(session_page_counts) if session_page_counts else 0
        
        # Create aggregated metrics
        metrics = {
            'window_start': datetime.fromtimestamp(window.start / 1000).isoformat(),
            'window_end': datetime.fromtimestamp(window.end / 1000).isoformat(),
            'ico_website_id': key,
            'total_events': total_events,
            'unique_sessions': unique_sessions,
            'unique_users': unique_users,
            'page_views': page_views,
            'avg_page_load_time': round(avg_load_time, 2),
            'bounce_rate': round(bounce_rate * 100, 2),
            'device_type_distribution': device_types,
            'country_distribution': countries,
            'top_countries': sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5],
            'processed_at': datetime.now().isoformat()
        }
        
        out.collect(metrics)

def create_kafka_consumer(env, topic: str, group_id: str = "flink-analytics"):
    """Create Kafka consumer for given topic"""
    properties = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': group_id,
        'auto.offset.reset': 'latest'
    }
    
    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )

def create_kafka_producer(topic: str):
    """Create Kafka producer for given topic"""
    properties = {
        'bootstrap.servers': 'kafka:29092',
    }
    
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config=properties
    )

def setup_postgres_sink(env, table_name: str):
    """Setup PostgreSQL sink"""
    # This would typically use Flink's JDBC connector
    # For simplicity, we'll use Kafka as an intermediate step
    # and have a separate service consume from Kafka and write to Postgres
    pass

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60000)  # Checkpoint every minute
    
    # Create table environment for SQL operations
    t_env = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.new_instance()
                                           .in_streaming_mode()
                                           .build()
    )
    
    # Add JAR files for connectors
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")
    env.add_jars("file:///opt/flink/lib/flink-connector-jdbc-3.1.0-1.17.jar")
    env.add_jars("file:///opt/flink/lib/postgresql-42.6.0.jar")
    
    # Create Kafka consumers
    clickstream_source = create_kafka_consumer(env, "clickstream-events")
    registration_source = create_kafka_consumer(env, "user-registrations")
    conversion_source = create_kafka_consumer(env, "conversions")
    
    # Process clickstream events
    clickstream_stream = env.add_source(clickstream_source) \
                            .map(ClickstreamEventProcessor(), 
                                 output_type=Types.PICKLED_BYTE_ARRAY()) \
                            .filter(lambda x: x is not None)
    
    # Process user registrations
    registration_stream = env.add_source(registration_source) \
                            .map(UserRegistrationProcessor(), 
                                 output_type=Types.PICKLED_BYTE_ARRAY()) \
                            .filter(lambda x: x is not None)
    
    # Process conversions
    conversion_stream = env.add_source(conversion_source) \
                          .map(ConversionProcessor(), 
                               output_type=Types.PICKLED_BYTE_ARRAY()) \
                          .filter(lambda x: x is not None)
    
    # Real-time metrics: 5-minute windows
    metrics_5min = clickstream_stream \
        .key_by(lambda event: event.get('ico_website_id', 0)) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .apply(MetricsAggregator(), 
               output_type=Types.PICKLED_BYTE_ARRAY())
    
    # Real-time metrics: 15-minute windows
    metrics_15min = clickstream_stream \
        .key_by(lambda event: event.get('ico_website_id', 0)) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(15))) \
        .apply(MetricsAggregator(), 
               output_type=Types.PICKLED_BYTE_ARRAY())
    
    # Real-time metrics: 1-hour windows
    metrics_hourly = clickstream_stream \
        .key_by(lambda event: event.get('ico_website_id', 0)) \
        .window(TumblingProcessingTimeWindows.of(Time.hours(1))) \
        .apply(MetricsAggregator(), 
               output_type=Types.PICKLED_BYTE_ARRAY())
    
    # Anomaly detection
    anomalies = clickstream_stream \
        .key_by(lambda event: event.get('ico_website_id', 0)) \
        .process(AnomalyDetector(), 
                 output_type=Types.PICKLED_BYTE_ARRAY())
    
    # Output streams to Kafka topics for further processing
    metrics_5min.map(lambda x: json.dumps(x, default=str), 
                     output_type=Types.STRING()) \
               .add_sink(create_kafka_producer("metrics-5min"))
    
    metrics_15min.map(lambda x: json.dumps(x, default=str), 
                      output_type=Types.STRING()) \
                .add_sink(create_kafka_producer("metrics-15min"))
    
    metrics_hourly.map(lambda x: json.dumps(x, default=str), 
                       output_type=Types.STRING()) \
                 .add_sink(create_kafka_producer("metrics-hourly"))
    
    anomalies.map(lambda x: json.dumps(x, default=str), 
                  output_type=Types.STRING()) \
            .add_sink(create_kafka_producer("anomalies"))
    
    # Processed events back to Kafka for storage
    clickstream_stream.map(lambda x: json.dumps(x, default=str), 
                          output_type=Types.STRING()) \
                     .add_sink(create_kafka_producer("processed-clickstream"))
    
    registration_stream.map(lambda x: json.dumps(x, default=str), 
                           output_type=Types.STRING()) \
                      .add_sink(create_kafka_producer("processed-registrations"))
    
    conversion_stream.map(lambda x: json.dumps(x, default=str), 
                         output_type=Types.STRING()) \
                    .add_sink(create_kafka_producer("processed-conversions"))
    
    # Execute the job
    try:
        env.execute("ICO Analytics Stream Processing")
    except Exception as e:
        logger.error(f"Job execution failed: {e}")
        raise

if __name__ == "__main__":
    main()