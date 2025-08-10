import json
import time
import random
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import uuid
import hashlib

from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import numpy as np

from config.ico_websites import ICO_WEBSITES, PAGE_TYPES, USER_AGENTS, CRYPTO_FRIENDLY_COUNTRIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

@dataclass
class ClickstreamEvent:
    event_id: str
    session_id: str
    user_id: Optional[str]
    timestamp: str
    event_type: str
    page_type: str
    page_url: str
    referrer: Optional[str]
    user_agent: str
    ip_address: str
    country: str
    city: str
    device_type: str
    browser: str
    session_duration: Optional[int]
    page_load_time: float
    ico_website_id: int
    ico_website_name: str
    button_clicked: Optional[str]
    form_filled: Optional[str]
    scroll_depth: Optional[float]
    bounce: bool

@dataclass
class UserRegistration:
    registration_id: str
    user_id: str
    timestamp: str
    email: str
    wallet_address: Optional[str]
    social_handles: Dict[str, str]
    consent_marketing: bool
    consent_data: bool
    country: str
    ip_address: str
    ico_website_id: int
    referral_source: Optional[str]
    kyc_completed: bool
    verification_tier: str

@dataclass
class ConversionEvent:
    conversion_id: str
    user_id: str
    session_id: str
    timestamp: str
    conversion_type: str
    ico_website_id: int
    amount_usd: Optional[float]
    token_amount: Optional[float]
    payment_method: Optional[str]
    funnel_stage: str
    time_to_convert: int
    previous_visits: int

class ICODataGenerator:
    def __init__(self):
        self.kafka_config = {
            'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092'),
            'client.id': 'ico-data-generator'
        }
        
        self.schema_registry_client = SchemaRegistryClient({
            'url': os.getenv('SCHEMA_REGISTRY_URL', 'http://schema-registry:8081')
        })
        
        self.producer = Producer(self.kafka_config)
        
        self.events_per_minute = int(os.getenv('EVENTS_PER_MINUTE', '500'))
        self.ico_websites_count = int(os.getenv('ICO_WEBSITES_COUNT', '20'))
        self.data_quality_issues = os.getenv('DATA_QUALITY_ISSUES', 'true').lower() == 'true'
        
        # Session tracking
        self.active_sessions: Dict[str, Dict] = {}
        self.user_registry: Dict[str, Dict] = {}
        
        # Load schemas and create serializers
        self.setup_serializers()
        
    def setup_serializers(self):
        # Register schemas
        clickstream_schema = """
        {
            "type": "record",
            "name": "ClickstreamEvent",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "session_id", "type": "string"},
                {"name": "user_id", "type": ["null", "string"]},
                {"name": "timestamp", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "page_type", "type": "string"},
                {"name": "page_url", "type": "string"},
                {"name": "referrer", "type": ["null", "string"]},
                {"name": "user_agent", "type": "string"},
                {"name": "ip_address", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "device_type", "type": "string"},
                {"name": "browser", "type": "string"},
                {"name": "session_duration", "type": ["null", "int"]},
                {"name": "page_load_time", "type": "float"},
                {"name": "ico_website_id", "type": "int"},
                {"name": "ico_website_name", "type": "string"},
                {"name": "button_clicked", "type": ["null", "string"]},
                {"name": "form_filled", "type": ["null", "string"]},
                {"name": "scroll_depth", "type": ["null", "float"]},
                {"name": "bounce", "type": "boolean"}
            ]
        }
        """
        
        user_registration_schema = """
        {
            "type": "record",
            "name": "UserRegistration",
            "fields": [
                {"name": "registration_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "wallet_address", "type": ["null", "string"]},
                {"name": "social_handles", "type": {"type": "map", "values": "string"}},
                {"name": "consent_marketing", "type": "boolean"},
                {"name": "consent_data", "type": "boolean"},
                {"name": "country", "type": "string"},
                {"name": "ip_address", "type": "string"},
                {"name": "ico_website_id", "type": "int"},
                {"name": "referral_source", "type": ["null", "string"]},
                {"name": "kyc_completed", "type": "boolean"},
                {"name": "verification_tier", "type": "string"}
            ]
        }
        """
        
        conversion_schema = """
        {
            "type": "record",
            "name": "ConversionEvent",
            "fields": [
                {"name": "conversion_id", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "session_id", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "conversion_type", "type": "string"},
                {"name": "ico_website_id", "type": "int"},
                {"name": "amount_usd", "type": ["null", "float"]},
                {"name": "token_amount", "type": ["null", "float"]},
                {"name": "payment_method", "type": ["null", "string"]},
                {"name": "funnel_stage", "type": "string"},
                {"name": "time_to_convert", "type": "int"},
                {"name": "previous_visits", "type": "int"}
            ]
        }
        """
        
        self.clickstream_serializer = AvroSerializer(
            self.schema_registry_client,
            clickstream_schema,
            lambda obj, ctx: asdict(obj)
        )
        
        self.registration_serializer = AvroSerializer(
            self.schema_registry_client,
            user_registration_schema,
            lambda obj, ctx: asdict(obj)
        )
        
        self.conversion_serializer = AvroSerializer(
            self.schema_registry_client,
            conversion_schema,
            lambda obj, ctx: asdict(obj)
        )

    def generate_session_id(self) -> str:
        return str(uuid.uuid4())

    def generate_user_id(self) -> str:
        return f"user_{hashlib.md5(fake.email().encode()).hexdigest()[:8]}"

    def generate_wallet_address(self) -> str:
        return f"0x{fake.hexify(text='^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')}"

    def get_weighted_country(self) -> Dict[str, str]:
        weights = [c['weight'] for c in CRYPTO_FRIENDLY_COUNTRIES]
        country = np.random.choice(CRYPTO_FRIENDLY_COUNTRIES, p=weights)
        return {
            'country': country['country'],
            'code': country['code'],
            'city': fake.city()
        }

    def get_business_hour_weight(self, timestamp: datetime) -> float:
        hour = timestamp.hour
        # Higher activity during business hours (9 AM - 6 PM UTC)
        if 9 <= hour <= 18:
            return 1.5
        elif 6 <= hour <= 9 or 18 <= hour <= 22:
            return 1.0
        else:
            return 0.3

    def should_introduce_data_quality_issue(self) -> bool:
        return self.data_quality_issues and random.random() < 0.05  # 5% chance

    def generate_clickstream_event(self) -> ClickstreamEvent:
        timestamp = datetime.now(timezone.utc)
        
        # Adjust for business hours
        business_weight = self.get_business_hour_weight(timestamp)
        if random.random() > business_weight:
            return None
        
        ico_website = random.choice(ICO_WEBSITES[:self.ico_websites_count])
        location = self.get_weighted_country()
        user_agent = random.choice(USER_AGENTS)
        
        # Determine device type and browser from user agent
        if 'Mobile' in user_agent or 'iPhone' in user_agent:
            device_type = 'mobile'
        elif 'iPad' in user_agent:
            device_type = 'tablet'
        else:
            device_type = 'desktop'
            
        browser = 'Chrome' if 'Chrome' in user_agent else 'Firefox' if 'Firefox' in user_agent else 'Safari'
        
        # Generate or use existing session
        session_key = f"{fake.ipv4()}_{device_type}"
        if session_key not in self.active_sessions or random.random() < 0.3:
            self.active_sessions[session_key] = {
                'session_id': self.generate_session_id(),
                'start_time': timestamp,
                'page_views': 0,
                'user_id': self.generate_user_id() if random.random() < 0.4 else None
            }
        
        session = self.active_sessions[session_key]
        session['page_views'] += 1
        
        page_type = random.choice(PAGE_TYPES)
        event_type = np.random.choice(
            ['page_view', 'click', 'scroll', 'form_interaction'],
            p=[0.5, 0.25, 0.15, 0.10]
        )
        
        # Data quality issues
        user_id = session['user_id']
        if self.should_introduce_data_quality_issue():
            user_id = None  # Simulate missing user ID
        
        ip_address = fake.ipv4()
        if self.should_introduce_data_quality_issue():
            ip_address = "unknown"  # Simulate missing IP with default value
        
        event = ClickstreamEvent(
            event_id=str(uuid.uuid4()),
            session_id=session['session_id'],
            user_id=user_id,
            timestamp=timestamp.isoformat(),
            event_type=event_type,
            page_type=page_type,
            page_url=f"https://{ico_website['domain']}/{page_type}",
            referrer=f"https://google.com/search?q={ico_website['name']}" if random.random() < 0.6 else None,
            user_agent=user_agent,
            ip_address=ip_address,
            country=location['country'],
            city=location['city'],
            device_type=device_type,
            browser=browser,
            session_duration=(timestamp - session['start_time']).seconds if session else None,
            page_load_time=round(random.uniform(0.5, 5.0), 2),
            ico_website_id=ico_website['id'],
            ico_website_name=ico_website['name'],
            button_clicked=random.choice(['register', 'whitepaper', 'presale', 'connect_wallet']) if event_type == 'click' else None,
            form_filled=random.choice(['registration', 'newsletter', 'kyc']) if event_type == 'form_interaction' else None,
            scroll_depth=round(random.uniform(0.1, 1.0), 2) if event_type == 'scroll' else None,
            bounce=session['page_views'] == 1 and random.random() < 0.4
        )
        
        return event

    def generate_user_registration(self) -> UserRegistration:
        timestamp = datetime.now(timezone.utc)
        location = self.get_weighted_country()
        ico_website = random.choice(ICO_WEBSITES[:self.ico_websites_count])
        user_id = self.generate_user_id()
        
        # Store user for future events
        self.user_registry[user_id] = {
            'registration_time': timestamp,
            'ico_website_id': ico_website['id'],
            'country': location['country']
        }
        
        # Social handles with some missing data
        social_handles = {}
        if random.random() < 0.7:
            social_handles['twitter'] = f"@{fake.user_name()}"
        if random.random() < 0.5:
            social_handles['telegram'] = f"@{fake.user_name()}"
        if random.random() < 0.3:
            social_handles['discord'] = f"{fake.user_name()}#{fake.random_int(1000, 9999)}"
        
        registration = UserRegistration(
            registration_id=str(uuid.uuid4()),
            user_id=user_id,
            timestamp=timestamp.isoformat(),
            email=fake.email(),
            wallet_address=self.generate_wallet_address() if random.random() < 0.8 else None,
            social_handles=social_handles,
            consent_marketing=random.random() < 0.7,
            consent_data=random.random() < 0.9,
            country=location['country'],
            ip_address=fake.ipv4(),
            ico_website_id=ico_website['id'],
            referral_source=random.choice(['google', 'twitter', 'reddit', 'telegram', 'direct']) if random.random() < 0.8 else None,
            kyc_completed=random.random() < 0.3,
            verification_tier=random.choice(['basic', 'advanced', 'premium'])
        )
        
        return registration

    def generate_conversion_event(self) -> ConversionEvent:
        if not self.user_registry:
            return None
            
        timestamp = datetime.now(timezone.utc)
        user_id = random.choice(list(self.user_registry.keys()))
        user_info = self.user_registry[user_id]
        
        # Only convert users who registered at least 10 minutes ago
        if (timestamp - user_info['registration_time']).total_seconds() < 600:
            return None
        
        conversion_types = ['wallet_connect', 'presale_participate', 'newsletter_signup', 'whitepaper_download']
        conversion_type = random.choice(conversion_types)
        
        amount_usd = None
        token_amount = None
        payment_method = None
        
        if conversion_type == 'presale_participate':
            amount_usd = round(random.uniform(100, 10000), 2)
            token_amount = round(amount_usd * random.uniform(1000, 5000), 2)
            payment_method = random.choice(['ETH', 'BTC', 'USDT', 'USDC'])
        
        conversion = ConversionEvent(
            conversion_id=str(uuid.uuid4()),
            user_id=user_id,
            session_id=self.generate_session_id(),
            timestamp=timestamp.isoformat(),
            conversion_type=conversion_type,
            ico_website_id=user_info['ico_website_id'],
            amount_usd=amount_usd,
            token_amount=token_amount,
            payment_method=payment_method,
            funnel_stage=random.choice(['awareness', 'interest', 'consideration', 'purchase']),
            time_to_convert=(timestamp - user_info['registration_time']).seconds,
            previous_visits=random.randint(1, 10)
        )
        
        return conversion

    def send_to_kafka(self, topic: str, event: Any, serializer):
        try:
            serialized_event = serializer(event, SerializationContext(topic, MessageField.VALUE))
            self.producer.produce(
                topic=topic,
                value=serialized_event,
                key=str(uuid.uuid4())
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to send event to {topic}: {e}")

    def run(self):
        logger.info(f"Starting ICO data generator - {self.events_per_minute} events per minute")
        
        events_per_second = self.events_per_minute / 60
        sleep_time = 1.0 / events_per_second if events_per_second > 1 else 1.0
        events_in_batch = max(1, int(events_per_second))
        
        while True:
            try:
                for _ in range(events_in_batch):
                    # Generate different types of events with different probabilities
                    event_type = np.random.choice(
                        ['clickstream', 'registration', 'conversion'],
                        p=[0.85, 0.10, 0.05]
                    )
                    
                    if event_type == 'clickstream':
                        event = self.generate_clickstream_event()
                        if event:
                            self.send_to_kafka('clickstream-events', event, self.clickstream_serializer)
                    
                    elif event_type == 'registration':
                        event = self.generate_user_registration()
                        if event:
                            self.send_to_kafka('user-registrations', event, self.registration_serializer)
                    
                    elif event_type == 'conversion':
                        event = self.generate_conversion_event()
                        if event:
                            self.send_to_kafka('conversions', event, self.conversion_serializer)
                
                self.producer.flush()
                time.sleep(sleep_time)
                
                # Clean up old sessions every 1000 iterations
                if random.randint(1, 1000) == 1:
                    current_time = datetime.now(timezone.utc)
                    self.active_sessions = {
                        k: v for k, v in self.active_sessions.items()
                        if (current_time - v['start_time']).total_seconds() < 3600  # Keep sessions for 1 hour
                    }
                    
                    # Clean up old users (keep last 10000)
                    if len(self.user_registry) > 10000:
                        sorted_users = sorted(
                            self.user_registry.items(),
                            key=lambda x: x[1]['registration_time'],
                            reverse=True
                        )
                        self.user_registry = dict(sorted_users[:10000])
                
            except KeyboardInterrupt:
                logger.info("Shutting down data generator...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)
        
        self.producer.flush()

if __name__ == "__main__":
    generator = ICODataGenerator()
    generator.run()