#!/usr/bin/env python3
"""
Simplified ICO Data Generator - JSON Version
Generates realistic ICO website analytics data
"""

import json
import time
import random
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import uuid
import hashlib

from faker import Faker
from kafka import KafkaProducer
import numpy as np

from config.ico_websites import ICO_WEBSITES, PAGE_TYPES, USER_AGENTS, CRYPTO_FRIENDLY_COUNTRIES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class ICODataGenerator:
    def __init__(self):
        self.kafka_config = {
            'bootstrap_servers': [os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')],
            'value_serializer': lambda x: json.dumps(x).encode('utf-8'),
            'key_serializer': lambda x: x.encode('utf-8') if x else None,
        }
        
        self.producer = KafkaProducer(**self.kafka_config)
        
        self.events_per_minute = int(os.getenv('EVENTS_PER_MINUTE', '500'))
        self.ico_websites_count = int(os.getenv('ICO_WEBSITES_COUNT', '20'))
        
        # Session tracking
        self.active_sessions: Dict[str, Dict] = {}
        self.user_registry: Dict[str, Dict] = {}

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

    def generate_clickstream_event(self) -> Dict[str, Any]:
        timestamp = datetime.now(timezone.utc)
        
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
                'session_id': str(uuid.uuid4()),
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
        
        event = {
            "event_id": str(uuid.uuid4()),
            "session_id": session['session_id'],
            "user_id": session['user_id'],
            "timestamp": timestamp.isoformat(),
            "event_type": event_type,
            "page_type": page_type,
            "page_url": f"https://{ico_website['domain']}/{page_type}",
            "referrer": f"https://google.com/search?q={ico_website['name']}" if random.random() < 0.6 else None,
            "user_agent": user_agent,
            "ip_address": fake.ipv4(),
            "country": location['country'],
            "city": location['city'],
            "device_type": device_type,
            "browser": browser,
            "session_duration": (timestamp - session['start_time']).seconds if session else None,
            "page_load_time": round(random.uniform(0.5, 5.0), 2),
            "ico_website_id": ico_website['id'],
            "ico_website_name": ico_website['name'],
            "button_clicked": random.choice(['register', 'whitepaper', 'presale', 'connect_wallet']) if event_type == 'click' else None,
            "form_filled": random.choice(['registration', 'newsletter', 'kyc']) if event_type == 'form_interaction' else None,
            "scroll_depth": round(random.uniform(0.1, 1.0), 2) if event_type == 'scroll' else None,
            "bounce": session['page_views'] == 1 and random.random() < 0.4
        }
        
        return event

    def generate_user_registration(self) -> Dict[str, Any]:
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
        
        registration = {
            "registration_id": str(uuid.uuid4()),
            "user_id": user_id,
            "timestamp": timestamp.isoformat(),
            "email": fake.email(),
            "wallet_address": self.generate_wallet_address() if random.random() < 0.8 else None,
            "social_handles": social_handles,
            "consent_marketing": random.random() < 0.7,
            "consent_data": random.random() < 0.9,
            "country": location['country'],
            "ip_address": fake.ipv4(),
            "ico_website_id": ico_website['id'],
            "referral_source": random.choice(['google', 'twitter', 'reddit', 'telegram', 'direct']) if random.random() < 0.8 else None,
            "kyc_completed": random.random() < 0.3,
            "verification_tier": random.choice(['basic', 'advanced', 'premium'])
        }
        
        return registration

    def generate_conversion_event(self) -> Optional[Dict[str, Any]]:
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
        
        conversion = {
            "conversion_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": str(uuid.uuid4()),
            "timestamp": timestamp.isoformat(),
            "conversion_type": conversion_type,
            "ico_website_id": user_info['ico_website_id'],
            "amount_usd": amount_usd,
            "token_amount": token_amount,
            "payment_method": payment_method,
            "funnel_stage": random.choice(['awareness', 'interest', 'consideration', 'purchase']),
            "time_to_convert": (timestamp - user_info['registration_time']).seconds,
            "previous_visits": random.randint(1, 10)
        }
        
        return conversion

    def send_to_kafka(self, topic: str, event: Dict[str, Any]):
        try:
            future = self.producer.send(topic, value=event, key=event.get('event_id', str(uuid.uuid4())))
            # Don't wait for all sends to complete - fire and forget for performance
            if random.random() < 0.1:  # Occasionally check if sends are working
                future.get(timeout=1)
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
                            self.send_to_kafka('clickstream-events', event)
                    
                    elif event_type == 'registration':
                        event = self.generate_user_registration()
                        if event:
                            self.send_to_kafka('user-registrations', event)
                    
                    elif event_type == 'conversion':
                        event = self.generate_conversion_event()
                        if event:
                            self.send_to_kafka('conversions', event)
                
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
                    
                    logger.info(f"Active sessions: {len(self.active_sessions)}, Registered users: {len(self.user_registry)}")
                
            except KeyboardInterrupt:
                logger.info("Shutting down data generator...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(5)
        
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    generator = ICODataGenerator()
    generator.run()