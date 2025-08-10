# Avro Schemas for ICO Analytics Pipeline

This directory contains Avro schemas for the ICO analytics data pipeline. These schemas define the structure of events flowing through Kafka topics and ensure data consistency across producers and consumers.

## Schemas

### 1. ClickstreamEvent (`clickstream-event.avsc`)
Captures user interactions on ICO websites including page views, clicks, and form interactions.

**Key Fields:**
- `event_id`: Unique event identifier
- `session_id`: Session identifier
- `user_id`: User identifier (optional)
- `event_type`: Type of event (page_view, click, scroll, form_interaction)
- `page_type`: Type of page (homepage, whitepaper, presale, etc.)
- `ico_website_id`: ICO website identifier
- `device_type`: Device type (desktop, mobile, tablet)
- `country`: User's country
- `page_load_time`: Page load performance metric

**Usage:**
```bash
# Topic: clickstream-events
# Retention: 7 days
# Partitions: 12 (by ico_website_id)
```

### 2. UserRegistration (`user-registration.avsc`)
Records user registration events across ICO websites.

**Key Fields:**
- `registration_id`: Unique registration identifier
- `user_id`: Unique user identifier
- `email`: User email address
- `wallet_address`: Crypto wallet address (optional)
- `social_handles`: Map of social media handles
- `consent_marketing`: Marketing consent flag
- `kyc_completed`: KYC verification status
- `verification_tier`: User verification level

**Usage:**
```bash
# Topic: user-registrations
# Retention: 30 days
# Partitions: 6 (by ico_website_id)
```

### 3. ConversionEvent (`conversion-event.avsc`)
Tracks conversion events including purchases, downloads, and other valuable actions.

**Key Fields:**
- `conversion_id`: Unique conversion identifier
- `user_id`: User identifier
- `conversion_type`: Type of conversion (wallet_connect, presale_participate, etc.)
- `amount_usd`: Financial amount (for purchases)
- `token_amount`: Number of tokens
- `payment_method`: Payment method used
- `time_to_convert`: Time from first visit to conversion

**Usage:**
```bash
# Topic: conversions
# Retention: 90 days
# Partitions: 6 (by ico_website_id)
```

## Schema Evolution

All schemas include a `schema_version` field to support backward and forward compatibility. When evolving schemas:

1. **Adding Fields**: Add new fields with default values
2. **Removing Fields**: Mark fields as deprecated first, then remove in next major version
3. **Changing Types**: Use union types for backward compatibility
4. **Renaming Fields**: Add new field with default, deprecate old field

## Example Schema Evolution

```json
// Version 1.0 -> 1.1: Adding new field
{
  "name": "new_field",
  "type": ["null", "string"],
  "default": null,
  "doc": "New field added in version 1.1"
}
```

## Validation

Schemas are validated using the Confluent Schema Registry. All producers must register their schemas before publishing messages.

### Register Schema
```bash
# Register clickstream event schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @clickstream-event.avsc \
  http://localhost:8081/subjects/clickstream-events-value/versions

# Register user registration schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @user-registration.avsc \
  http://localhost:8081/subjects/user-registrations-value/versions

# Register conversion event schema
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @conversion-event.avsc \
  http://localhost:8081/subjects/conversions-value/versions
```

## Data Quality

### Required Fields
All events must include:
- Unique identifiers (`event_id`, `registration_id`, `conversion_id`)
- Timestamp in ISO 8601 format
- ICO website identifier

### Optional Fields
Fields marked as unions with `null` can be omitted but should be included when available:
- User identifiers (for anonymous events)
- Geographic information
- Performance metrics

### Validation Rules
- Timestamps must be valid ISO 8601 format
- Email addresses must be valid format
- Wallet addresses must match standard crypto address formats
- Amounts must be positive numbers
- Enum values must match defined symbols

## Performance Considerations

### Partitioning Strategy
- **ClickstreamEvents**: Partitioned by `ico_website_id` for balanced load
- **UserRegistrations**: Partitioned by `ico_website_id` for co-location
- **ConversionEvents**: Partitioned by `ico_website_id` for analytics efficiency

### Serialization
- Use Avro binary encoding for optimal performance
- Enable schema registry caching in producers/consumers
- Consider compression (snappy) for high-throughput topics

### Monitoring
- Track schema evolution metrics
- Monitor serialization/deserialization errors
- Alert on schema compatibility issues

## Tools

### Schema Registry UI
Access the Schema Registry UI at: http://localhost:8081

### Kafka Tools
```bash
# List schemas
curl http://localhost:8081/subjects

# Get latest schema version
curl http://localhost:8081/subjects/clickstream-events-value/versions/latest

# Check compatibility
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @new-schema.avsc \
  http://localhost:8081/compatibility/subjects/clickstream-events-value/versions/latest
```