package com.ico.analytics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class ICOAnalyticsJob {

    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String POSTGRES_URL = "jdbc:postgresql://postgres:5432/ico_analytics";
    private static final String POSTGRES_USER = "flink_user";
    private static final String POSTGRES_PASSWORD = "flink_password";

    public static void main(String[] args) throws Exception {
        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); // Checkpoint every minute
        
        // Create table environment for SQL operations
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            env, 
            EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build()
        );

        // Configure Kafka sources
        KafkaSource<String> clickstreamSource = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics("clickstream-events")
            .setGroupId("flink-analytics-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> registrationSource = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics("user-registrations")
            .setGroupId("flink-analytics-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> conversionSource = KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics("conversions")
            .setGroupId("flink-analytics-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Create data streams
        DataStream<String> clickstreamStream = env.fromSource(
            clickstreamSource,
            WatermarkStrategy.noWatermarks(),
            "Clickstream Source"
        );

        DataStream<String> registrationStream = env.fromSource(
            registrationSource,
            WatermarkStrategy.noWatermarks(),
            "Registration Source"
        );

        DataStream<String> conversionStream = env.fromSource(
            conversionSource,
            WatermarkStrategy.noWatermarks(),
            "Conversion Source"
        );

        // Process clickstream events
        processClickstreamEvents(env, tableEnv, clickstreamStream);
        
        // Process user registrations
        processUserRegistrations(env, tableEnv, registrationStream);
        
        // Process conversions
        processConversions(env, tableEnv, conversionStream);
        
        // Create real-time aggregations
        createRealTimeAggregations(env, tableEnv);

        // Execute job
        env.execute("ICO Analytics Processing Job");
    }

    private static void processClickstreamEvents(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            DataStream<String> clickstreamStream) {

        // Parse and transform clickstream events
        DataStream<ClickstreamEvent> parsedEvents = clickstreamStream
            .map(new ClickstreamEventParser());

        // Sink to PostgreSQL
        parsedEvents.addSink(JdbcSink.sink(
            "INSERT INTO raw_data.clickstream_events (" +
            "event_id, session_id, user_id, timestamp, event_type, page_type, " +
            "page_url, referrer, user_agent, ip_address, country, city, " +
            "device_type, browser, session_duration, page_load_time, " +
            "ico_website_id, ico_website_name, button_clicked, form_filled, " +
            "scroll_depth, bounce) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::inet, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (JdbcStatementBuilder<ClickstreamEvent>) (preparedStatement, event) -> {
                preparedStatement.setString(1, event.eventId);
                preparedStatement.setString(2, event.sessionId);
                preparedStatement.setString(3, event.userId);
                preparedStatement.setTimestamp(4, Timestamp.valueOf(event.timestamp));
                preparedStatement.setString(5, event.eventType);
                preparedStatement.setString(6, event.pageType);
                preparedStatement.setString(7, event.pageUrl);
                preparedStatement.setString(8, event.referrer);
                preparedStatement.setString(9, event.userAgent);
                preparedStatement.setString(10, event.ipAddress);
                preparedStatement.setString(11, event.country);
                preparedStatement.setString(12, event.city);
                preparedStatement.setString(13, event.deviceType);
                preparedStatement.setString(14, event.browser);
                if (event.sessionDuration != null) {
                    preparedStatement.setInt(15, event.sessionDuration);
                } else {
                    preparedStatement.setNull(15, java.sql.Types.INTEGER);
                }
                preparedStatement.setFloat(16, event.pageLoadTime);
                preparedStatement.setInt(17, event.icoWebsiteId);
                preparedStatement.setString(18, event.icoWebsiteName);
                preparedStatement.setString(19, event.buttonClicked);
                preparedStatement.setString(20, event.formFilled);
                if (event.scrollDepth != null) {
                    preparedStatement.setFloat(21, event.scrollDepth);
                } else {
                    preparedStatement.setNull(21, java.sql.Types.FLOAT);
                }
                preparedStatement.setBoolean(22, event.bounce);
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(1000)
                .withBatchIntervalMs(5000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(POSTGRES_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(POSTGRES_USER)
                .withPassword(POSTGRES_PASSWORD)
                .build()
        ));

        // Real-time metrics: Events per minute
        parsedEvents
            .keyBy(event -> event.icoWebsiteId)
            .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
            .apply(new WindowFunction<ClickstreamEvent, RealtimeMetric, Integer, TimeWindow>() {
                @Override
                public void apply(Integer key, TimeWindow window, 
                                Iterable<ClickstreamEvent> input, 
                                Collector<RealtimeMetric> out) {
                    long count = 0;
                    for (ClickstreamEvent event : input) {
                        count++;
                    }
                    out.collect(new RealtimeMetric(
                        new Timestamp(window.getEnd()),
                        "events_per_minute",
                        (float) count,
                        key
                    ));
                }
            })
            .addSink(JdbcSink.sink(
                "INSERT INTO analytics.realtime_metrics (timestamp, metric_name, metric_value, ico_website_id) VALUES (?, ?, ?, ?)",
                (JdbcStatementBuilder<RealtimeMetric>) (preparedStatement, metric) -> {
                    preparedStatement.setTimestamp(1, metric.timestamp);
                    preparedStatement.setString(2, metric.metricName);
                    preparedStatement.setFloat(3, metric.metricValue);
                    preparedStatement.setInt(4, metric.icoWebsiteId);
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(100)
                    .withBatchIntervalMs(1000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(POSTGRES_URL)
                    .withDriverName("org.postgresql.Driver")
                    .withUsername(POSTGRES_USER)
                    .withPassword(POSTGRES_PASSWORD)
                    .build()
            ));
    }

    private static void processUserRegistrations(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            DataStream<String> registrationStream) {

        DataStream<UserRegistration> parsedRegistrations = registrationStream
            .map(new UserRegistrationParser());

        parsedRegistrations.addSink(JdbcSink.sink(
            "INSERT INTO raw_data.user_registrations (" +
            "registration_id, user_id, timestamp, email, wallet_address, " +
            "social_handles, consent_marketing, consent_data, country, " +
            "ip_address, ico_website_id, referral_source, kyc_completed, verification_tier) " +
            "VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?::inet, ?, ?, ?, ?)",
            (JdbcStatementBuilder<UserRegistration>) (preparedStatement, registration) -> {
                preparedStatement.setString(1, registration.registrationId);
                preparedStatement.setString(2, registration.userId);
                preparedStatement.setTimestamp(3, Timestamp.valueOf(registration.timestamp));
                preparedStatement.setString(4, registration.email);
                preparedStatement.setString(5, registration.walletAddress);
                preparedStatement.setString(6, registration.socialHandles);
                preparedStatement.setBoolean(7, registration.consentMarketing);
                preparedStatement.setBoolean(8, registration.consentData);
                preparedStatement.setString(9, registration.country);
                preparedStatement.setString(10, registration.ipAddress);
                preparedStatement.setInt(11, registration.icoWebsiteId);
                preparedStatement.setString(12, registration.referralSource);
                preparedStatement.setBoolean(13, registration.kycCompleted);
                preparedStatement.setString(14, registration.verificationTier);
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(5000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(POSTGRES_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(POSTGRES_USER)
                .withPassword(POSTGRES_PASSWORD)
                .build()
        ));
    }

    private static void processConversions(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            DataStream<String> conversionStream) {

        DataStream<ConversionEvent> parsedConversions = conversionStream
            .map(new ConversionEventParser());

        parsedConversions.addSink(JdbcSink.sink(
            "INSERT INTO raw_data.conversions (" +
            "conversion_id, user_id, session_id, timestamp, conversion_type, " +
            "ico_website_id, amount_usd, token_amount, payment_method, " +
            "funnel_stage, time_to_convert, previous_visits) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (JdbcStatementBuilder<ConversionEvent>) (preparedStatement, conversion) -> {
                preparedStatement.setString(1, conversion.conversionId);
                preparedStatement.setString(2, conversion.userId);
                preparedStatement.setString(3, conversion.sessionId);
                preparedStatement.setTimestamp(4, Timestamp.valueOf(conversion.timestamp));
                preparedStatement.setString(5, conversion.conversionType);
                preparedStatement.setInt(6, conversion.icoWebsiteId);
                if (conversion.amountUsd != null) {
                    preparedStatement.setFloat(7, conversion.amountUsd);
                } else {
                    preparedStatement.setNull(7, java.sql.Types.FLOAT);
                }
                if (conversion.tokenAmount != null) {
                    preparedStatement.setFloat(8, conversion.tokenAmount);
                } else {
                    preparedStatement.setNull(8, java.sql.Types.FLOAT);
                }
                preparedStatement.setString(9, conversion.paymentMethod);
                preparedStatement.setString(10, conversion.funnelStage);
                preparedStatement.setInt(11, conversion.timeToConvert);
                preparedStatement.setInt(12, conversion.previousVisits);
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(5000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(POSTGRES_URL)
                .withDriverName("org.postgresql.Driver")
                .withUsername(POSTGRES_USER)
                .withPassword(POSTGRES_PASSWORD)
                .build()
        ));
    }

    private static void createRealTimeAggregations(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv) {

        // Register Kafka tables in Table API
        tableEnv.executeSql(
            "CREATE TABLE clickstream_events (" +
            "  event_id STRING," +
            "  session_id STRING," +
            "  user_id STRING," +
            "  timestamp TIMESTAMP(3)," +
            "  event_type STRING," +
            "  page_type STRING," +
            "  ico_website_id INT," +
            "  country STRING," +
            "  device_type STRING," +
            "  page_load_time FLOAT" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = 'clickstream-events'," +
            "  'properties.bootstrap.servers' = '" + KAFKA_BOOTSTRAP_SERVERS + "'," +
            "  'properties.group.id' = 'flink-table-api-group'," +
            "  'scan.startup.mode' = 'latest-offset'," +
            "  'format' = 'json'" +
            ")"
        );

        // Create 5-minute aggregations using SQL
        Table aggregations5min = tableEnv.sqlQuery(
            "SELECT " +
            "  TUMBLE_START(timestamp, INTERVAL '5' MINUTE) as window_start," +
            "  TUMBLE_END(timestamp, INTERVAL '5' MINUTE) as window_end," +
            "  ico_website_id," +
            "  country," +
            "  device_type," +
            "  COUNT(*) as total_events," +
            "  COUNT(DISTINCT session_id) as unique_sessions," +
            "  COUNT(DISTINCT user_id) as unique_users," +
            "  SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views," +
            "  AVG(page_load_time) as avg_page_load_time " +
            "FROM clickstream_events " +
            "WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1' HOUR " +
            "GROUP BY " +
            "  TUMBLE(timestamp, INTERVAL '5' MINUTE)," +
            "  ico_website_id," +
            "  country," +
            "  device_type"
        );

        // Convert back to DataStream and sink to PostgreSQL
        env.fromDataStream(tableEnv.toChangelogStream(aggregations5min))
            .map(new MetricsAggregationMapper())
            .addSink(JdbcSink.sink(
                "INSERT INTO analytics.metrics_5min (" +
                "window_start, window_end, ico_website_id, country, device_type, " +
                "total_events, unique_sessions, unique_users, page_views, avg_page_load_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (window_start, ico_website_id, COALESCE(country, ''), COALESCE(device_type, '')) " +
                "DO UPDATE SET " +
                "total_events = EXCLUDED.total_events, " +
                "unique_sessions = EXCLUDED.unique_sessions, " +
                "unique_users = EXCLUDED.unique_users, " +
                "page_views = EXCLUDED.page_views, " +
                "avg_page_load_time = EXCLUDED.avg_page_load_time",
                (JdbcStatementBuilder<MetricsAggregation>) (preparedStatement, metrics) -> {
                    preparedStatement.setTimestamp(1, metrics.windowStart);
                    preparedStatement.setTimestamp(2, metrics.windowEnd);
                    preparedStatement.setInt(3, metrics.icoWebsiteId);
                    preparedStatement.setString(4, metrics.country);
                    preparedStatement.setString(5, metrics.deviceType);
                    preparedStatement.setLong(6, metrics.totalEvents);
                    preparedStatement.setLong(7, metrics.uniqueSessions);
                    preparedStatement.setLong(8, metrics.uniqueUsers);
                    preparedStatement.setLong(9, metrics.pageViews);
                    preparedStatement.setFloat(10, metrics.avgPageLoadTime);
                },
                JdbcExecutionOptions.builder()
                    .withBatchSize(50)
                    .withBatchIntervalMs(10000)
                    .withMaxRetries(3)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(POSTGRES_URL)
                    .withDriverName("org.postgresql.Driver")
                    .withUsername(POSTGRES_USER)
                    .withPassword(POSTGRES_PASSWORD)
                    .build()
            ));
    }

    // Data classes and parsers
    public static class ClickstreamEvent {
        public String eventId;
        public String sessionId;
        public String userId;
        public String timestamp;
        public String eventType;
        public String pageType;
        public String pageUrl;
        public String referrer;
        public String userAgent;
        public String ipAddress;
        public String country;
        public String city;
        public String deviceType;
        public String browser;
        public Integer sessionDuration;
        public Float pageLoadTime;
        public Integer icoWebsiteId;
        public String icoWebsiteName;
        public String buttonClicked;
        public String formFilled;
        public Float scrollDepth;
        public Boolean bounce;

        // Constructor, getters, setters...
    }

    public static class UserRegistration {
        public String registrationId;
        public String userId;
        public String timestamp;
        public String email;
        public String walletAddress;
        public String socialHandles;
        public Boolean consentMarketing;
        public Boolean consentData;
        public String country;
        public String ipAddress;
        public Integer icoWebsiteId;
        public String referralSource;
        public Boolean kycCompleted;
        public String verificationTier;
    }

    public static class ConversionEvent {
        public String conversionId;
        public String userId;
        public String sessionId;
        public String timestamp;
        public String conversionType;
        public Integer icoWebsiteId;
        public Float amountUsd;
        public Float tokenAmount;
        public String paymentMethod;
        public String funnelStage;
        public Integer timeToConvert;
        public Integer previousVisits;
    }

    public static class RealtimeMetric {
        public Timestamp timestamp;
        public String metricName;
        public Float metricValue;
        public Integer icoWebsiteId;

        public RealtimeMetric(Timestamp timestamp, String metricName, Float metricValue, Integer icoWebsiteId) {
            this.timestamp = timestamp;
            this.metricName = metricName;
            this.metricValue = metricValue;
            this.icoWebsiteId = icoWebsiteId;
        }
    }

    public static class MetricsAggregation {
        public Timestamp windowStart;
        public Timestamp windowEnd;
        public Integer icoWebsiteId;
        public String country;
        public String deviceType;
        public Long totalEvents;
        public Long uniqueSessions;
        public Long uniqueUsers;
        public Long pageViews;
        public Float avgPageLoadTime;
    }

    // Mapper functions
    public static class ClickstreamEventParser implements MapFunction<String, ClickstreamEvent> {
        @Override
        public ClickstreamEvent map(String value) throws Exception {
            // Parse JSON and map to ClickstreamEvent
            // Implementation would use Jackson or similar JSON parser
            return new ClickstreamEvent(); // Placeholder
        }
    }

    public static class UserRegistrationParser implements MapFunction<String, UserRegistration> {
        @Override
        public UserRegistration map(String value) throws Exception {
            return new UserRegistration(); // Placeholder
        }
    }

    public static class ConversionEventParser implements MapFunction<String, ConversionEvent> {
        @Override
        public ConversionEvent map(String value) throws Exception {
            return new ConversionEvent(); // Placeholder
        }
    }

    public static class MetricsAggregationMapper implements MapFunction<Object, MetricsAggregation> {
        @Override
        public MetricsAggregation map(Object value) throws Exception {
            return new MetricsAggregation(); // Placeholder
        }
    }
}