FROM postgres:15

# Install additional extensions
RUN apt-get update && apt-get install -y \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

# Set default configuration for analytics workload
RUN echo "shared_preload_libraries = 'pg_stat_statements'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "pg_stat_statements.track = all" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "pg_stat_statements.max = 10000" >> /usr/share/postgresql/postgresql.conf.sample

# Optimize for write-heavy workloads
RUN echo "checkpoint_completion_target = 0.9" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "wal_buffers = 16MB" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "checkpoint_timeout = 15min" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_wal_size = 2GB" >> /usr/share/postgresql/postgresql.conf.sample