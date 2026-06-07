FROM apache/hive:4.0.0

USER root

# apache/hive:4.0.0 không đi kèm curl và PostgreSQL JDBC driver
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -fsSL -o /opt/hive/lib/postgresql-42.7.3.jar \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER hive
