FROM apache/spark:3.5.1

# ============================================================
# Why a custom Spark image?
# ============================================================
# The official apache/spark image doesn't include the PostgreSQL
# JDBC driver. Spark needs this driver to write to Postgres via:
#
#   df.write.format("jdbc").option("url", "jdbc:postgresql://...")
#
# Without the JAR in /opt/spark/jars/, Spark throws
# ClassNotFoundException at runtime.
#
# Alternative: use --packages org.postgresql:postgresql:42.7.3
# at spark-submit time (downloads from Maven Central at runtime).
# Baking it into the image is more reliable (works offline,
# faster startup, no Maven dependency).
#
# Note: We switched from bitnami/spark to apache/spark because
# Bitnami moved their images behind a commercial subscription
# (Bitnami Secure Images) in 2026. The free docker.io/bitnami/*
# images are no longer available.
# ============================================================

USER root
RUN curl -sL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/spark/jars/postgresql-42.7.3.jar

# ============================================================
# Kafka connector JARs (Phase 2.4 — Structured Streaming)
# ============================================================
# Spark Structured Streaming needs the Kafka connector to read from
# Kafka topics via readStream.format("kafka"). The official apache/spark
# image does NOT include it — only ships core Spark JARs.
#
# We bake 4 JARs into the image (same approach as the JDBC driver):
#
#   1. spark-sql-kafka-0-10_2.12-3.5.1.jar
#      The main connector — provides KafkaSourceProvider for
#      format("kafka") in Structured Streaming.
#
#   2. spark-token-provider-kafka-0-10_2.12-3.5.1.jar
#      Dependency of #1 — handles Kafka authentication tokens
#      (SASL/Delegation tokens). Required even for PLAINTEXT
#      brokers because the connector loads it unconditionally.
#
#   3. kafka-clients-3.5.1.jar
#      The Kafka protocol client library. spark-sql-kafka delegates
#      all network I/O (fetch, offset commit) to this. Version must
#      match the Spark Kafka connector version (3.5.1).
#
#   4. commons-pool2-2.11.1.jar
#      Object pooling library. spark-token-provider uses it to pool
#      Kafka client connections. Without it: NoClassDefFoundError
#      at stream startup.
#
# Alternative: --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
# at spark-submit time (downloads from Maven at runtime). Baking is
# more reliable — works offline, no Maven dependency, faster startup.
#
# Scala version: _2.12 (apache/spark:3.5.1 is built against Scala 2.12)
# ============================================================
RUN curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar \
    -o /opt/spark/jars/kafka-clients-3.5.1.jar && \
    curl -sL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /opt/spark/jars/commons-pool2-2.11.1.jar

# Checkpoint directory for Structured Streaming (Phase 2.4).
# Named volumes inherit permissions from this directory on first mount.
# Must be owned by spark user — Spark runs as spark, not root.
RUN mkdir -p /opt/spark/checkpoints && chown spark:spark /opt/spark/checkpoints

# Switch back to the spark user (UID 185 in apache/spark images)
USER spark
