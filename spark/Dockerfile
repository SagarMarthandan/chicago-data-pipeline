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

# Switch back to the spark user (UID 185 in apache/spark images)
USER spark
