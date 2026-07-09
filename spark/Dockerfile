FROM bitnami/spark:3.5

# ============================================================
# Why a custom Spark image?
# ============================================================
# The bitnami/spark image doesn't include the PostgreSQL JDBC
# driver. Spark needs this driver to write to Postgres via:
#
#   df.write.format("jdbc").option("url", "jdbc:postgresql://...")
#
# Without the JAR in /opt/bitnami/spark/jars/, Spark throws
# ClassNotFoundException at runtime.
#
# Alternative: use --packages org.postgresql:postgresql:42.7.3
# at spark-submit time (downloads from Maven Central at runtime).
# Baking it into the image is more reliable (works offline,
# faster startup, no Maven dependency).
# ============================================================

USER root
RUN curl -sL https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /opt/bitnami/spark/jars/postgresql-42.7.3.jar

# Switch back to the default non-root user (UID 1001 in bitnami images)
USER 1001
