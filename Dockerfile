# ==============================================================================
# GitPulse: Offline Apache Iceberg 1.7.1 & PySpark 3.5.4 Runtime
# ==============================================================================
# Provides a self-contained runtime matching AWS Glue 5.0 (Spark 3.5.4 / Iceberg 1.7.1)
# Operates 100% offline with zero cloud credentials or external network calls needed.
# ==============================================================================

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    SPARK_VERSION=3.5.4 \
    ICEBERG_VERSION=1.7.1

# Install OpenJDK 17 and utility tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /workspace

# Install core python requirements
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-dev.txt

# Download Apache Iceberg Spark Runtime Jar for Spark 3.5 / Scala 2.12
RUN mkdir -p /opt/spark-jars && \
    curl -fsSL -o /opt/spark-jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar

ENV SPARK_CLASSPATH="/opt/spark-jars/*"
ENV PYTHONPATH="/workspace/src:${PYTHONPATH}"

# Copy source repository
COPY . .

RUN chmod +x scripts/run_demo.sh

# Default command: run the full verification demo
CMD ["bash", "scripts/run_demo.sh"]
