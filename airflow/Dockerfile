# Custom Airflow 3 image with Java + PySpark for maritime fishing pipeline.
# Build from this folder:
#   docker compose build
#
# Base must stay aligned with docker-compose image tag / Python version.
# Airflow 3.4.0-python3.12 is Debian 13 (trixie): OpenJDK 17 is gone from
# default apt; use OpenJDK 21 (supported by PySpark 4.x).

FROM apache/airflow:3.4.0-python3.12

# 1. Install Java 21 (Debian trixie default OpenJDK)
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Configure JAVA_HOME (path differs by CPU arch)
RUN JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && JAVA_DIR="$(dirname "$(dirname "$JAVA_BIN")")" \
    && ln -sfn "$JAVA_DIR" /usr/lib/jvm/java-21

ENV JAVA_HOME=/usr/lib/jvm/java-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 3. Install Python dependencies as airflow user
USER airflow

COPY --chown=airflow:root requirements-airflow.txt /requirements-airflow.txt

# Reinstall airflow at the image version so pip does not downgrade/upgrade it
# when resolving extra packages (Airflow docker-stack recommendation).
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /requirements-airflow.txt

# 4. Verify Java and Python at build time
RUN java -version && python --version
