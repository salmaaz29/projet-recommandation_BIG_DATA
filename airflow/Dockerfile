# Image Airflow personnalisée avec Java 8 + PySpark
# Personne 3 — Pipeline Airflow

FROM apache/airflow:2.8.1

USER root

# Installer Java 8
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add - && \
    echo "deb https://adoptopenjdk.jfrog.io/adoptopenjdk/deb bookworm main" > /etc/apt/sources.list.d/adoptopenjdk.list && \
    apt-get update && \
    apt-get install -y adoptopenjdk-8-hotspot && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurer JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/adoptopenjdk-8-hotspot-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Installer PySpark et kafka-python
RUN pip install --no-cache-dir \
    pyspark==3.4.0 \
    kafka-python \
    pandas \
    numpy \
    setuptools