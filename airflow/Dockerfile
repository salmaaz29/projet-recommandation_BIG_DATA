# Image Airflow personnalisée avec Java 11 + PySpark + kafka-python
# Personne 3 — Pipeline Airflow
# Note : Java 11 est utilisé dans le conteneur Docker (Linux).
#        Java 8 est requis uniquement sur Windows (Personne 2).
#        Spark 3.4 est compatible Java 8, 11 et 17.

FROM apache/airflow:2.8.1

USER root

# Installer Java 11 (Temurin — source officielle et fiable)
RUN apt-get update && \
    apt-get install -y wget apt-transport-https && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
        | gpg --dearmor \
        | tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null && \
    echo "deb https://packages.adoptium.net/artifactory/deb bookworm main" \
        > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y temurin-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurer JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV JAVA_TOOL_OPTIONS=""
ENV PYSPARK_PYTHON=python3

USER airflow

# Installer PySpark et dépendances
RUN pip install --no-cache-dir \
    pyspark==3.4.0 \
    kafka-python \
    pandas \
    numpy \
    setuptools