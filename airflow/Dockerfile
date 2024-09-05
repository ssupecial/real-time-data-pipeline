# Dockerfile

FROM apache/airflow:2.9.3

# Install dependencies
ARG AIRFLOW_UID=${AIRFLOW_UID}
USER root

RUN apt-get update
RUN apt install -y default-jdk wget git
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*


# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

# Set Spark Home
ENV SPARK_HOME /opt/spark
RUN export SPARK_HOME

USER airflow

RUN pip install --no-cache-dir apache-airflow[mysql,spark,postgres]

WORKDIR /opt/airflow
