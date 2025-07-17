FROM public.ecr.aws/emr-serverless/spark/emr-7.5.0:20250425

USER root
ENV PYSPARK_PYTHON=/usr/bin/python3.11

RUN yum install -y iputils
RUN yum install -y git

# Update and install required tools
RUN yum update -y && yum install -y wget

# Download specific JAR files
RUN wget -P /usr/lib/spark/jars \
   https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.160/bundle-2.20.160.jar && \
   wget -P /usr/lib/spark/jars \
   https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.21.0/snowflake-jdbc-3.21.0.jar && \
   wget -P /usr/lib/spark/jars \
   https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/3.1.1/spark-snowflake_2.12-3.1.1.jar

   
# Remove conflicting JAR
# RUN rm /usr/lib/spark/jars/spark-efgac-iceberg_2.12-3.5.2-amzn-1.jar
# wget -P /usr/lib/spark/jars \
# https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.1/iceberg-spark-runtime-3.5_2.12-1.4.1.jar && \

# Set environment variables
ARG ENVIRONMENT


# Install Python requirements
COPY ./docker/spark/spark-utils/requirements.txt requirements.txt
RUN /usr/bin/python3.11 -m pip install --no-cache-dir --ignore-installed -r requirements.txt 
RUN /usr/bin/python3.11 -m pip install --force-reinstall dbt-adapters

COPY ./docker/spark/spark-utils/adapter_backed_client.py  /usr/local/lib/python3.11/site-packages/dbt_metricflow/cli/dbt_connectors/adapter_backed_client.py 


# Copy dbt files and create necessary directories
COPY ./dbt/coin_spark /var/lib/spark/coin_spark
COPY ./dbt/profiles.yml /var/lib/spark/.dbt/.

# Create logs directory and set permissions

WORKDIR /var/lib/spark/coin_spark
ENV DBT_PROFILES_DIR=/var/lib/spark/.dbt


# Install dbt dependencies
RUN dbt clean
RUN dbt deps

# Copy additional scripts

RUN chmod -R 777 /var/lib/spark/coin_spark

COPY ./docker/spark/spark-utils/start-thrift-server.sh /start-thrift-server.sh


USER hadoop:hadoop