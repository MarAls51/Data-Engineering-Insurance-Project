FROM public.ecr.aws/bitnami/spark:latest

WORKDIR /app

USER root

RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    pandas joblib psycopg2-binary scikit-learn python-dotenv pyarrow

ENV POSTGRES_DRIVER_VERSION 42.7.3
ENV SPARK_DRIVER_URL https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar

RUN apt-get update && apt-get install -y wget --no-install-recommends \
    && wget -qO /opt/bitnami/spark/jars/postgresql.jar $SPARK_DRIVER_URL \
    && rm -rf /var/lib/apt/lists/*

