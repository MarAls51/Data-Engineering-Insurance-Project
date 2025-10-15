
FROM public.ecr.aws/bitnami/spark:latest

WORKDIR /app

COPY ./spark /app
COPY .env /app/.env
copy ./models /app

USER root

RUN /opt/bitnami/python/bin/pip install --no-cache-dir \
    pandas joblib psycopg2-binary scikit-learn python-dotenv pyarrow

