FROM apache/airflow:3.0.6

ARG AIRFLOW_PROVIDER_REF=0ac475f1d8a50657ed80b4ef495e02ab2add8c01

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon @ git+https://github.com/apache/airflow.git@${AIRFLOW_PROVIDER_REF}#subdirectory=providers/amazon"
