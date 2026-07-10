FROM apache/airflow:3.0.6

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_PROVIDER_REF=02a7adc531e4987c2049b8a2501672cc6b1e3e4c
ARG AZURE_PROVIDER_REF=108ecac75b737b2c7315e2b1093bbc73fe6f4c74
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon==9.30.0" \
    "apache-airflow-providers-google @ git+https://github.com/apache/airflow.git@${AIRFLOW_PROVIDER_REF}#subdirectory=providers/google" \
    "apache-airflow-providers-microsoft-azure @ git+https://github.com/apache/airflow.git@${AZURE_PROVIDER_REF}#subdirectory=providers/microsoft/azure" \
    "google-cloud-aiplatform>=1.155.0" \
    "google-genai>=1.0.0"
