# Agentic Airflow PoC

Apache Airflow local invoking an Amazon Bedrock AgentCore Runtime to troubleshoot failed DAG tasks and post findings to Slack.

## Prerequisites

- Docker with buildx
- Python 3.11+
- AWS CLI configured for Bedrock, AgentCore, ECR, and IAM
- Bedrock model access in the target region
- GitHub PAT with read-only access to this repo
- Slack incoming webhook
- ngrok or equivalent tunnel for local Airflow

See [PLAN.MD](PLAN.MD) for the implementation plan.

## Local Airflow Configuration

Copy `airflow/.env.example` to `airflow/.env` and fill the `AIRFLOW_VAR_*` values. Airflow exposes those env vars as `{{ var.value.* }}`, so the AgentCore setup DAGs do not require manually creating Airflow Variables in the UI.
