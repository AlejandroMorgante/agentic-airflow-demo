# Agentic Airflow PoC

Apache Airflow local invoking an Amazon Bedrock AgentCore Runtime to troubleshoot failed DAG tasks, open a draft GitHub PR with the suggested fix, and post findings to Slack.

## Prerequisites

- Docker with buildx
- Python 3.11+
- AWS CLI configured for Bedrock, AgentCore, ECR, and IAM
- Bedrock model access in the target region
- GitHub PAT with read and write access to this repo (Contents + Pull requests)
- Slack incoming webhook

## Local Airflow Configuration

Copy `airflow/.env.example` to `airflow/.env` and fill the `AIRFLOW_VAR_*` values. Airflow exposes those env vars as `{{ var.value.* }}`, so the AgentCore setup DAGs do not require manually creating Airflow Variables in the UI.

The local Airflow instance only calls AgentCore. The failed task metadata and log excerpt are sent in the invoke payload, so the AgentCore runtime does not need network access back into local Airflow.

## AgentCore Execution Role

Create an IAM execution role for AgentCore Runtime with the templates in `infra/iam/`.
Replace `<AWS_REGION>` and `<AWS_ACCOUNT_ID>`, create the role, attach the inline policy, and set the resulting ARN in `AIRFLOW_VAR_AGENTCORE_EXECUTION_ROLE_ARN`.

For this PoC, the AgentCore runtime uses GitHub for DAG source and Slack for the report. Airflow is not exposed publicly.
