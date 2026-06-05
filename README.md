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

## AgentCore Execution Role

Create an IAM execution role for AgentCore Runtime with the templates in `infra/iam/`.
Replace `<AWS_REGION>` and `<AWS_ACCOUNT_ID>`, create the role, attach the inline policy, and set the resulting ARN in `AIRFLOW_VAR_AGENTCORE_EXECUTION_ROLE_ARN`.

For the local PoC, `AIRFLOW_VAR_AIRFLOW_BASE_URL` must be a public tunnel URL that AgentCore can reach, such as an ngrok URL for `localhost:8080`.
