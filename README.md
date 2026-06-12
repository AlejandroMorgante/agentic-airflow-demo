# Agentic Airflow PoC

Apache Airflow local invoking either an Amazon Bedrock AgentCore Runtime or a Google Vertex AI Agent Engine runtime to troubleshoot failed Dag tasks, open a draft GitHub PR with the suggested fix, and post findings to Slack.

## Prerequisites

- Docker with buildx
- Python 3.11+
- AWS CLI configured for Bedrock, AgentCore, ECR, and IAM
- Google Cloud CLI configured for Vertex AI, Artifact Registry, and ADC
- Bedrock model access in the target region
- GitHub PAT with read and write access to this repo (Contents + Pull requests)
- Slack incoming webhook

## Local Airflow Configuration

Copy `airflow/.env.example` to `airflow/.env` and fill the `AIRFLOW_VAR_*` values. Airflow exposes those env vars as `{{ var.value.* }}`, so the AgentCore and Vertex AI setup Dag files do not require manually creating Airflow Variables in the UI.

For the GCP path, point `AIRFLOW_GOOGLE_PROVIDER_SRC` at the local Airflow checkout that contains the new Vertex AI Agent Engine operators, and make sure `gcloud auth application-default login` has been run so the container can use ADC via the mounted `~/.config/gcloud` directory.

The local Airflow instance only calls AgentCore. The failed task metadata and log excerpt are sent in the invoke payload, so the AgentCore runtime does not need network access back into local Airflow.

## Demo Dags

- `demo_failing_etl`: simple bad key lookup.
- `demo_schema_contract_etl`: upstream/downstream task payload contract mismatch.
- `demo_missing_config_etl`: missing runtime configuration.
- `demo_sql_schema_etl`: SQL references a column that is not present in the staged table.

The GCP Dag files mirror the same failure cases with the Vertex AI Agent Engine query operator.

## AgentCore Execution Role

Create an IAM execution role for AgentCore Runtime with the templates in `infra/iam/`.
Replace `<AWS_REGION>` and `<AWS_ACCOUNT_ID>`, create the role, attach the inline policy, and set the resulting ARN in `AIRFLOW_VAR_AGENTCORE_EXECUTION_ROLE_ARN`.

For this PoC, the AgentCore runtime uses GitHub for Dag source and Slack for the report. Airflow is not exposed publicly.
