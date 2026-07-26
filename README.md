# Agentic Airflow PoC

Local Apache Airflow demo that sends failed DAG context to a BYOC (bring your own
container) agent runtime, asks the agent to diagnose the failure, opens a draft GitHub
PR with the proposed DAG fix, and posts a summary to Slack.

The same end-to-end workflow runs on three cloud backends using operators contributed
upstream to Apache Airflow:

- AWS Bedrock AgentCore Runtime
- Google Vertex AI Agent Engine
- Microsoft Azure AI Foundry Agents

## BYOC Runtime Coverage

| Cloud | BYOC runtime | Airflow operators exercised | Upstream contribution |
| --- | --- | --- | --- |
| AWS | Bedrock AgentCore Runtime | Create, invoke, delete | [apache/airflow#67984](https://github.com/apache/airflow/pull/67984), merged into `main` |
| Google Cloud | Vertex AI Agent Engine | Create, get, query, update, delete | [apache/airflow#68479](https://github.com/apache/airflow/pull/68479), merged into `main` |
| Microsoft Azure | AI Foundry Hosted Agents | Create, update, run, delete | [apache/airflow#68799](https://github.com/apache/airflow/pull/68799), merged into `main` |

Native operators for Amazon Bedrock AgentCore:

- `BedrockCreateAgentRuntimeOperator`
- `BedrockInvokeAgentRuntimeOperator`
- `BedrockDeleteAgentRuntimeOperator`

Native operators for Google Vertex AI Agent Engine:

- `CreateAgentEngineOperator`
- `GetAgentEngineOperator`
- `RunQueryJobOperator`
- `UpdateAgentEngineOperator`
- `DeleteAgentEngineOperator`

Native operators for Microsoft Azure AI Foundry Hosted Agents:

- `CreateAzureAIAgentOperator`
- `UpdateAzureAIAgentOperator`
- `RunAzureAIAgentOperator`
- `DeleteAzureAIAgentOperator`

Each path builds a provider-specific agent container, manages its lifecycle with the
corresponding Airflow provider operators, and runs the same four troubleshooting
failure cases.

Implementation highlights:

- Split the agent image into provider-specific entrypoints:
  - `agent/aws_agentcore/`
  - `agent/gcp_gemini_agent_platform/`
  - `agent/azure_ai_agents/`
- Moved reusable agent logic and tools into `agent/shared/`.
- Moved AWS DAGs under `airflow/dags/aws_agentcore/`.
- Added GCP Agent Engine setup and demo DAGs under
  `airflow/dags/gcp_gemini_agent_platform/`.
- Added Azure AI Foundry Agents setup and demo DAGs under
  `airflow/dags/azure_ai_agents/`.
- Added `scripts/deploy_gcp_agent.sh` to build and push the GCP agent image to
  Artifact Registry.
- Added `scripts/deploy_azure_agent.sh` to build and push the Azure hosted agent
  image to Azure Container Registry.
- Updated the Airflow image to include the Amazon provider, the Google provider with
  Vertex AI Agent Engine operators, the Microsoft Azure provider with AI Foundry
  Agents operators, and the Google Gen AI dependencies.
- Added an Airflow triggerer service for deferrable GCP operators.

## Can Anyone Run This?

Anyone can clone the repo and start the local Airflow stack, but the full end-to-end
demo requires cloud and integration credentials.

To run the full AWS path, you need AWS credentials with Bedrock, AgentCore, ECR, and
IAM access, plus Bedrock model access in the target region.

To run the full GCP path, you need Google Cloud credentials with Vertex AI and
Artifact Registry access, plus Application Default Credentials available locally.

To run the full Azure path, you need an Azure AI Foundry project, a deployed model,
and an Airflow connection for the Foundry project endpoint.

All three paths need:

- A GitHub token with repository contents and pull request permissions.
- A Slack incoming webhook.
- A repository ref that the agent can read and open draft PRs against.

The Azure AI Foundry Agents path exercises the Azure Hosted Agent operators through
the `invocations` protocol. The hosted container owns the tool loop, so the Azure
demos can fetch DAG source, create a draft GitHub PR, and post to Slack.

Without those credentials, Airflow can still start locally, but the setup and
troubleshooting DAGs that call AWS, GCP, Azure, GitHub, or Slack will fail.

## Prerequisites

- Docker with buildx
- Python 3.11+
- AWS CLI, for the AWS AgentCore path
- Google Cloud CLI, for the GCP Agent Engine path
- Azure CLI and Azure AI Foundry project credentials, for the Azure AI Agents path
- GitHub PAT with read/write access to this repo
- Slack incoming webhook

## Repository Layout

```text
agent/
  aws_agentcore/                 AWS runtime entrypoint and requirements
  gcp_gemini_agent_platform/     GCP runtime entrypoint and requirements
  azure_ai_agents/               Azure hosted agent entrypoint and requirements
  shared/                        Shared prompt, troubleshooting flow, and tools

airflow/
  dags/aws_agentcore/            AWS setup and failure demo DAGs
  dags/gcp_gemini_agent_platform/ GCP setup and failure demo DAGs
  dags/azure_ai_agents/          Azure AI Foundry Agents setup and failure demo DAGs
  docker-compose.yml             Local Airflow stack
  Dockerfile                     Airflow image with required providers

scripts/
  deploy_agent.sh                Build and push AWS agent image to ECR
  deploy_gcp_agent.sh            Build and push GCP agent image to Artifact Registry
  deploy_azure_agent.sh          Build and push Azure Hosted Agent image to ACR
```

The Airflow image uses the released Amazon provider `9.30.0`. Google and Azure are
installed from explicit commits associated with
[apache/airflow#68479](https://github.com/apache/airflow/pull/68479) and
[apache/airflow#68799](https://github.com/apache/airflow/pull/68799), respectively.
All three operator contributions are now merged into Apache Airflow's `main` branch;
the Google and Azure commit pins remain explicit for reproducible builds until this
PoC moves to provider releases that include those operators.

## Security Notes

This is a PoC. Runtime secrets are passed as environment variables into the cloud
agent runtime. Do not commit real values to `.env`, `airflow/.env`, or the example
files.

The local compose file mounts cloud CLI config directories into Airflow:

- `${HOME}/.aws:/home/airflow/.aws:ro`
- `${HOME}/.config/gcloud:/home/airflow/.config/gcloud:ro`

These mounts do not publish credentials to git, but containers can use those local
credentials while they are running. This is acceptable for a local demo when you trust
the DAGs and image. For shared or production usage, replace this with dedicated
service accounts, least-privilege permissions, and a managed secret store such as AWS
Secrets Manager or Google Secret Manager.

The failure collector sends a tail of the failed task log to the runtime. If task logs
contain secrets or PII, that data can be sent to the selected model/runtime. Keep demo
logs clean or add redaction before using this pattern with real workloads.

## Local Airflow Setup

Create the Airflow env file:

```bash
cp airflow/.env.example airflow/.env
```

Fill the values you need in `airflow/.env`.

Common values:

```bash
AIRFLOW_VAR_GITHUB_REPO=owner/repo
AIRFLOW_VAR_GITHUB_REF=main
AIRFLOW_VAR_GITHUB_DAG_PATH=airflow/dags
AIRFLOW_VAR_GITHUB_TOKEN=<github-token>
AIRFLOW_VAR_SLACK_WEBHOOK_URL=<slack-webhook-url>
```

AWS values:

```bash
AWS_PROFILE=default
AWS_REGION=us-east-1
AIRFLOW_VAR_AGENT_CONTAINER_URI=<account-id>.dkr.ecr.<region>.amazonaws.com/agentic-airflow-agent:<tag>
AIRFLOW_VAR_AGENTCORE_EXECUTION_ROLE_ARN=<agentcore-execution-role-arn>
AIRFLOW_VAR_BEDROCK_MODEL_ID=us.anthropic.claude-haiku-4-5-20251001-v1:0
```

GCP values:

```bash
AIRFLOW_VAR_GCP_PROJECT_ID=<gcp-project-id>
AIRFLOW_VAR_GCP_REGION=us-central1
AIRFLOW_VAR_GCP_AGENT_ENGINE_CONTAINER_URI=<region>-docker.pkg.dev/<project>/<repository>/airflow-troubleshooting-agent:<tag>
AIRFLOW_VAR_GCP_AGENT_ENGINE_QUERY_OUTPUT_GCS_URI=gs://<gcp-project-id>-agent-engine-query-output/query-output/
AIRFLOW_VAR_GCP_AGENT_ENGINE_DISPLAY_NAME=airflow-agent-engine
AIRFLOW_VAR_GEMINI_MODEL_ID=gemini-2.5-pro
AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=google-cloud-platform://?extra__google_cloud_platform__project=<gcp-project-id>&extra__google_cloud_platform__scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform
```

Azure AI Foundry Agents values:

```bash
AIRFLOW_VAR_AZURE_AI_AGENTS_MODEL_DEPLOYMENT_NAME=gpt-4o
AIRFLOW_VAR_AZURE_AI_AGENTS_CONTAINER_IMAGE=<registry>.azurecr.io/airflow-troubleshooting-agent:<tag>
AIRFLOW_VAR_AZURE_AI_AGENT_NAME=airflow-troubleshooting-agent
AIRFLOW_CONN_AZURE_AI_AGENTS_DEFAULT=azure-ai-agents://<client-id>:<client-secret>@<url-encoded-endpoint>?tenantId=<tenant-id>
```

The Azure connection endpoint format is:

```text
https://<aiservices-id>.services.ai.azure.com/api/projects/<project-name>
```

For the GCP path, run this on the host so the Airflow container can use ADC through
the read-only gcloud config mount:

```bash
gcloud auth application-default login
gcloud config set project <gcp-project-id>
```

Start Airflow:

```bash
docker compose -f airflow/docker-compose.yml up -d --build
```

Open Airflow at:

```text
http://localhost:8080
```

The local demo credentials are `admin` / `admin`.

## AWS AgentCore Path

Create an AgentCore execution role with the templates in `infra/iam/`. Replace
`<AWS_REGION>` and `<AWS_ACCOUNT_ID>`, create the role, attach the inline policy, and
set the resulting ARN in `AIRFLOW_VAR_AGENTCORE_EXECUTION_ROLE_ARN`.

Build and push the AWS agent image:

```bash
scripts/deploy_agent.sh
```

In Airflow, use the AWS setup DAGs:

- `aws_agentcore_create_runtime`
- `aws_agentcore_invoke_runtime`
- `aws_agentcore_delete_runtime`
- `aws_agentcore_full_lifecycle`

If you use the standalone create DAG, copy the created runtime ARN and ID from XCom
or task output into:

```bash
AIRFLOW_VAR_AGENT_RUNTIME_ARN=<agent-runtime-arn>
AIRFLOW_VAR_AGENT_RUNTIME_ID=<agent-runtime-id>
```

Then restart Airflow so the env values are loaded:

```bash
docker compose -f airflow/docker-compose.yml up -d
```

AWS demo DAGs:

- `aws_agentcore_demo_failing_etl`
- `aws_agentcore_demo_schema_contract_etl`
- `aws_agentcore_demo_missing_config_etl`
- `aws_agentcore_demo_sql_schema_etl`

Each demo intentionally fails, collects failure context, invokes AgentCore, and lets
the agent fetch the DAG source from GitHub, create a draft PR, and notify Slack.

## GCP Agent Engine Path

Make sure the required GCP APIs are enabled for your project:

- Vertex AI API
- Artifact Registry API

Create an Artifact Registry Docker repository if you do not already have one:

```bash
gcloud artifacts repositories create airflow-agent-platform \
  --repository-format=docker \
  --location=us-central1
```

Build and push the GCP agent image:

```bash
GCP_PROJECT_ID=<gcp-project-id> scripts/deploy_gcp_agent.sh
```

The script prints the pushed image URI. Put that value in:

```bash
AIRFLOW_VAR_GCP_AGENT_ENGINE_CONTAINER_URI=<printed-image-uri>
AIRFLOW_VAR_GCP_AGENT_ENGINE_QUERY_OUTPUT_GCS_URI=gs://<gcp-project-id>-agent-engine-query-output/query-output/
```

Restart Airflow so the env value is loaded:

```bash
docker compose -f airflow/docker-compose.yml up -d
```

In Airflow, use the GCP setup DAGs:

- `gcp_agentengine_create_runtime`
- `gcp_agentengine_query_runtime`
- `gcp_agentengine_get_runtime`
- `gcp_agentengine_update_runtime`
- `gcp_agentengine_delete_runtime`
- `gcp_agentengine_full_lifecycle`

If you use the standalone create DAG, copy the created Agent Engine name into:

```bash
AIRFLOW_VAR_GCP_AGENT_ENGINE_NAME=projects/<project>/locations/<region>/reasoningEngines/<agent-engine-id>
```

Then restart Airflow.

GCP demo DAGs:

- `gcp_agentengine_demo_failing_etl`
- `gcp_agentengine_demo_schema_contract_etl`
- `gcp_agentengine_demo_missing_config_etl`
- `gcp_agentengine_demo_sql_schema_etl`

Each demo intentionally fails, collects failure context, queries Vertex AI Agent
Engine, and lets the agent fetch the DAG source from GitHub, create a draft PR, and
notify Slack.

## Azure AI Foundry Agents Path

Create an Azure AI Foundry project and deploy a model such as `gpt-4o`. Configure
the `azure_ai_agents_default` Airflow connection with the Foundry project endpoint
and either client secret credentials or a supported Azure default credential.

Build and push the hosted agent container to Azure Container Registry:

```bash
AZURE_CONTAINER_REGISTRY_NAME=<registry-name> scripts/deploy_azure_agent.sh
```

Set the hosted agent image, model deployment name, and agent name:

```bash
AIRFLOW_VAR_AZURE_AI_AGENTS_CONTAINER_IMAGE=<registry>.azurecr.io/airflow-troubleshooting-agent:<tag>
AIRFLOW_VAR_AZURE_AI_AGENTS_MODEL_DEPLOYMENT_NAME=<model-deployment-name>
AIRFLOW_VAR_AZURE_AI_AGENT_NAME=airflow-troubleshooting-agent
```

In Airflow, use the Azure setup DAGs:

- `azure_ai_agents_create_agent`
- `azure_ai_agents_run_agent`
- `azure_ai_agents_update_agent`
- `azure_ai_agents_delete_agent`
- `azure_ai_agents_full_lifecycle`

The hosted agent is addressed by `AIRFLOW_VAR_AZURE_AI_AGENT_NAME`; no Azure
Functions or `AIRFLOW_VAR_AZURE_AI_AGENT_ID` handoff is required.

Azure demo DAGs:

- `azure_ai_agents_demo_failing_etl`
- `azure_ai_agents_demo_schema_contract_etl`
- `azure_ai_agents_demo_missing_config_etl`
- `azure_ai_agents_demo_sql_schema_etl`

Each demo intentionally fails, collects failure context, and invokes the Azure AI
Foundry Hosted Agent through the `invocations` protocol. The container owns the
tool loop, so it can fetch DAG source from GitHub, create a draft PR, and notify
Slack without Azure Functions.

Example observable output from this repository's E2E run:

- Draft PR: [#25](https://github.com/AlejandroMorgante/agentic-airflow-demo/pull/25)
- Slack: notification sent successfully
- Airflow DAG: `azure_ai_agents_full_lifecycle` completed `create`, `update`,
  `run`, and `delete` successfully.

The generated PR is intentionally a draft. It changes the demo typo from
`data["rowz"]` to `data["rows"]`, and must be reviewed by a human before merging.

## Demo Failure Cases

- Bad key lookup: reads `rowz` when the payload contains `rows`.
- Schema contract mismatch: downstream task expects a different shape than upstream
  emits.
- Missing configuration: task expects runtime config that is not present.
- SQL schema mismatch: query references a column that is not present in the staged
  table.

## Local Agent Behavior

The shared troubleshooting flow has two modes:

- `AGENT_USE_MODEL=true`: calls the selected cloud model/runtime and executes tools.
- `AGENT_USE_MOCKS=true`: deterministic local mock path used by tests.

Cloud setup DAGs create runtimes with:

```bash
AGENT_USE_MODEL=true
AGENT_USE_MOCKS=false
```

## Development

Run agent tests locally from the `agent/` directory:

```bash
cd agent
uv run pytest
```

The tests use mocks and do not require AWS, GCP, Azure, GitHub, or Slack credentials.

## Cleanup

Use the delete setup DAGs to remove cloud runtimes:

- `aws_agentcore_delete_runtime`
- `gcp_agentengine_delete_runtime`
- `azure_ai_agents_delete_agent`

Stop the local Airflow stack:

```bash
docker compose -f airflow/docker-compose.yml down
```

Remove local Airflow state, including the Postgres volume:

```bash
docker compose -f airflow/docker-compose.yml down -v
```
