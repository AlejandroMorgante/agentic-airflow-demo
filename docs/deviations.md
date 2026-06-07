# Deviations

## Local deterministic agent path

The plan describes a local agent phase with mocked tools and a Strands/Bedrock agent. The implementation keeps that path available with `AGENT_USE_MODEL=true`, but defaults to a deterministic local flow when the model is not enabled. This makes local tests independent from AWS credentials while preserving the same tool order.

## Airflow local smoke executed after Docker startup

Initial validation stopped because Docker Desktop was not running. After Docker was started, the Airflow image built successfully, the Amazon provider installed from the PR merge commit, the AgentCore operators imported successfully, and `smoke_dummy` completed with both tasks in `success`.

During provider installation, pip reported that `aiobotocore 2.24.1` requires `botocore<1.39.12`, while the provider from the PR installed `botocore 1.43.24`. This does not block the synchronous `BedrockInvokeAgentRuntimeOperator` path used by the demo, but it may affect deferrable operators. Keep `_agentcore_setup.py` non-deferrable unless this dependency set is reconciled.
