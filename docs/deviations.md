# Deviations

## Local deterministic agent path

The plan describes a local agent phase with mocked tools and a Strands/Bedrock agent. The implementation keeps that path available with `AGENT_USE_MODEL=true`, but defaults to a deterministic local flow when the model is not enabled. This makes local tests independent from AWS credentials while preserving the same tool order.

## Docker runtime smoke not executed

`docker compose config` was validated, but image build and Airflow runtime smoke tests were not executed because the Docker daemon was not running on this machine:

```text
Cannot connect to the Docker daemon at unix:///Users/ale/.docker/run/docker.sock. Is the docker daemon running?
```

Run the Airflow smoke checks once Docker is available.
