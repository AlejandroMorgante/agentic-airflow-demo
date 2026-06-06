from __future__ import annotations

import base64
import logging
import os
from urllib.parse import quote

import requests
from tools.strands_compat import tool

log = logging.getLogger(__name__)

SOURCE_HEAD_CHARS = 6000


def _use_mocks() -> bool:
    return os.environ.get("AGENT_USE_MOCKS", "false").lower() in {"1", "true", "yes"}


def _mock_dag_source(filename: str) -> str:
    log.info("Mock fetch_dag_source filename=%s", filename)
    return '''from airflow.sdk import DAG, task

@task
def extract() -> dict:
    return {"rows": 100, "source": "demo"}

@task
def transform(data: dict) -> dict:
    return {"transformed": data["rowz"]}

@task
def load(data: dict) -> str:
    return f"loaded {data['transformed']}"
'''


def _content_path(filename: str) -> str:
    if "/" in filename:
        return filename.lstrip("/")
    prefix = os.environ.get("GITHUB_DAG_PATH", "airflow/dags").strip("/")
    return f"{prefix}/{filename}"


@tool
def fetch_dag_source(filename: str) -> str:
    """Fetch DAG source from the configured GitHub repository."""
    if _use_mocks():
        return _mock_dag_source(filename)

    try:
        repo = os.environ["GITHUB_REPO"]
        token = os.environ.get("GITHUB_TOKEN")
        ref = os.environ.get("GITHUB_REF", "main")
        path = _content_path(filename)
        encoded_path = quote(path, safe="/")
        url = f"https://api.github.com/repos/{repo}/contents/{encoded_path}"
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        log.info("GitHub GET repo=%s path=%s ref=%s", repo, path, ref)
        resp = requests.get(
            url,
            headers=headers,
            params={"ref": ref},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        content = base64.b64decode(data["content"]).decode("utf-8", errors="replace")
        if len(content) > SOURCE_HEAD_CHARS:
            content = content[:SOURCE_HEAD_CHARS] + "\n...[truncated]..."
        return content
    except Exception as exc:
        log.exception("Failed to fetch DAG source")
        return f"ERROR fetching DAG source {filename}: {exc}"
