from __future__ import annotations

import base64
import ast
import hashlib
import logging
import os
import re
from typing import Any
from urllib.parse import quote

import requests
from shared.tools.strands_compat import tool

log = logging.getLogger(__name__)

SOURCE_HEAD_CHARS = 6000
DEFAULT_DAG_PATH = "airflow/dags"
MAX_FIXED_CONTENT_CHARS = 100_000


def _use_mocks() -> bool:
    return (
        os.environ.get("AIRFLOW_AGENT_USE_MOCKS") or os.environ.get("AGENT_USE_MOCKS", "false")
    ).lower() in {"1", "true", "yes"}


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


def _github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _mock_create_github_pr(filename: str, fixed_content: str, pr_title: str, pr_body: str) -> dict[str, Any]:
    log.info("Mock create_github_pr filename=%s", filename)
    return {
        "pr_url": "https://github.com/owner/repo/pull/1",
        "pr_number": 1,
        "branch": "agent/fix-mock",
        "mock": True,
    }


def _content_path(filename: str) -> str:
    prefix = os.environ.get("GITHUB_DAG_PATH", DEFAULT_DAG_PATH).strip("/")
    cleaned = filename.strip().lstrip("/")
    if not cleaned or cleaned.endswith("/"):
        raise ValueError("filename must point to a DAG file")
    if ".." in cleaned.split("/"):
        raise ValueError("filename must not contain parent directory segments")
    if any(part.startswith(".") for part in cleaned.split("/")):
        raise ValueError(f"filename must be under {prefix}")
    if cleaned.startswith(f"{prefix}/") or cleaned == prefix:
        return cleaned
    return f"{prefix}/{cleaned}"


def _validate_dag_update(path: str, fixed_content: str) -> None:
    if not path.endswith(".py"):
        raise ValueError("create_github_pr can only update Python DAG files")
    max_chars = int(os.environ.get("GITHUB_MAX_FIXED_CONTENT_CHARS", str(MAX_FIXED_CONTENT_CHARS)))
    if len(fixed_content) > max_chars:
        raise ValueError(f"fixed_content is too large; max is {max_chars} characters")
    ast.parse(fixed_content, filename=path)


def _branch_name(filename: str, fixed_content: str) -> str:
    stem = filename.removesuffix(".py").strip().strip("/")
    stem = re.sub(r"[^A-Za-z0-9._/-]+", "-", stem)
    stem = stem.replace("/", "-").strip("-") or "dag"
    digest = hashlib.sha256(fixed_content.encode()).hexdigest()[:12]
    return f"agent/fix-{stem}-{digest}"


def _delete_branch(base_url: str, headers: dict[str, str], branch_name: str) -> None:
    resp = requests.delete(
        f"{base_url}/git/refs/heads/{quote(branch_name, safe='/')}",
        headers=headers,
        timeout=15,
    )
    if resp.status_code not in {204, 404}:
        resp.raise_for_status()


@tool
def fetch_dag_source(filename: str) -> str:
    """Fetch DAG source from the configured GitHub repository."""
    if _use_mocks():
        return _mock_dag_source(filename)

    try:
        repo = os.environ["GITHUB_REPO"]
        ref = os.environ.get("GITHUB_REF", "main")
        path = _content_path(filename)
        encoded_path = quote(path, safe="/")
        url = f"https://api.github.com/repos/{repo}/contents/{encoded_path}"
        log.info("GitHub GET repo=%s path=%s ref=%s", repo, path, ref)
        resp = requests.get(
            url,
            headers=_github_headers(),
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


@tool
def create_github_pr(
    filename: str,
    fixed_content: str,
    pr_title: str,
    pr_body: str,
) -> dict[str, Any]:
    """Create a draft GitHub PR with the agent-suggested fix for a failing DAG.

    Args:
        filename: DAG filename (e.g. 'demo_failing_etl.py').
        fixed_content: Full corrected content of the DAG file.
        pr_title: Title for the pull request.
        pr_body: Description body for the pull request (markdown).

    Returns:
        Dict with pr_url, pr_number, and branch name, or an error key on failure.
    """
    if _use_mocks():
        return _mock_create_github_pr(filename, fixed_content, pr_title, pr_body)

    try:
        repo = os.environ["GITHUB_REPO"]
        ref = os.environ.get("GITHUB_REF", "main")
        path = _content_path(filename)
        _validate_dag_update(path, fixed_content)
        encoded_path = quote(path, safe="/")
        base_url = f"https://api.github.com/repos/{repo}"
        headers = _github_headers()
        owner = repo.split("/", 1)[0]
        branch_name = _branch_name(filename, fixed_content)
        branch_created = False

        # Return the existing draft/open PR for the same generated fix instead of spamming retries.
        existing_prs_resp = requests.get(
            f"{base_url}/pulls",
            headers=headers,
            params={"state": "open", "head": f"{owner}:{branch_name}", "base": ref},
            timeout=15,
        )
        existing_prs_resp.raise_for_status()
        existing_prs = existing_prs_resp.json()
        if existing_prs:
            pr_data = existing_prs[0]
            return {
                "pr_url": pr_data["html_url"],
                "pr_number": pr_data["number"],
                "branch": branch_name,
                "existing": True,
            }

        # 1. Get base branch HEAD commit SHA (needed to create the new branch)
        ref_resp = requests.get(
            f"{base_url}/git/ref/heads/{ref}",
            headers=headers,
            timeout=15,
        )
        ref_resp.raise_for_status()
        head_sha = ref_resp.json()["object"]["sha"]

        # 2. Create fix branch. A 422 means an earlier retry already created it.
        create_branch_resp = requests.post(
            f"{base_url}/git/refs",
            headers=headers,
            json={"ref": f"refs/heads/{branch_name}", "sha": head_sha},
            timeout=15,
        )
        if create_branch_resp.status_code == 422:
            log.info("GitHub branch already exists: %s", branch_name)
        else:
            create_branch_resp.raise_for_status()
            branch_created = True

        # 3. Get current file SHA on the fix branch (needed for the update PUT)
        file_resp = requests.get(
            f"{base_url}/contents/{encoded_path}",
            headers=headers,
            params={"ref": branch_name},
            timeout=15,
        )
        file_resp.raise_for_status()
        file_sha = file_resp.json()["sha"]

        # 4. Commit the fixed file onto the new branch
        requests.put(
            f"{base_url}/contents/{encoded_path}",
            headers=headers,
            json={
                "message": f"fix: agent-suggested fix for {filename}",
                "content": base64.b64encode(fixed_content.encode()).decode(),
                "sha": file_sha,
                "branch": branch_name,
            },
            timeout=15,
        ).raise_for_status()

        # 5. Open a draft PR so a human reviews before merging
        pr_resp = requests.post(
            f"{base_url}/pulls",
            headers=headers,
            json={
                "title": pr_title,
                "body": pr_body,
                "head": branch_name,
                "base": ref,
                "draft": True,
            },
            timeout=15,
        )
        pr_resp.raise_for_status()
        pr_data = pr_resp.json()
        log.info("Draft PR created: %s", pr_data["html_url"])
        return {
            "pr_url": pr_data["html_url"],
            "pr_number": pr_data["number"],
            "branch": branch_name,
        }
    except Exception as exc:
        log.exception("Failed to create GitHub PR")
        if "base_url" in locals() and "headers" in locals() and branch_created:
            try:
                _delete_branch(base_url, headers, branch_name)
            except Exception:
                log.exception("Failed to clean up GitHub branch %s", branch_name)
        return {"error": str(exc)}
