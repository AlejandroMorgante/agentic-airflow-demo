from __future__ import annotations

import json
import logging
import os

import azure.functions as func

from shared.tools.github_api import create_github_pr as _create_github_pr
from shared.tools.github_api import fetch_dag_source as _fetch_dag_source
from shared.tools.slack import post_to_slack as _post_to_slack

logging.basicConfig(level=logging.INFO)

app = func.FunctionApp()


@app.queue_trigger(
    arg_name="msg",
    queue_name="fetch-dag-source-input",
    connection="AzureWebJobsStorage",
)
@app.queue_output(
    arg_name="out",
    queue_name="fetch-dag-source-output",
    connection="AzureWebJobsStorage",
)
def fetch_dag_source(msg: func.QueueMessage, out: func.Out[str]) -> None:
    args = json.loads(msg.get_body().decode())
    result = _fetch_dag_source(**args)
    out.set(json.dumps({"result": result}))


@app.queue_trigger(
    arg_name="msg",
    queue_name="create-github-pr-input",
    connection="AzureWebJobsStorage",
)
@app.queue_output(
    arg_name="out",
    queue_name="create-github-pr-output",
    connection="AzureWebJobsStorage",
)
def create_github_pr(msg: func.QueueMessage, out: func.Out[str]) -> None:
    args = json.loads(msg.get_body().decode())
    result = _create_github_pr(**args)
    out.set(json.dumps(result))


@app.queue_trigger(
    arg_name="msg",
    queue_name="post-to-slack-input",
    connection="AzureWebJobsStorage",
)
@app.queue_output(
    arg_name="out",
    queue_name="post-to-slack-output",
    connection="AzureWebJobsStorage",
)
def post_to_slack(msg: func.QueueMessage, out: func.Out[str]) -> None:
    args = json.loads(msg.get_body().decode())
    result = _post_to_slack(**args)
    out.set(json.dumps(result))
