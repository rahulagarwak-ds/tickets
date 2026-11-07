import ast
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import TypedDict
import uuid

from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, START, END
from pydantic import BaseModel, ConfigDict, Field

from .prompts import PROMPTS


class Rule(BaseModel):
    model_config = ConfigDict(extra="forbid")
    column: str
    observation: str
    recommendation: str
    rationale: str


class Proposal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    summary: str = Field(min_length=1)
    grain: str = Field(min_length=1)
    rules: list[Rule] = Field(min_length=1)
    assumptions: list[str]
    code: str = Field(min_length=1, max_length=50000)
    validation_cases: list[str] = Field(min_length=1)


class State(TypedDict):
    agent: str
    context: dict
    proposal: dict
    usage: dict
    response_id: str


def validate_code(agent: str, code: str) -> None:
    if agent == "gold_design":
        body = re.sub(r"\s*CREATE\s+VIEW\s+candidate\.report\s+AS\s+", "", code, count=1, flags=re.I)
        if body == code or not body.lstrip().upper().startswith("SELECT"):
            raise ValueError("Gold output must create candidate.report from SELECT")
        if ";" in body.rstrip().removesuffix(";") or "--" in body or "/*" in body:
            raise ValueError("Gold output must contain a single uncommented statement")
    else:
        tree = ast.parse(code)
        if not any(isinstance(node, ast.FunctionDef) and node.name == "transform" for node in tree.body):
            raise ValueError("DQ output requires transform(df)")
        if any(not isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef)) for node in tree.body):
            raise ValueError("DQ module may contain imports and functions only")
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
            else:
                modules = []
            if any(module.split('.')[0] not in {"pyspark", "decimal", "datetime", "re"} for module in modules):
                raise ValueError("Unexpected import in DQ candidate")
            if isinstance(node, ast.Name) and node.id in {"exec", "eval", "open", "__import__", "compile"}:
                raise ValueError("Executable I/O is outside the DQ component contract")
            if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
                raise ValueError("Dunder access is outside the DQ component contract")


def build_workflow(model):
    def generate(state):
        response = model.with_structured_output(Proposal, method="json_schema", strict=True, include_raw=True).invoke([
            ("system", PROMPTS[state["agent"]]),
            ("human", json.dumps(state["context"], default=str)),
        ])
        if response.get("parsing_error") or response.get("parsed") is None:
            raise ValueError("Model output failed the proposal schema")
        proposal = Proposal.model_validate(response["parsed"])
        validate_code(state["agent"], proposal.code)
        raw = response.get("raw")
        return {"proposal": proposal.model_dump(), "usage": getattr(raw, "usage_metadata", None) or {},
                "response_id": getattr(raw, "id", "") or ""}

    graph = StateGraph(State)
    graph.add_node("data_quality", generate)
    graph.add_node("gold_design", generate)
    graph.add_conditional_edges(START, lambda state: state["agent"], {name: name for name in PROMPTS})
    for name in PROMPTS:
        graph.add_edge(name, END)
    return graph.compile()


def fingerprint(directory: Path) -> str:
    digest = hashlib.sha256()
    for name in ("context.json", "proposal.json", "manifest.json", "candidate.py", "candidate.sql"):
        path = directory / name
        if path.exists():
            digest.update(name.encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


def generate_run(agent: str, context: dict, output: Path, model=None) -> Path:
    if agent not in PROMPTS:
        raise ValueError("Unknown agent")
    if model is None and not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("Configure OPENAI_API_KEY in the shell before generation")
    live = model is None
    model_name = os.environ.get("LLM_MODEL", "gpt-6-luna")
    model = model if model is not None else ChatOpenAI(
        model=model_name, use_responses_api=True, reasoning_effort="low",
        timeout=120, max_retries=1, max_completion_tokens=8000,
    )
    directory = output / uuid.uuid4().hex
    directory.mkdir(parents=True, exist_ok=False)
    context = dict(context, run_id=directory.name, system_prompt=PROMPTS[agent])
    (directory / "context.json").write_text(json.dumps(context, indent=2, default=str))
    start = time.monotonic()
    trace = {"run_id": directory.name, "agent": agent, "model": model_name,
             "source": "openai" if live else "test_double",
             "started_at": datetime.now(timezone.utc).isoformat(),
             "prompt_sha256": hashlib.sha256(PROMPTS[agent].encode()).hexdigest()}
    try:
        result = build_workflow(model).invoke({"agent": agent, "context": context})
        proposal = result["proposal"]
        (directory / "proposal.json").write_text(json.dumps(proposal, indent=2))
        artifact = "candidate.py" if agent == "data_quality" else "candidate.sql"
        (directory / artifact).write_text(proposal["code"])
        manifest = {"stage": "silver" if agent == "data_quality" else "gold", "order": 2 if agent == "data_quality" else 3,
                    "entrypoint": artifact + (":transform" if agent == "data_quality" else ""),
                    "inputs": ["bronze.raw_tickets" if agent == "data_quality" else "silver.tickets"],
                    "outputs": ["candidate.cost_sla" if agent == "data_quality" else "candidate.report"],
                    "status": "pending_human_review"}
        (directory / "manifest.json").write_text(json.dumps(manifest, indent=2))
        trace.update(status="pending_human_review", usage=result["usage"], response_id=result["response_id"],
                     fingerprint=fingerprint(directory))
    except Exception as error:
        trace.update(status="failed", error_type=type(error).__name__)
        raise
    finally:
        trace["elapsed_seconds"] = round(time.monotonic() - start, 3)
        (directory / "trace.json").write_text(json.dumps(trace, indent=2))
    return directory


def approve_run(directory: Path, reviewer: str) -> None:
    if not reviewer.strip():
        raise ValueError("Reviewer name is required")
    trace = json.loads((directory / "trace.json").read_text())
    if trace["status"] != "pending_human_review" or fingerprint(directory) != trace["fingerprint"]:
        raise ValueError("Candidate is incomplete or modified; generate a revision before approval")
    path = directory / "approval.json"
    with path.open("x") as stream:
        json.dump({"reviewer": reviewer, "fingerprint": fingerprint(directory),
                   "approved_at": datetime.now(timezone.utc).isoformat()}, stream, indent=2)
