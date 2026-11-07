"""Repeatable offline integration test. Uses a simulated OpenAI endpoint."""
import json
import io
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from langchain_openai import ChatOpenAI
import psycopg

from ..pipeline.config import DATABASE_CONFIG
from .context import build_context
from . import cli
from .evaluate import evaluate_run
from .workflow import Proposal, generate_run, validate_code


def main() -> None:
    examples = Path(__file__).with_name("examples")
    with psycopg.connect(**DATABASE_CONFIG) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        context = build_context(connection, "data_quality", "Generate cost/SLA cleaning with reasons")
    assert context["profile"]["rows"] == 10280
    assert context["profile"]["columns"]["cost"]["numeric"]["negative"] == 195
    assert context["profile"]["columns"]["sla_hours"]["numeric"]["iqr_outliers"] == 557
    requests = []
    bad_sql = False

    def respond(request):
        payload = json.loads(request.content)
        assert request.url.path == "/v1/responses"
        assert payload["model"] == "gpt-6-luna"
        assert payload["text"]["format"]["type"] == "json_schema"
        assert all(status in json.dumps(payload) for status in ("in_progress", "pending_vendor")), "canonical silver statuses must reach the model"
        requests.append(payload)
        dq = "Data Quality agent" in json.dumps(payload)
        code = (examples / ("cost_sla.py" if dq else "building_report.sql")).read_text()
        if bad_sql and not dq:
            code = code.replace("COUNT(cost)", "COUNT(*)")
        proposal = Proposal(
            summary="Handwritten integration-test output from simulated endpoint",
            grain="One row per input ticket" if dq else "One row per building",
            rules=[{"column": "cost", "observation": "195 negative costs in profile",
                    "recommendation": "Flag negative costs; retain recorded zero",
                    "rationale": "Avoid treating missing/invalid amounts as recorded zero"}],
            assumptions=["Currency and SLA sentinel meanings remain unconfirmed"],
            code=code,
            validation_cases=["zero and missing cost", "unknown statuses and groups"],
        )
        return httpx.Response(200, json={
            "id": "resp_simulated", "object": "response", "created_at": 1,
            "status": "completed", "model": "gpt-6-luna",
            "output": [{"type": "message", "id": "msg_simulated", "role": "assistant", "status": "completed",
                        "content": [{"type": "output_text", "text": proposal.model_dump_json(), "annotations": []}]}],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
        })

    with TemporaryDirectory() as temporary, httpx.Client(transport=httpx.MockTransport(respond)) as client:
        model = ChatOpenAI(model="gpt-6-luna", api_key="test-only", use_responses_api=True, http_client=client)
        for agent in ("data_quality", "gold_design"):
            output = io.StringIO()
            with patch("sys.argv", ["agents", "propose", agent, "--instruction", "Generate the approved component with evidence"]), \
                 patch.object(cli, "generate_run", side_effect=lambda a, c, _out: generate_run(a, c, Path(temporary), model)), redirect_stdout(output):
                assert cli.main() == 0
            directory = Path(output.getvalue().strip())
            assert json.loads((directory / "trace.json").read_text())["source"] == "test_double"
            try:
                evaluate_run(directory)
            except FileNotFoundError:
                pass
            else:
                raise AssertionError("Unapproved code was evaluated")
            with patch("sys.argv", ["agents", "approve", str(directory), "--reviewer", "automated fixture approval, not a human live review"]), redirect_stdout(io.StringIO()):
                assert cli.main() == 0
            output = io.StringIO()
            with patch("sys.argv", ["agents", "evaluate", str(directory)]), redirect_stdout(output):
                assert cli.main() == 0
            result = json.loads(output.getvalue())
            assert result["passed"]
            print(f"PASS {agent}: real LangGraph/LangChain/Responses serialization, proposal artifacts, approval, executable evaluation", flush=True)
            artifact = directory / ("candidate.py" if agent == "data_quality" else "candidate.sql")
            original = artifact.read_text()
            artifact.write_text(original + "\n")
            try:
                evaluate_run(directory)
            except ValueError:
                pass
            else:
                raise AssertionError("Modified approval was accepted")
            print(f"PASS {agent}: changed artifacts invalidate approval", flush=True)

        revision = generate_run("data_quality", dict(context, feedback="Keep zero costs", previous_proposal={"summary": "old draft"}), Path(temporary), model)
        packet = json.loads((revision / "context.json").read_text())
        assert packet["feedback"] == "Keep zero costs" and packet["previous_proposal"]
        bad_sql = True
        broken = generate_run("gold_design", dict(context, agent="gold_design"), Path(temporary), model)
        from .workflow import approve_run
        approve_run(broken, "automated negative-test fixture")
        try:
            evaluate_run(broken)
        except AssertionError:
            assert json.loads((broken / "evaluation.json").read_text())["passed"] is False
        else:
            raise AssertionError("Incorrect cost coverage passed evaluation")
        print("PASS evaluator rejects an approved but semantically wrong SQL candidate", flush=True)
    for agent, code in [("data_quality", "import os\ndef transform(df): return df"),
                        ("gold_design", "CREATE VIEW candidate.report AS SELECT 1"),
                        ("gold_design", "CREATE MATERIALIZED VIEW candidate.report AS SELECT 1; DROP TABLE silver.tickets")]:
        try:
            validate_code(agent, code)
        except ValueError:
            pass
        else:
            raise AssertionError("Invalid candidate was accepted")
    assert len(requests) == 4
    print("PASS profile evidence, revision feedback, import/statement checks. Temporary run artifacts removed.")
    print("OFFLINE TEST ONLY: endpoint replies were handwritten; no live Luna generation or usefulness measurement.")


if __name__ == "__main__":
    main()
