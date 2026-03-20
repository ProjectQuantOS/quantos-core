import os
import json
import sqlite3
import urllib.request
import urllib.error

from ledger import init_db, create_run, create_event

API_KEY = os.environ["OPENAI_API_KEY"]
URL = "https://api.openai.com/v1/responses"
DB_PATH = "quantos.db"

ALLOWED_TASKS = [
    "log_planner_output_to_ledger",
    "validate_planner_schema",
    "write_reviewer_stub",
    "write_arbiter_stub",
]


def get_last_events(limit=5):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT type, payload FROM events ORDER BY created_at DESC LIMIT ?",
        (limit,)
    )
    rows = cursor.fetchall()
    conn.close()

    cleaned = []
    for event_type, payload in rows:
        try:
            payload_obj = json.loads(payload)
        except json.JSONDecodeError:
            payload_obj = {"raw_payload": payload}
        cleaned.append((event_type, payload_obj))

    cleaned.reverse()
    return cleaned


def reviewer(task):
    required = {
        "task_name": str,
        "rationale": str,
        "difficulty": int,
        "required_tools": list,
    }

    for key, expected_type in required.items():
        if key not in task:
            return {"approved": False, "reason": f"missing key: {key}"}
        if not isinstance(task[key], expected_type):
            return {"approved": False, "reason": f"wrong type for: {key}"}

    return {"approved": True, "reason": "schema_valid"}


def arbiter(task, review):
    if not review["approved"]:
        return {"approved": False, "reason": "review_failed"}

    if task["task_name"] not in ALLOWED_TASKS:
        return {"approved": False, "reason": "task_not_allowed"}

    return {"approved": True, "reason": "doctrine_aligned"}


init_db()
run_id = create_run()

events = get_last_events(5)

ledger_context = "\n".join(
    f"- {event_type}: {payload}"
    for event_type, payload in events
)

allowed_tasks_text = "\n- ".join(ALLOWED_TASKS)

prompt = f"""You are the planner for QuantOS Genesis-02.

Analyze the recent event ledger below and choose the NEXT logical task.

Recent ledger:
{ledger_context}

You must choose exactly one task_name from this allowed list:
- {allowed_tasks_text}

Rules:
- Pick only from the allowed task_name list.
- Choose the smallest next step.
- Do not invent unrelated domains.
- Return STRICT JSON only.
"""

create_event(run_id, "planner_prompt", {"prompt": prompt})

payload = {
    "model": "gpt-4.1-mini",
    "input": prompt,
    "text": {
        "format": {
            "type": "json_schema",
            "name": "next_task",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "task_name": {
                        "type": "string",
                        "enum": ALLOWED_TASKS
                    },
                    "rationale": {"type": "string"},
                    "difficulty": {"type": "integer"},
                    "required_tools": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["task_name", "rationale", "difficulty", "required_tools"],
                "additionalProperties": False
            }
        }
    }
}

try:
    req = urllib.request.Request(
        URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))

except urllib.error.HTTPError as e:
    create_event(run_id, "api_failure", {
        "error": f"HTTPError: {e.code} {e.reason}"
    })
    create_event(run_id, "run_closed", {"final_status": False})
    print("Planner run failed:", run_id)
    raise SystemExit(1)

except urllib.error.URLError as e:
    create_event(run_id, "api_failure", {
        "error": f"URLError: {e.reason}"
    })
    create_event(run_id, "run_closed", {"final_status": False})
    print("Planner run failed:", run_id)
    raise SystemExit(1)

text = body["output"][0]["content"][0]["text"]

try:
    task = json.loads(text)
except json.JSONDecodeError:
    create_event(run_id, "parsing_failure", {"raw_text": text})
    create_event(run_id, "run_closed", {"final_status": False})
    print("Planner run failed:", run_id)
    raise SystemExit(1)

create_event(run_id, "planner_output", task)

review = reviewer(task)
create_event(run_id, "reviewer_output", review)

arb = arbiter(task, review)
create_event(run_id, "arbiter_output", arb)

create_event(run_id, "run_closed", {"final_status": arb["approved"]})

print("Planner run:", run_id)
print(json.dumps(task, indent=2))
print(json.dumps(review, indent=2))
print(json.dumps(arb, indent=2))
