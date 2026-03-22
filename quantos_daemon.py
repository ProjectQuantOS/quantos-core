"""
QuantOS Daemon — Genesis-04 Perpetual Orchestrator (v2.1 FINAL)
Fuses Planner (OpenAI o4-mini) + Executor (Anthropic Claude + Gemini) + Healing + Idempotency + Circuit Breaker
Constitutional Council: Quant (Mind) | Claude (Coder) | Gemini (Arbiter)
Runs as systemd service. No stdin. Controlled via HALT_LOCK.flag.
Economic constraints: €1,200 Tuition Capital. Prompt Caching mandatory. Tier routing enforced.
Escalation: Opus receives clean-slate original code, never degraded healing attempts.
"""

import json
import os
import sqlite3
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request

from ledger import (
    init_db,
    create_run,
    create_event,
    compute_file_hash,
    register_artifact,
)

# ── DAEMON IDENTITY ──────────────────────────────────────────────────────────
DAEMON_VERSION = "genesis-04-v2.1"

# ── PATHS (absolute — systemd WorkingDirectory is fragile) ───────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HALT_LOCK_FILE = os.path.join(_SCRIPT_DIR, "HALT_LOCK.flag")

# DB_PATH kept relative so it matches ledger.py's module-level constant.
# os.chdir(_SCRIPT_DIR) at __main__ startup anchors both to the same file.
DB_PATH = "quantos.db"

# ── ENDPOINTS ────────────────────────────────────────────────────────────────
PLANNER_URL = "https://api.openai.com/v1/responses"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

# ── LOOP CONTROLS ────────────────────────────────────────────────────────────
MAX_RETRIES = 3
MAX_CONSECUTIVE_LOOPS = 5
MAX_PLANNER_REJECTS = 5

# ── CREDENTIALS (loaded at runtime — NOT at import time) ──────────────────────
# Initialised to None so the process can start, reach the HALT_LOCK check,
# and enter sleep mode even when secrets are absent from the environment.
# Populated by load_runtime_config() inside the daemon loop after the halt-lock
# guard, before any API call is attempted.
OPENAI_API_KEY = None
ANTHROPIC_API_KEY = None
GEMINI_API_KEY = None
TOOL_SERVER_KEY = None

# ── MODEL ROUTING (parameterized — cognitive cost tiers enforced) ─────────────
PLANNER_MODEL = os.environ.get("PLANNER_MODEL", "o4-mini")              # Tier 2
CODER_MODEL = os.environ.get("CODER_MODEL", "claude-sonnet-4-6")        # Tier 3
ESCALATION_CODER = os.environ.get("ESCALATION_CODER", "claude-opus-4-6")  # Tier 4
REVIEWER_MODEL = os.environ.get("REVIEWER_MODEL", "gemini-3-flash-preview")  # Tier 2
TOOL_SERVER_URL = os.environ.get("TOOL_SERVER_URL", "http://127.0.0.1:8000")

# ── PLANNER DOCTRINE — Genesis-04: executable Python tasks only ───────────────
ALLOWED_TASKS = [
    "write_reviewer_stub",
    "write_arbiter_stub",
]

# ── TARGET FILE FALLBACKS ─────────────────────────────────────────────────────
TARGET_FILE_FALLBACKS = {
    "write_reviewer_stub": "reviewer.py",
    "write_arbiter_stub": "arbiter.py",
}


# ─────────────────────────────────────────────────────────────────────────────
# RUNTIME CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

def load_runtime_config():
    """
    Read and validate required credentials from the environment.

    Called inside the daemon loop AFTER the HALT_LOCK check, so the process
    can always start and reach sleep mode even when secrets are missing.
    Uses global assignment so all helper functions (send_planner, anthropic_text,
    gemini_text, fs_write, run_tests) continue to reference the module-level
    names without signature changes.

    Raises EnvironmentError listing every missing variable if any are absent.
    """
    global OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, TOOL_SERVER_KEY

    required = (
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "GEMINI_API_KEY",
        "TOOL_SERVER_KEY",
    )
    missing = [k for k in required if not os.environ.get(k, "").strip()]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
    GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
    TOOL_SERVER_KEY = os.environ["TOOL_SERVER_KEY"]


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY
# ─────────────────────────────────────────────────────────────────────────────

def safe_json_loads(text):
    """Parse JSON, raising ValueError with context on failure."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON parse error: {e} | raw: {text[:200]}")


def strip_code_fences(text):
    """Remove markdown code fences from LLM output."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
    return text.strip() + "\n"


def format_http_error(e):
    """Extract status, reason, and body from an HTTPError for ledger logging."""
    try:
        body = e.read().decode("utf-8", errors="replace")
    except Exception:
        body = ""
    return {
        "error": f"HTTPError: {e.code} {e.reason}",
        "body": body[:500],
    }


# ─────────────────────────────────────────────────────────────────────────────
# IDEMPOTENCY
# ─────────────────────────────────────────────────────────────────────────────

def compute_intent_hash(task_name, rationale):
    """Stable SHA-256 fingerprint of task intent — the idempotency key."""
    import hashlib
    basis = f"{task_name}::{(rationale or '').strip()}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()



def intent_already_executed_successfully(cursor, intent_hash):
    """
    Hard skip gate — the ONLY condition that prevents re-execution.
    Returns True if this exact intent was previously executed and its run
    closed with final_status=True.
    """
    cursor.execute(
        """
        SELECT 1 FROM events AS et
        JOIN events AS rc ON et.run_id = rc.run_id
        WHERE et.type = 'executor_task'
          AND json_extract(et.payload, '$.intent_hash') = ?
          AND rc.type = 'run_closed'
          AND json_extract(rc.payload, '$.final_status') = 1
        LIMIT 1
        """,
        (intent_hash,),
    )
    return cursor.fetchone() is not None


def is_task_completed(target_file):
    """
    Telemetry helper only — NOT a skip gate.

    Returns True if this file was previously produced in a successful run.
    CRITICAL: Do NOT use the return value to skip execution. Using file
    existence as a skip gate causes the 'code-freeze paradox' where the
    daemon refuses to ever update a file it already created.
    The hard skip gate is intent_already_executed_successfully() exclusively.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT 1 FROM events AS fc
            WHERE fc.type = 'file_content_hash'
              AND json_extract(fc.payload, '$.file') = ?
              AND EXISTS (
                SELECT 1 FROM events rc
                WHERE rc.run_id = fc.run_id
                  AND rc.type = 'run_closed'
                  AND json_extract(rc.payload, '$.final_status') = 1
              )
            LIMIT 1
            """,
            (target_file,),
        )
        return cursor.fetchone() is not None
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# TARGET RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def resolve_target_file(task):
    """Map a task dict (or task_name string) to a target filename."""
    if isinstance(task, dict):
        if task.get("target_file"):
            return task["target_file"]
        task_name = task["task_name"]
    else:
        task_name = task
    if task_name in TARGET_FILE_FALLBACKS:
        return TARGET_FILE_FALLBACKS[task_name]
    raise ValueError(f"unsupported task_name: {task_name}")


# ─────────────────────────────────────────────────────────────────────────────
# LEDGER CONTEXT — strategic macro-milestones only (Goldfish Memory fix)
# ─────────────────────────────────────────────────────────────────────────────

def get_macro_context(limit=10):
    """
    Fetch macro-milestones for Planner context — NOT micro-execution exhaust.

    The Planner needs to see its own previous intentions (planner_output),
    whether the body succeeded (run_closed), and any system-level events
    (daemon_boot, api_failure, circuit_breaker_tripped, daemon_error).

    Without this filter, fetching the last N events returns only the tail-end
    mechanical events of the previous cycle (tool_call_response, etc.),
    causing 'Goldfish Memory' — the Planner loses all strategic awareness
    after a single loop and cannot see what task was just completed. Fatal
    in a perpetual daemon.

    CRITICAL: The try/except JSONDecodeError fallback prevents the
    'Fractal Escaping Virus' (exponential backslash propagation when
    serialized payloads are re-embedded in prompt strings).
    Do NOT simplify or remove the deserialization step.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT type, payload FROM events
        WHERE type IN (
            'daemon_boot',
            'planner_output',
            'skipped_task',
            'run_closed',
            'circuit_breaker_tripped',
            'api_failure',
            'daemon_error'
        )
        ORDER BY created_at DESC LIMIT ?
        """,
        (limit,),
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


# ─────────────────────────────────────────────────────────────────────────────
# PLANNER PHASE — Tier 2: o4-mini via OpenAI Responses API
# ─────────────────────────────────────────────────────────────────────────────

def reviewer(task):
    """Local schema validation of planner output — NOT the Gemini code reviewer."""
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
    """Local doctrine alignment check — task_name must be in ALLOWED_TASKS."""
    if not review["approved"]:
        return {"approved": False, "reason": "review_failed"}
    if task["task_name"] not in ALLOWED_TASKS:
        return {"approved": False, "reason": "task_not_allowed"}
    return {"approved": True, "reason": "doctrine_aligned"}


def send_planner(prompt):
    """
    Invoke o4-mini via OpenAI Responses API with strict JSON schema enforcement.

    Endpoint: https://api.openai.com/v1/responses  (NOT /v1/chat/completions)
    The Responses API 'text.format' field enforces the schema at the model
    level — o4-mini's latent reasoning guarantees adherence. gpt-4.1-mini
    (conversational) hallucinates schemas and must NOT be substituted here.

    Raises HTTPError on failure; the daemon loop's backoff handler captures
    the x-request-id header for diagnostics.
    """
    payload = {
        "model": PLANNER_MODEL,
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
                            "enum": ALLOWED_TASKS,
                        },
                        "target_file": {
                            "type": "string",
                            "pattern": r"\.py$",
                        },
                        "rationale": {"type": "string"},
                        "difficulty": {"type": "integer"},
                        "required_tools": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": [
                        "task_name",
                        "target_file",
                        "rationale",
                        "difficulty",
                        "required_tools",
                    ],
                    "additionalProperties": False,
                },
            }
        },
    }

    req = urllib.request.Request(
        PLANNER_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read().decode("utf-8"))

    # ── Resilient response parsing ────────────────────────────────────────────
    # The Responses API shape varies across model versions. Try the most direct
    # path first, then scan the output array defensively.
    text = None

    # 1. Top-level output_text shortcut (present on some response shapes)
    candidate = body.get("output_text", "")
    if candidate and candidate.strip():
        text = candidate.strip()

    # 2. Scan output array for content blocks
    if text is None:
        for item in body.get("output", []):
            for block in item.get("content", []):
                block_text = block.get("text", "")
                if not block_text:
                    continue
                block_type = block.get("type", "")
                # Prefer typed output_text / text blocks; accept any with text
                if block_type in ("output_text", "text") or text is None:
                    text = block_text
                    if block_type in ("output_text", "text"):
                        break  # best match found for this item
            if text is not None:
                break

    if not text:
        keys = list(body.keys())
        snippet = json.dumps(body)[:1000]
        raise ValueError(
            f"Planner response contained no extractable text. "
            f"Top-level keys: {keys} | body[:1000]: {snippet}"
        )

    return safe_json_loads(text)


# ─────────────────────────────────────────────────────────────────────────────
# CODER PHASE — Tier 3: claude-sonnet-4-6 with mandatory Prompt Caching
# ─────────────────────────────────────────────────────────────────────────────

def anthropic_text(model, system_text, user_text):
    """
    Call Anthropic /v1/messages with top-level automatic Prompt Caching.

    ECONOMIC LAW: cache_control {"type": "ephemeral"} on the request body
    activates GA-tier automatic caching. Every repeated system prompt context
    (coder instructions, healer instructions) becomes a cache read at ~10x
    lower cost. Removing cache_control burns survival capital.

    No anthropic-beta header is required — prompt caching is a GA feature.
    Headers are exactly: Content-Type, x-api-key, anthropic-version.
    """
    payload = {
        "model": model,
        "max_tokens": 4096,
        "cache_control": {"type": "ephemeral"},
        "system": system_text,
        "messages": [
            {"role": "user", "content": user_text}
        ],
    }

    req = urllib.request.Request(
        ANTHROPIC_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read().decode())

    parts = []
    for item in body.get("content", []):
        if item.get("type") == "text":
            parts.append(item.get("text", ""))

    if not parts:
        raise ValueError("no text returned by Anthropic")

    return "".join(parts)


def gemini_text(model, instructions, input_text):
    """Call Gemini generateContent. Used for Tier-2 code review."""
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )

    prompt = f"{instructions}\n\n{input_text}"

    payload = {
        "contents": [
            {"parts": [{"text": prompt}]}
        ]
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": GEMINI_API_KEY,
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        body = json.loads(resp.read().decode())

    candidates = body.get("candidates", [])
    if not candidates:
        raise ValueError("no candidates returned by Gemini")

    parts = candidates[0].get("content", {}).get("parts", [])
    text_parts = [part.get("text", "") for part in parts if "text" in part]

    if not text_parts:
        raise ValueError("no text returned by Gemini")

    return "".join(text_parts)


# ─────────────────────────────────────────────────────────────────────────────
# CODE GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def build_coder_input(task, target_file):
    """Construct the user-turn prompt for the coder from a task dict."""
    task_name = task["task_name"] if isinstance(task, dict) else task
    rationale = task.get("rationale", "") if isinstance(task, dict) else ""
    required_tools = (
        task.get("required_tools", []) if isinstance(task, dict) else []
    )
    tools_str = ", ".join(required_tools) if required_tools else "none"
    return (
        f"Task: {task_name}\n"
        f"Target file: {target_file}\n"
        f"Rationale: {rationale}\n"
        f"Required tools: {tools_str}\n\n"
        f"Generate the complete, production-ready Python code for "
        f"{target_file}.\n"
    )


def generate_code(task, target_file):
    """Invoke Tier-3 coder to produce first-attempt code for the given task."""
    instructions = (
        "You are the Coder. Generate the exact Python code required to fulfill "
        "the requested task. Return ONLY raw Python code. No markdown fences. "
        "No explanations."
    )
    input_text = build_coder_input(task, target_file)
    code = anthropic_text(CODER_MODEL, instructions, input_text)
    return strip_code_fences(code)


def review_code(task_name, target_file, code):
    """Invoke Tier-2 Gemini reviewer to validate and patch candidate code."""
    instructions = (
        "You are the Reviewer. Ensure this Python code is syntactically valid, "
        "safe, and fulfills the objective. Return ONLY approved Python code. "
        "If fatal flaws exist, fix them and return the patched code. "
        "No markdown fences. No explanations."
    )
    input_text = (
        f"Task: {task_name}\n"
        f"Target file: {target_file}\n\n"
        f"Review this code and patch it if needed:\n\n{code}\n"
    )
    reviewed = gemini_text(REVIEWER_MODEL, instructions, input_text)
    return strip_code_fences(reviewed)


def heal_code_with_model(model, task, target_file, broken_code, error_text):
    """
    Healing attempt using the specified model.

    POISONED CHALICE RULE: For Opus escalation, broken_code MUST be the
    original first-generation code — never Sonnet's degraded nth-attempt
    mutation. Passing degraded code wastes Tier-4 tokens on deciphering
    hallucinated logic instead of solving the original problem.
    """
    task_name = task["task_name"] if isinstance(task, dict) else task
    instructions = (
        "You are the Coder. You are given broken Python code and an error "
        "traceback. Fix the error and return the complete corrected Python "
        "code. Return ONLY raw Python code. No markdown fences. No "
        "explanations."
    )
    input_text = (
        f"Task: {task_name}\n"
        f"Target file: {target_file}\n\n"
        f"Broken code:\n{broken_code}\n\n"
        f"Error:\n{error_text}\n\n"
        f"Return the complete fixed Python code.\n"
    )
    code = anthropic_text(model, instructions, input_text)
    return strip_code_fences(code)


def heal_code(task, target_file, broken_code, error_text):
    """Healing attempt using CODER_MODEL (Tier 3). Delegates to heal_code_with_model."""
    return heal_code_with_model(
        CODER_MODEL, task, target_file, broken_code, error_text
    )


# ─────────────────────────────────────────────────────────────────────────────
# FILE SYSTEM & EXECUTION GATE
# ─────────────────────────────────────────────────────────────────────────────

def fs_write(rel_path, content):
    """Write file via authenticated FastAPI tool-server."""
    url = f"{TOOL_SERVER_URL}/fs_write"
    payload = json.dumps(
        {"rel_path": rel_path, "content": content}
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": TOOL_SERVER_KEY,
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())


def run_tests(target_file=""):
    """
    Run pytest via the authenticated FastAPI tool-server.

    DO NOT bypass the control plane with local subprocess calls. All side
    effects must be mediated by the tool server, which enforces the
    _safe(BASE_PATH) path-traversal jail. A direct subprocess call would
    allow a hallucinated task to run tests on an arbitrary path.
    Returns (returncode, stdout, stderr).
    """
    url = f"{TOOL_SERVER_URL}/run_tests"
    if target_file:
        query = urllib.parse.urlencode({"test_file": target_file})
        url = f"{url}?{query}"

    req = urllib.request.Request(
        url,
        data=b"",
        headers={
            "X-API-Key": TOOL_SERVER_KEY,
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read().decode("utf-8"))
        return (
            result.get("code", 1),
            result.get("stdout", ""),
            result.get("stderr", ""),
        )


def no_tests_collected(returncode, stderr):
    """
    Soft-pass check. Pytest exit code 5 means no tests were collected.
    Expected for new source files with no test suite yet.
    """
    if returncode == 5:
        return True
    combined = stderr.lower()
    return "no tests ran" in combined or "no tests collected" in combined


def compute_written_hash(target_file):
    """Compute SHA-256 of a file that was just written to disk."""
    return compute_file_hash(target_file)


def write_and_run_execution_gate(run_id, task_name, target_file, code):
    """
    Write candidate code to disk via fs_write, then run tests.

    Returns (success: bool, detail: str).
    Soft-passes when no_tests_collected (pytest exit code 5).
    PRESERVES SEMANTICS: does NOT force test_file= for non-test source files.
    """
    create_event(run_id, "tool_call_initiated", {
        "tool": "fs_write",
        "target": target_file,
    })
    write_result = fs_write(target_file, code)
    create_event(run_id, "tool_call_response", write_result)

    returncode, stdout, stderr = run_tests(target_file)

    if returncode == 0:
        return True, stdout

    if no_tests_collected(returncode, stderr):
        create_event(run_id, "no_tests_collected", {
            "target_file": target_file,
            "returncode": returncode,
        })
        return True, stdout

    return False, (stderr or stdout)


# ─────────────────────────────────────────────────────────────────────────────
# DAEMON ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Anchor CWD to the script directory so all relative paths (including
    # ledger.py's DB_PATH = "quantos.db") resolve to the same file regardless
    # of how systemd sets WorkingDirectory.
    os.chdir(_SCRIPT_DIR)

    init_db()

    error_multiplier = 0
    consecutive_loops = 0
    consecutive_planner_rejects = 0

    # Log daemon boot for ledger continuity across service restarts.
    boot_run_id = create_run()
    create_event(boot_run_id, "daemon_boot", {
        "version": DAEMON_VERSION,
        "coder_model": CODER_MODEL,
        "escalation_model": ESCALATION_CODER,
        "planner_model": PLANNER_MODEL,
        "reviewer_model": REVIEWER_MODEL,
    })
    create_event(boot_run_id, "run_closed", {"final_status": True})

    print(
        f"[DAEMON] QuantOS Daemon {DAEMON_VERSION} initialized. "
        f"Entering perpetual loop."
    )

    while True:
        # ── SP-1: ASYNCHRONOUS WAKE-LOCK ──────────────────────────────────────
        if os.path.exists(HALT_LOCK_FILE):
            print("[DAEMON] HALT_LOCK detected. Organism is asleep.")
            time.sleep(10)
            continue
        # run_id is NOT created while the halt lock exists (continue above).

        # ── CREDENTIALS: validate before touching the ledger or any API ───────
        try:
            load_runtime_config()
        except EnvironmentError as _cred_err:
            with open(HALT_LOCK_FILE, "w") as hf:
                hf.write(
                    f"Fatal boot error — missing credentials: {_cred_err}"
                )
            print(
                f"[FATAL] Missing credentials: {_cred_err}. "
                f"Halt-lock written. Resolve env vars and remove "
                f"HALT_LOCK.flag to resume."
            )
            time.sleep(10)
            continue

        run_id = create_run()

        try:
            # ── PHASE 1: PLANNER ──────────────────────────────────────────────
            events = get_macro_context(10)
            ledger_context = "\n".join(
                f"- {etype}: {payload}" for etype, payload in events
            )
            allowed_tasks_text = "\n- ".join(ALLOWED_TASKS)

            prompt = (
                f"You are the planner for QuantOS {DAEMON_VERSION}.\n\n"
                f"Analyze the recent event ledger below and choose the NEXT "
                f"logical task.\n\n"
                f"Recent ledger:\n{ledger_context}\n\n"
                f"You must choose exactly one task_name from this allowed "
                f"list:\n- {allowed_tasks_text}\n\n"
                f"Rules:\n"
                f"- Pick only from the allowed task_name list.\n"
                f"- Genesis-04 executes ONLY Python source file tasks. "
                f"Every task must produce a .py file.\n"
                f"- target_file MUST be a relative .py path "
                f"(e.g. reviewer.py, arbiter.py). "
                f"Non-.py targets will be rejected by the executor.\n"
                f"- Choose the smallest next step.\n"
                f"- Do not invent unrelated domains.\n"
                f"- Return STRICT JSON only.\n"
            )

            create_event(run_id, "planner_prompt", {"prompt": prompt})
            task = send_planner(prompt)
            create_event(run_id, "planner_output", task)

            task_name = task["task_name"]
            print(
                f"[PLANNER] Task proposed: {task_name} "
                f"→ {task.get('target_file', '?')}"
            )

            review = reviewer(task)
            create_event(run_id, "planner_schema_review", review)

            arb = arbiter(task, review)
            create_event(run_id, "planner_doctrine_check", arb)

            if not arb["approved"]:
                create_event(run_id, "run_closed", {"final_status": False})
                consecutive_planner_rejects += 1
                error_multiplier += 1
                sleep_time = 2 ** error_multiplier
                print(
                    f"[PLANNER] Arbiter rejected: {arb['reason']}. "
                    f"Backoff: {sleep_time}s. "
                    f"Rejects: {consecutive_planner_rejects}/{MAX_PLANNER_REJECTS}"
                )
                if consecutive_planner_rejects >= MAX_PLANNER_REJECTS:
                    with open(HALT_LOCK_FILE, "w") as hf:
                        hf.write(
                            f"Planner spin-loop: {MAX_PLANNER_REJECTS} "
                            f"consecutive arbiter rejections. "
                            f"Last reason: {arb['reason']}"
                        )
                    create_event(run_id, "circuit_breaker_tripped", {
                        "reason": "planner_spin_loop",
                        "consecutive_rejects": consecutive_planner_rejects,
                    })
                    print("[BREAKER] Planner spin-loop detected. Halt-lock written.")
                    consecutive_planner_rejects = 0
                time.sleep(sleep_time)
                continue

            consecutive_planner_rejects = 0

            # ── PHASE 2: IDEMPOTENCY GATE ─────────────────────────────────────
            target_file = resolve_target_file(task)

            # Hard guard — Genesis-04 executor only handles Python source files.
            if not target_file.endswith(".py"):
                create_event(run_id, "task_rejected", {
                    "reason": "non_code_target_not_supported_in_genesis_04",
                    "target_file": target_file,
                    "task_name": task_name,
                })
                create_event(run_id, "run_closed", {"final_status": False})
                error_multiplier += 1
                sleep_time = 2 ** error_multiplier
                print(
                    f"[EXECUTOR] Rejected non-.py target '{target_file}'. "
                    f"Backoff: {sleep_time}s."
                )
                time.sleep(sleep_time)
                continue

            rationale = task.get("rationale", "")
            intent_hash = compute_intent_hash(task_name, rationale)

            # Telemetry only — return value MUST NOT cause a skip.
            # Using file existence as a hard gate causes the code-freeze paradox.
            file_already_done = is_task_completed(target_file)
            create_event(run_id, "file_completion_check", {
                "target_file": target_file,
                "already_done": file_already_done,
            })

            # Hard skip gate — the ONLY condition that skips execution.
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            already_done = intent_already_executed_successfully(
                cursor, intent_hash
            )
            conn.close()

            if already_done:
                create_event(run_id, "skipped_task", {
                    "intent_hash": intent_hash,
                    "task_name": task_name,
                    "target_file": target_file,
                })
                create_event(run_id, "run_closed", {"final_status": True})
                error_multiplier = 0  # past transient errors don't penalise future cycles
                print(
                    f"[EXECUTOR] Skipped (intent already executed): "
                    f"{intent_hash[:12]}..."
                )
                time.sleep(2)
                continue

            # ── PHASE 3: EXECUTOR ─────────────────────────────────────────────
            create_event(run_id, "executor_task", {
                "task_name": task_name,
                "target_file": target_file,
                "intent_hash": intent_hash,
                "difficulty": task.get("difficulty", 0),
            })

            create_event(run_id, "coder_prompt", {
                "model": CODER_MODEL,
                "task_name": task_name,
                "target_file": target_file,
            })
            print(f"[EXECUTOR] Coder ({CODER_MODEL}) generating: {target_file}")
            coder_code = generate_code(task, target_file)
            create_event(run_id, "coder_output", {
                "model": CODER_MODEL,
                "code": coder_code,
            })

            # Immutable clean-slate copy for potential Opus escalation.
            # CONSTITUTIONAL LAW: Opus receives THIS, never the mutated candidate.
            original_coder_code = coder_code

            create_event(run_id, "reviewer_prompt", {
                "model": REVIEWER_MODEL,
                "task_name": task_name,
                "target_file": target_file,
            })
            print(
                f"[EXECUTOR] Reviewer ({REVIEWER_MODEL}) reviewing: {target_file}"
            )
            reviewed_code = review_code(task_name, target_file, coder_code)
            create_event(run_id, "code_reviewer_output", {
                "model": REVIEWER_MODEL,
                "code": reviewed_code,
            })

            # ── PHASE 4: OUROBOROS HEALING REFLEX ────────────────────────────
            candidate_code = reviewed_code
            healing_count = 0
            already_escalated = False
            final_error_text = ""
            task_succeeded = False

            while True:
                # Compile gate — syntax check before any disk I/O.
                try:
                    compile(candidate_code, target_file, "exec")
                    create_event(run_id, "syntax_check", {"valid": True})
                except SyntaxError as se:
                    final_error_text = str(se)
                    create_event(run_id, "syntax_failure", {
                        "error": final_error_text,
                    })

                    if healing_count >= MAX_RETRIES:
                        # ── ESCALATION CHECK (syntax failure path) ──
                        if (
                            task.get("difficulty", 0) >= 4
                            and not already_escalated
                        ):
                            already_escalated = True
                            print(
                                f"[HEALING] Escalating to {ESCALATION_CODER}. "
                                f"Clean slate — original_coder_code, not "
                                f"degraded candidate."
                            )
                            create_event(run_id, "escalation_attempt", {
                                "model": ESCALATION_CODER,
                                "phase": "syntax",
                                "error": final_error_text,
                            })
                            escalated = heal_code_with_model(
                                ESCALATION_CODER,
                                task,
                                target_file,
                                original_coder_code,  # clean slate
                                final_error_text,
                            )
                            # Constitutional law: no model bypasses the pipeline.
                            escalated = review_code(
                                task_name, target_file, escalated
                            )
                            candidate_code = escalated
                            continue  # back to compile gate

                        else:
                            # Exhaustion — failure triggers backoff (SP-4 fix).
                            create_event(run_id, "run_closed", {
                                "final_status": False,
                            })
                            error_multiplier += 1
                            sleep_time = 2 ** error_multiplier
                            print(
                                f"[ERROR] All healing exhausted (syntax) for "
                                f"{target_file}. Backoff: {sleep_time}s"
                            )
                            if error_multiplier > 10:
                                with open(HALT_LOCK_FILE, "w") as hf:
                                    hf.write(
                                        f"Fatal syntax healing loop on "
                                        f"{target_file}: "
                                        f"{final_error_text[:300]}"
                                    )
                            else:
                                time.sleep(sleep_time)
                            break  # task_succeeded stays False

                    healing_count += 1
                    print(
                        f"[HEALING] SyntaxError attempt "
                        f"{healing_count}/{MAX_RETRIES} for {target_file}"
                    )
                    create_event(run_id, f"healing_attempt_{healing_count}", {
                        "phase": "syntax",
                        "error": final_error_text,
                        "attempt": healing_count,
                    })
                    candidate_code = heal_code(
                        task, target_file, candidate_code, final_error_text
                    )
                    continue  # retry compile gate

                # Syntax OK — attempt execution gate.
                gate_ok, gate_detail = write_and_run_execution_gate(
                    run_id, task_name, target_file, candidate_code
                )

                if gate_ok:
                    task_succeeded = True
                    break

                final_error_text = gate_detail

                if healing_count >= MAX_RETRIES:
                    # ── ESCALATION CHECK (execution gate failure path) ──
                    if (
                        task.get("difficulty", 0) >= 4
                        and not already_escalated
                    ):
                        already_escalated = True
                        print(
                            f"[HEALING] Escalating to {ESCALATION_CODER}. "
                            f"Clean slate — original_coder_code, not "
                            f"degraded candidate."
                        )
                        create_event(run_id, "escalation_attempt", {
                            "model": ESCALATION_CODER,
                            "phase": "execution",
                            "error": final_error_text[:500],
                        })
                        escalated = heal_code_with_model(
                            ESCALATION_CODER,
                            task,
                            target_file,
                            original_coder_code,  # clean slate
                            final_error_text,
                        )
                        # Constitutional law: no model bypasses the pipeline.
                        escalated = review_code(
                            task_name, target_file, escalated
                        )
                        candidate_code = escalated
                        continue  # back to compile gate

                    else:
                        # Exhaustion — failure triggers backoff (SP-4 fix).
                        create_event(run_id, "run_closed", {
                            "final_status": False,
                        })
                        error_multiplier += 1
                        sleep_time = 2 ** error_multiplier
                        print(
                            f"[ERROR] All healing exhausted (gate) for "
                            f"{target_file}. Backoff: {sleep_time}s"
                        )
                        if error_multiplier > 10:
                            with open(HALT_LOCK_FILE, "w") as hf:
                                hf.write(
                                    f"Fatal gate healing loop on "
                                    f"{target_file}: "
                                    f"{final_error_text[:300]}"
                                )
                        else:
                            time.sleep(sleep_time)
                        break  # task_succeeded stays False

                healing_count += 1
                print(
                    f"[HEALING] Gate failed. Attempt "
                    f"{healing_count}/{MAX_RETRIES} for {target_file}"
                )
                create_event(run_id, f"healing_attempt_{healing_count}", {
                    "phase": "execution",
                    "error": final_error_text[:500],
                    "attempt": healing_count,
                })
                candidate_code = heal_code(
                    task, target_file, candidate_code, final_error_text
                )
                # back to compile gate

            # Healing loop exited without success — skip finalization.
            if not task_succeeded:
                continue

            # ── PHASE 5: FINALIZE ─────────────────────────────────────────────
            file_hash = compute_written_hash(target_file)
            create_event(run_id, "file_content_hash", {
                "file": target_file,
                "sha256": file_hash,
            })

            artifact_id = register_artifact(run_id, target_file)
            create_event(run_id, "artifact_registered", {
                "artifact_id": artifact_id,
                "file": target_file,
                "sha256": file_hash,
            })

            create_event(run_id, "run_closed", {
                "final_status": True,
                "intent_hash": intent_hash,
            })

            print(
                f"[EXECUTOR] Cycle complete. "
                f"File: {target_file} | Hash: {intent_hash[:12]}..."
            )

            # ── SP-4a: CIRCUIT BREAKER ────────────────────────────────────────
            # Only counts real successful executions — not skips or idle cycles.
            error_multiplier = 0
            consecutive_loops += 1

            if consecutive_loops >= MAX_CONSECUTIVE_LOOPS:
                print(
                    f"[BREAKER] {MAX_CONSECUTIVE_LOOPS} autonomous cycles "
                    f"complete. Tripping circuit breaker."
                )
                with open(HALT_LOCK_FILE, "w") as hf:
                    hf.write(
                        f"Circuit breaker tripped after "
                        f"{MAX_CONSECUTIVE_LOOPS} autonomous cycles."
                    )
                create_event(run_id, "circuit_breaker_tripped", {
                    "loops": consecutive_loops,
                    "version": DAEMON_VERSION,
                })
                consecutive_loops = 0

            # ── INTER-CYCLE COOLDOWN ──────────────────────────────────────────
            time.sleep(2)

        # ── SP-4b: EXPONENTIAL BACKOFF ────────────────────────────────────────
        except urllib.error.HTTPError as e:
            request_id = ""
            try:
                request_id = (
                    e.headers.get("x-request-id", "")
                    or e.headers.get("request-id", "")
                )
            except Exception:
                pass
            error_payload = format_http_error(e)
            error_payload["request_id"] = request_id
            create_event(run_id, "api_failure", error_payload)
            create_event(run_id, "run_closed", {"final_status": False})
            error_multiplier += 1
            sleep_time = 2 ** error_multiplier
            print(
                f"[ERROR] API {e.code} {e.reason} (req: {request_id}). "
                f"Backoff: {sleep_time}s. Multiplier: {error_multiplier}"
            )
            if error_multiplier > 10:
                with open(HALT_LOCK_FILE, "w") as hf:
                    hf.write(f"Fatal API error loop: {e.code} {e.reason}")
            else:
                time.sleep(sleep_time)

        except urllib.error.URLError as e:
            create_event(run_id, "api_failure", {
                "error": f"URLError: {e.reason}",
            })
            create_event(run_id, "run_closed", {"final_status": False})
            error_multiplier += 1
            sleep_time = 2 ** error_multiplier
            print(
                f"[ERROR] URL: {e.reason}. "
                f"Backoff: {sleep_time}s. Multiplier: {error_multiplier}"
            )
            if error_multiplier > 10:
                with open(HALT_LOCK_FILE, "w") as hf:
                    hf.write(f"Fatal URL error loop: {e.reason}")
            else:
                time.sleep(sleep_time)

        except Exception as e:
            tb_str = traceback.format_exc()
            create_event(run_id, "daemon_error", {
                "error": str(e),
                "traceback": tb_str[:2000],
            })
            create_event(run_id, "run_closed", {"final_status": False})
            error_multiplier += 1
            sleep_time = 2 ** error_multiplier
            print(
                f"[ERROR] {str(e)[:200]}. "
                f"Backoff: {sleep_time}s. Multiplier: {error_multiplier}"
            )
            if error_multiplier > 10:
                with open(HALT_LOCK_FILE, "w") as hf:
                    hf.write(f"Fatal error loop: {str(e)[:500]}")
            else:
                time.sleep(sleep_time)
