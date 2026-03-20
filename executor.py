import json
import os
import urllib.request
import urllib.error

from ledger import init_db, create_run, create_event, compute_file_hash

TOOL_SERVER_URL = "http://127.0.0.1:8000"
TOOL_SERVER_KEY = os.environ["TOOL_SERVER_KEY"]
TARGET_FILE = "arbiter.py"
TARGET_CONTENT = "def arbiter(decision):\n    return decision\n"


def fs_write(rel_path, content):
    url = f"{TOOL_SERVER_URL}/fs_write"
    payload = json.dumps({
        "rel_path": rel_path,
        "content": content
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": TOOL_SERVER_KEY,
        },
        method="POST"
    )

    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


if __name__ == "__main__":
    init_db()
    run_id = create_run()

    try:
        create_event(run_id, "tool_call_initiated", {
            "tool": "fs_write",
            "target": TARGET_FILE
        })

        result = fs_write(TARGET_FILE, TARGET_CONTENT)

        create_event(run_id, "tool_call_response", result)

        file_hash = compute_file_hash(TARGET_FILE)
        create_event(run_id, "file_content_hash", {
            "file": TARGET_FILE,
            "sha256": file_hash
        })

        create_event(run_id, "run_closed", {"final_status": True})

        print("Executor run:", run_id)
        print(json.dumps(result, indent=2))

    except urllib.error.HTTPError as e:
        create_event(run_id, "tool_call_failure", {
            "error": f"HTTPError: {e.code} {e.reason}"
        })
        create_event(run_id, "run_closed", {"final_status": False})
        print("Executor run failed:", run_id)

    except urllib.error.URLError as e:
        create_event(run_id, "tool_call_failure", {
            "error": f"URLError: {e.reason}"
        })
        create_event(run_id, "run_closed", {"final_status": False})
        print("Executor run failed:", run_id)
