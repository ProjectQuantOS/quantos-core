from ledger import init_db, create_run, create_event, register_artifact, compute_file_hash

def planner():
    return True

def reviewer(decision):
    return decision

def arbiter(decision):
    return decision

def replay_run(run_id):
    import sqlite3

    conn = sqlite3.connect("quantos.db")
    cursor = conn.cursor()

    cursor.execute(
        "SELECT type, payload FROM events WHERE run_id = ? ORDER BY created_at",
        (run_id,)
    )

    rows = cursor.fetchall()
    conn.close()

    for r in rows:
        print(r)

def run_task():
    init_db()

    run_id = create_run()

    file_path = "task_runner.py"
    artifact_id = register_artifact(run_id, file_path)

    create_event(run_id, "artifact_registered", {
        "artifact_id": artifact_id,
        "file": file_path
    })

    current_hash = compute_file_hash(file_path)

    create_event(run_id, "pre_execution_check", {
        "file": file_path,
        "sha256": current_hash,
        "verified": True
    })

    # --- DECISION GATE ---
    decision = planner()
    create_event(run_id, "planner_decision", {"execute": decision})

    reviewed = reviewer(decision)
    create_event(run_id, "reviewer_decision", {"execute": reviewed})

    should_execute = arbiter(reviewed)
    create_event(run_id, "arbiter_decision", {"execute": should_execute})

    if not should_execute:
        create_event(run_id, "task_skipped", {})
        print("Task skipped:", run_id)
        return

    # --- EXECUTION ---
    create_event(run_id, "task_started", {
        "task": "genesis_test"
    })

    try:
        result = "ok"

        create_event(run_id, "task_completed", {
            "result": result
        })

    except Exception as e:
        create_event(run_id, "task_failed", {
            "error": str(e)
        })

if __name__ == "__main__":
    run_task()

    import sqlite3

    conn = sqlite3.connect("quantos.db")
    cursor = conn.cursor()

    cursor.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1")
    latest_run = cursor.fetchone()[0]

    conn.close()

    print("\n--- REPLAY ---")
    replay_run(latest_run)
