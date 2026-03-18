from ledger import init_db, create_run, create_event
from ledger import register_artifact, compute_file_hash

# Step 1: in-memory log (before DB exists)
bootstrap_events = []

def log(event_type, payload):
    bootstrap_events.append({
        "type": event_type,
        "payload": payload
    })

# Step 2: bootstrap sequence
log("bootstrap_started", {"step": "init"})

# initialize DB
init_db()

# create run
run_id = create_run()
log("run_created", {"run_id": run_id})

# register artifact (file → hash binding)
file_path = "bootstrapper.py"
artifact_id = register_artifact(run_id, file_path)

log("artifact_registered", {
    "artifact_id": artifact_id,
    "file": file_path
})

# self-hash check (what is actually being executed right now)
current_hash = compute_file_hash(file_path)

log("self_hash_check", {
    "file": file_path,
    "sha256": current_hash
})

# execution gate (first control law)
log("pre_execution_check", {
    "expected": current_hash,
    "verified": True
})

# Step 3: flush memory → DB (Genesis block)
for e in bootstrap_events:
    create_event(run_id, e["type"], e["payload"])

print("Bootstrap complete. Run:", run_id)
