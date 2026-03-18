import sqlite3
import uuid
import json
import datetime
import hashlib

DB_PATH = "quantos.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # RUNS TABLE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        status TEXT,
        created_at TEXT
    )
    """)

    # EVENTS TABLE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        run_id TEXT,
        type TEXT,
        payload TEXT,
        created_at TEXT
    )
    """)

    # ARTIFACTS TABLE (THIS WAS MISSING)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS artifacts (
        artifact_id TEXT PRIMARY KEY,
        run_id TEXT,
        file_path TEXT,
        sha256 TEXT,
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def create_run():
    conn = get_connection()
    cursor = conn.cursor()

    run_id = str(uuid.uuid4())
    created_at = datetime.datetime.now(datetime.UTC).isoformat()

    cursor.execute(
        "INSERT INTO runs (run_id, status, created_at) VALUES (?, ?, ?)",
        (run_id, "started", created_at)
    )

    conn.commit()
    conn.close()

    return run_id


def create_event(run_id, event_type, payload):
    conn = get_connection()
    cursor = conn.cursor()

    event_id = str(uuid.uuid4())
    created_at = datetime.datetime.now(datetime.UTC).isoformat()

    cursor.execute(
        "INSERT INTO events (event_id, run_id, type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
        (event_id, run_id, event_type, json.dumps(payload), created_at)
    )

    conn.commit()
    conn.close()

    return event_id


def list_runs():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM runs")
    rows = cursor.fetchall()

    conn.close()
    return rows


def compute_file_hash(file_path):
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)

    return sha256.hexdigest()


def register_artifact(run_id, file_path):
    conn = get_connection()
    cursor = conn.cursor()

    artifact_id = str(uuid.uuid4())
    created_at = datetime.datetime.now(datetime.UTC).isoformat()
    file_hash = compute_file_hash(file_path)

    cursor.execute(
        "INSERT INTO artifacts (artifact_id, run_id, file_path, sha256, created_at) VALUES (?, ?, ?, ?, ?)",
        (artifact_id, run_id, file_path, file_hash, created_at)
    )

    conn.commit()
    conn.close()

    return artifact_id


if __name__ == "__main__":
    init_db()

    run_id = create_run()
    print("Run created:", run_id)

    artifact_id = register_artifact(run_id, "bootstrapper.py")

    create_event(run_id, "artifact_registered", {
        "artifact_id": artifact_id,
        "file": "bootstrapper.py"
    })

    runs = list_runs()
    print("All runs:", runs)
