import sqlite3
from pathlib import Path
from typing import Dict, List


def init_db(db_path: str) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                activity_code TEXT,
                controller_ip TEXT NOT NULL,
                hw_version TEXT,
                fw_version TEXT,
                s1 INTEGER,
                s2 INTEGER,
                s3 INTEGER,
                s4 INTEGER,
                s5 INTEGER,
                s6 INTEGER,
                s7 INTEGER,
                s8 INTEGER,
                frame_complete INTEGER NOT NULL,
                latency_ms INTEGER,
                error_text TEXT,
                raw_messages TEXT
            )
            """
        )
        # Migration-safe: add activity_code when DB already exists with old schema.
        cols = conn.execute("PRAGMA table_info(sensor_frames)").fetchall()
        col_names = {c[1] for c in cols}
        if "activity_code" not in col_names:
            conn.execute("ALTER TABLE sensor_frames ADD COLUMN activity_code TEXT")
        conn.commit()


def save_frame(db_path: str, frame: Dict[str, object]) -> None:
    values = frame["values"]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO sensor_frames (
                ts, activity_code, controller_ip, hw_version, fw_version,
                s1, s2, s3, s4, s5, s6, s7, s8,
                frame_complete, latency_ms, error_text, raw_messages
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                frame["ts"],
                frame.get("activity_code"),
                frame["controller_ip"],
                frame.get("hw_version"),
                frame.get("fw_version"),
                values[0],
                values[1],
                values[2],
                values[3],
                values[4],
                values[5],
                values[6],
                values[7],
                1 if frame["frame_complete"] else 0,
                frame.get("latency_ms"),
                frame.get("error_text"),
                ",".join(frame.get("raw_messages", [])),
            ),
        )
        conn.commit()


def recent_frames(db_path: str, limit: int = 100) -> List[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM sensor_frames ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return rows


def list_activity_codes(db_path: str) -> List[str]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT activity_code
            FROM sensor_frames
            WHERE activity_code IS NOT NULL AND TRIM(activity_code) <> ''
            ORDER BY activity_code DESC
            """
        ).fetchall()
    return [r[0] for r in rows]


def frames_for_activity(db_path: str, activity_code: str, limit: int = 5000) -> List[sqlite3.Row]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT *
            FROM sensor_frames
            WHERE activity_code = ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (activity_code, limit),
        ).fetchall()
    return rows
