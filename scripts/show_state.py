from __future__ import annotations

import sqlite3
from pathlib import Path


DB_PATH = Path("/data/state.db")


def main() -> int:
    if not DB_PATH.exists():
        print("No state DB found at /data/state.db")
        return 0

    conn = sqlite3.connect(DB_PATH)
    print("Processed videos:")
    for row in conn.execute(
        """
        SELECT video_id, title, processed_at
        FROM processed_videos
        ORDER BY processed_at DESC
        LIMIT 20
        """
    ):
        print(f"- {row[0]} | {row[1]} | {row[2]}")

    print("\nVideo attempts:")
    for row in conn.execute(
        """
        SELECT video_id, title, status, reason, updated_at
        FROM video_attempts
        ORDER BY updated_at DESC
        LIMIT 20
        """
    ):
        print(f"- {row[0]} | {row[2]} | {row[3]} | {row[1]} | {row[4]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
