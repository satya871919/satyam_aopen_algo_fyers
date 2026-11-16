#!/usr/bin/env python3
"""Create persistent storage for Fyers' rolling daily request budget."""

import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from dotenv import load_dotenv

load_dotenv(os.path.join(PROJECT_ROOT, ".env"))


def main():
    parser = argparse.ArgumentParser(description="Persistent Fyers daily usage migration")
    parser.add_argument("--status", action="store_true", help="Check status without changing")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL", "sqlite:///db/openalgo.db")
    sqlite_prefix = "sqlite:///"
    if database_url.startswith(sqlite_prefix):
        sqlite_path = database_url[len(sqlite_prefix) :]
        if sqlite_path and sqlite_path != ":memory:" and not os.path.isabs(sqlite_path):
            database_url = sqlite_prefix + os.path.join(PROJECT_ROOT, sqlite_path)
            os.environ["DATABASE_URL"] = database_url

    if (
        args.status
        and database_url.startswith(sqlite_prefix)
        and database_url[len(sqlite_prefix) :] != ":memory:"
        and not os.path.exists(database_url[len(sqlite_prefix) :])
    ):
        print("Fyers daily usage table: MISSING")
        return 1

    from database.fyers_usage_db import (
        FyersApiRequest,
        FyersUsageLock,
        engine,
        init_db,
    )

    if args.status:
        from sqlalchemy import inspect, select

        tables = set(inspect(engine).get_table_names())
        usage_exists = FyersApiRequest.__tablename__ in tables
        lock_exists = FyersUsageLock.__tablename__ in tables
        lock_seeded = False
        if lock_exists:
            with engine.connect() as connection:
                lock_seeded = (
                    connection.scalar(
                        select(FyersUsageLock.id).where(FyersUsageLock.id == 1)
                    )
                    == 1
                )
        print("Fyers daily usage table: " + ("present" if usage_exists else "MISSING"))
        print("Fyers usage lock: " + ("ready" if lock_seeded else "MISSING"))
        return 0 if usage_exists and lock_seeded else 1

    init_db()
    print("Persistent Fyers daily usage table is ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
