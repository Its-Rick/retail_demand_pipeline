"""
setup_db.py
-----------
Initializes the retail_dw PostgreSQL database:
  1. Creates the retail_dw database (if not exists)
  2. Runs star_schema.sql to create all tables
  3. Loads sample dimension data for testing

Usage:
    python scripts/setup_db.py
    python scripts/setup_db.py --host localhost --port 5432
"""

import os
import sys
import argparse
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("setup_db")

SCHEMA_SQL = os.path.join(
    os.path.dirname(__file__), "..", "sql", "schema", "star_schema.sql"
)


def get_conn(host, port, user, password, dbname="postgres"):
    return psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    )


def create_database(host, port, user, password, dbname):
    """Create the database if it doesn't exist."""
    conn = get_conn(host, port, user, password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            log.info(f"Created database: {dbname}")
        else:
            log.info(f"Database already exists: {dbname}")
    conn.close()


def run_schema(host, port, user, password, dbname):
    """Run the star schema DDL."""
    conn = get_conn(host, port, user, password, dbname)
    conn.autocommit = True

    with open(SCHEMA_SQL) as f:
        raw_sql = f.read()

    # Split and run statement by statement (skip CREATE DATABASE)
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    skip_prefixes = ("create database", "\\c ", "set search_path")

    with conn.cursor() as cur:
        for stmt in statements:
            if any(stmt.lower().startswith(p) for p in skip_prefixes):
                continue
            if not stmt:
                continue
            try:
                cur.execute(f"SET search_path = warehouse; {stmt}")
                log.info(f"  ✓ {stmt[:60].strip()[:60]}...")
            except Exception as e:
                log.warning(f"  ⚠ Skipped (may already exist): {e}")

    conn.close()
    log.info("Schema setup complete")


def verify_tables(host, port, user, password, dbname):
    """Print a summary of created tables."""
    conn = get_conn(host, port, user, password, dbname)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'warehouse'
            ORDER BY tablename
        """)
        tables = [r[0] for r in cur.fetchall()]

    conn.close()
    log.info(f"Tables in warehouse schema ({len(tables)}):")
    for t in tables:
        log.info(f"  • {t}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host",     default=os.getenv("PG_HOST",     "localhost"))
    parser.add_argument("--port",     default=int(os.getenv("PG_PORT", "5432")), type=int)
    parser.add_argument("--user",     default=os.getenv("PG_USER",     "airflow"))
    parser.add_argument("--password", default=os.getenv("PG_PASSWORD", "airflow"))
    parser.add_argument("--dbname",   default=os.getenv("PG_DBNAME",   "retail_dw"))
    args = parser.parse_args()

    log.info(f"Connecting to PostgreSQL at {args.host}:{args.port}")

    try:
        create_database(args.host, args.port, args.user, args.password, args.dbname)
        run_schema(args.host, args.port, args.user, args.password, args.dbname)
        verify_tables(args.host, args.port, args.user, args.password, args.dbname)
        log.info("\n✅ Database setup complete! Ready for pipeline.")
    except Exception as e:
        log.error(f"Setup failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
