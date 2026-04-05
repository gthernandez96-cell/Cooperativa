#!/usr/bin/env python3
"""
Migra datos desde SQLite hacia PostgreSQL para el proyecto Cooperativa.

Uso:
  export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
  python scripts/migrate_sqlite_to_postgres.py --sqlite ./cooperativa.db

Requisitos:
  pip install psycopg[binary]
"""

import argparse
import os
import sqlite3
import sys
from typing import List

try:
    import psycopg  # type: ignore[import-not-found]
except Exception as exc:  # pragma: no cover - dependencia opcional
    print("ERROR: psycopg no esta instalado. Ejecuta: pip install psycopg[binary]", file=sys.stderr)
    raise SystemExit(1) from exc


def get_user_tables(sqlite_conn: sqlite3.Connection) -> List[str]:
    rows = sqlite_conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [row[0] for row in rows]


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def migrate_table(sqlite_conn: sqlite3.Connection, pg_conn, table: str) -> None:
    col_rows = sqlite_conn.execute(f"PRAGMA table_info({table})").fetchall()
    columns = [row[1] for row in col_rows]
    if not columns:
        print(f"[WARN] Tabla sin columnas: {table}")
        return

    select_sql = f"SELECT {', '.join(quote_ident(c) for c in columns)} FROM {quote_ident(table)}"
    data_rows = sqlite_conn.execute(select_sql).fetchall()

    with pg_conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {quote_ident(table)} RESTART IDENTITY CASCADE")
        if data_rows:
            placeholders = ', '.join(['%s'] * len(columns))
            insert_sql = (
                f"INSERT INTO {quote_ident(table)} ({', '.join(quote_ident(c) for c in columns)}) "
                f"VALUES ({placeholders})"
            )
            cur.executemany(insert_sql, data_rows)

    print(f"[OK] {table}: {len(data_rows)} filas")


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrar datos SQLite -> PostgreSQL")
    parser.add_argument(
        "--sqlite",
        default="./cooperativa.db",
        help="Ruta del archivo SQLite (default: ./cooperativa.db)",
    )
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        print("ERROR: Debes definir DATABASE_URL en el entorno.", file=sys.stderr)
        return 1

    if not os.path.exists(args.sqlite):
        print(f"ERROR: No existe el archivo SQLite: {args.sqlite}", file=sys.stderr)
        return 1

    sqlite_conn = sqlite3.connect(args.sqlite)
    sqlite_conn.row_factory = sqlite3.Row

    try:
        with psycopg.connect(database_url) as pg_conn:
            tables = get_user_tables(sqlite_conn)
            if not tables:
                print("No se encontraron tablas de usuario en SQLite.")
                return 0

            for table in tables:
                migrate_table(sqlite_conn, pg_conn, table)

            pg_conn.commit()
            print("\nMigracion finalizada correctamente.")
    finally:
        sqlite_conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
