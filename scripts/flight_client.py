#!/Users/ritesh/venv/bin/python
"""Flight SQL client that creates a table, inserts rows, and queries them back.

The script prefers the standalone ADBC Flight SQL driver when available and
falls back to the Python Flight SQL API when that driver is not installed.

Default interpreter: /Users/ritesh/venv/bin/python
"""

import argparse
import sys


DEFAULT_ROWS = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Carol"),
]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8815, type=int)
    parser.add_argument("--table", default="users")
    parser.add_argument("--query", default=None,
                        help="Optional final SELECT query. Defaults to selecting all inserted rows.")
    return parser.parse_args()


def create_statements(table_name, final_query=None):
    create_table = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER, name VARCHAR)"
    clear_table = f"DELETE FROM {table_name}"
    inserts = [
        f"INSERT INTO {table_name} VALUES ({row_id}, '{name}')"
        for row_id, name in DEFAULT_ROWS
    ]
    select_query = final_query or f"SELECT id, name FROM {table_name} ORDER BY id"
    return create_table, clear_table, inserts, select_query


def print_rows(result):
    if hasattr(result, "to_pandas"):
        print(result.to_pandas())
        return
    if hasattr(result, "fetchall"):
        for row in result.fetchall():
            print(row)
        return
    print(result)


def run_adbc_workflow(uri, table_name, final_query=None):
    try:
        import adbc_driver_flightsql.dbapi as adbc_dbapi
    except Exception as exc:
        return False, f"ADBC not available: {exc}"

    create_table, clear_table, inserts, select_query = create_statements(table_name, final_query)
    conn = None
    cur = None
    try:
        conn = adbc_dbapi.connect(uri)
        cur = conn.cursor()

        print("Connected via standalone ADBC Flight SQL driver")
        cur.execute(create_table)
        print(f"Created table '{table_name}'")

        cur.execute(clear_table)
        print(f"Cleared existing rows from '{table_name}'")

        for statement in inserts:
            cur.execute(statement)
        print(f"Inserted {len(DEFAULT_ROWS)} rows into '{table_name}'")

        cur.execute(select_query)
        print(f"Query results for: {select_query}")

        if hasattr(cur, "fetch_arrow_table"):
            print_rows(cur.fetch_arrow_table())
        else:
            print_rows(cur)
        return True, "OK"
    except Exception as exc:
        return False, str(exc)
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def run_pyarrow_flight_sql_workflow(uri, table_name, final_query=None):
    try:
        from pyarrow.flight import sql
    except Exception as exc:
        return False, f"pyarrow.flight.sql not available: {exc}"

    create_table, clear_table, inserts, select_query = create_statements(table_name, final_query)
    client = None
    cursor = None
    try:
        client = sql.connect(uri)
        cursor = client.cursor()

        cursor.execute(create_table)
        print(f"Created table '{table_name}'")

        cursor.execute(clear_table)
        print(f"Cleared existing rows from '{table_name}'")

        for statement in inserts:
            cursor.execute(statement)
        print(f"Inserted {len(DEFAULT_ROWS)} rows into '{table_name}'")

        cursor.execute(select_query)
        print(f"Query results for: {select_query}")
        if hasattr(cursor, "fetchall"):
            print_rows(cursor)
        else:
            print_rows(cursor.fetch_arrow_table())
        return True, "OK"
    except Exception as exc:
        return False, str(exc)
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


if __name__ == "__main__":
    args = parse_args()
    uri = f"grpc+tcp://{args.host}:{args.port}"

    ok, message = run_adbc_workflow(uri, args.table, args.query)
    if ok:
        sys.exit(0)
    print("ADBC attempt failed:", message)

    ok, message = run_pyarrow_flight_sql_workflow(uri, args.table, args.query)
    if ok:
        sys.exit(0)

    print("Flight SQL attempt failed:", message)
    print("Ensure pyarrow with ADBC or Flight SQL support is installed and the Flight server is reachable.")
    sys.exit(2)
