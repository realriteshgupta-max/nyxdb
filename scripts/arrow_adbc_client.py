#!/usr/bin/env python3
"""Arrow ADBC/Flight SQL test client

Attempts to use pyarrow.adbc if available, otherwise falls back to
pyarrow.flight.sql to connect to a Flight SQL server and run a query.

Usage:
  python scripts/arrow_adbc_client.py --host localhost --port 8815 --query "SELECT * FROM users"

"""
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="localhost")
parser.add_argument("--port", type=int, default=8815)
parser.add_argument("--query", default="SELECT COUNT(*) FROM users")
args = parser.parse_args()

uri = f"grpc+tcp://{args.host}:{args.port}"

# Try ADBC
try:
    import pyarrow.adbc as adbc
    has_adbc = True
except Exception:
    has_adbc = False

if has_adbc:
    print("Using pyarrow.adbc")
    try:
        # Driver name may vary depending on pyarrow build; try common names
        for driver_name in ("flight", "flight-sql", "adbc-flight", "adbc-flight-sql"):
            try:
                conn = adbc.connect(driver_name, {"uri": uri})
                print(f"Connected via ADBC driver '{driver_name}'")
                break
            except Exception:
                conn = None
        if conn is None:
            raise RuntimeError("No ADBC Flight SQL driver found")

        # Use a simple execute/fetch pattern if available
        try:
            cur = conn.cursor()
            cur.execute(args.query)
            # fetchall may or may not exist depending on implementation
            rows = None
            if hasattr(cur, "fetchall"):
                rows = cur.fetchall()
            elif hasattr(cur, "fetch_arrow_table"):
                tbl = cur.fetch_arrow_table()
                print(tbl.to_pandas())
                rows = None
            else:
                print("Query executed (no fetch API available on this ADBC cursor)")
            if rows is not None:
                for r in rows:
                    print(r)
            cur.close()
            conn.close()
            sys.exit(0)
        except Exception as e:
            print("ADBC query error:", e)
    except Exception as e:
        print("ADBC connection error:", e)

# Fallback: Flight SQL client
try:
    from pyarrow.flight import sql
    print("Falling back to pyarrow.flight.sql")
    client = sql.connect(uri)
    cur = client.cursor()
    cur.execute(args.query)
    # try fetchall, otherwise fetch_arrow_table
    if hasattr(cur, "fetchall"):
        for row in cur.fetchall():
            print(row)
    else:
        try:
            tbl = cur.fetch_arrow_table()
            print(tbl.to_pandas())
        except Exception:
            print("Query executed but could not fetch results via cursor API")
    cur.close()
    client.close()
except Exception as e:
    print("Flight SQL client error:", e)
    print("Ensure pyarrow with Flight/ADBC support is installed and the Flight server is running.")
    sys.exit(2)
