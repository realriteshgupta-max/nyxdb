"""Flight SQL client — tries ADBC then Flight SQL

This script connects to a Flight SQL server and runs a query.
It prefers ADBC (pyarrow.adbc) when available, otherwise falls back
to pyarrow.flight.sql.
"""
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", default=8815, type=int)
parser.add_argument("--query", default="SELECT * FROM users LIMIT 10")
args = parser.parse_args()

uri = f"grpc+tcp://{args.host}:{args.port}"

def try_adbc(uri, query):
    try:
        import pyarrow.adbc as adbc
    except Exception as e:
        return False, f"ADBC not available: {e}"

    # Try common driver names
    for driver in ("flight", "flight-sql", "adbc-flight", "adbc-flight-sql"):
        try:
            conn = adbc.connect(driver, {"uri": uri})
            cur = conn.cursor()
            cur.execute(query)
            if hasattr(cur, "fetch_arrow_table"):
                tbl = cur.fetch_arrow_table()
                print(tbl.to_pandas())
            elif hasattr(cur, "fetchall"):
                for r in cur.fetchall():
                    print(r)
            cur.close()
            conn.close()
            return True, "OK"
        except Exception:
            continue
    return False, "No ADBC Flight SQL driver found"

def try_flight_sql(uri, query):
    try:
        from pyarrow import flight
    except Exception as e:
        return False, f"pyarrow Flight not available: {e}"

    try:
        client = flight.connect(uri)
        # Use FlightDescriptor command form so server reads descriptor.getCommand()
        descriptor = flight.FlightDescriptor.for_command(query.encode())
        info = client.get_flight_info(descriptor)
        # If no endpoints, try do_action (execute)
        if not info.endpoints:
            # try as action (for UPDATE/INSERT)
            try:
                for res in client.do_action(flight.Action("execute", query.encode())):
                    print(res.to_pybytes().decode())
                return True, "OK"
            except Exception as e:
                return False, str(e)

        ticket = info.endpoints[0].ticket
        # ticket may be None; if so, construct ticket with SQL bytes
        if ticket is None or ticket.ticket is None:
            t = flight.Ticket(query.encode())
        else:
            t = ticket

        reader = client.do_get(t)
        table = reader.read_all()
        try:
            print(table.to_pandas())
        except Exception:
            print(table)
        return True, "OK"
    except Exception as e:
        return False, str(e)

if __name__ == "__main__":
    ok, msg = try_adbc(uri, args.query)
    if ok:
        sys.exit(0)
    print("ADBC attempt failed:", msg)
    ok, msg = try_flight_sql(uri, args.query)
    if ok:
        sys.exit(0)
    print("Flight SQL attempt failed:", msg)
    print("Ensure pyarrow with Flight/ADBC support is installed and the Flight server is reachable.")
    sys.exit(2)
