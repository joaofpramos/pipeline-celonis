import duckdb

def connect_to_duckdb(db_path):
    """Connect to the DuckDB database."""
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        print(f"Connected to DuckDB at {db_path}")
        return con
    except Exception as e:
        print(f"Failed to connect to DuckDB: {e}")
        return None

def show_schema(con):
    """Show the schema of the database."""
    try:
        schema = con.execute("PRAGMA show_tables;").fetchall()
        print("Schema:")
        for table in schema:
            print(table[0])
    except Exception as e:
        print(f"Failed to show schema: {e}")

def show_table_data(con, table_name):
    """Show data from a specific table."""
    try:
        data = con.execute(f"SELECT * FROM {table_name} LIMIT 5;").fetchdf()
        print(f"Data from {table_name}:")
        print(data)
    except Exception as e:
        print(f"Failed to show data from {table_name}: {e}")

def list_all_tables(con):
    """List all tables in the schema."""
    try:
        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()
        print("All tables in the schema:")
        for table in tables:
            print(table[0])
    except Exception as e:
        print(f"Failed to list all tables: {e}")

def show_materialized_view(con, view_name):
    """Show data from a materialized view."""
    try:
        data = con.execute(f"SELECT * FROM {view_name} LIMIT 5;").fetchdf()
        print(f"Data from materialized view {view_name}:")
        print(data)
    except Exception as e:
        print(f"Failed to show data from materialized view {view_name}: {e}")

def main():
    db_path = "data/gold/sales_product.duckdb"
    con = connect_to_duckdb(db_path)
    if con is None:
        return

    # Show schema
    show_schema(con)

    # List all tables
    list_all_tables(con)

    # Show data from a specific table (example: sales_2024_01)
    show_table_data(con, "sales_2024_01")

    # Show data from the materialized view
    show_materialized_view(con, "vw_sales_2024")

    # Close the connection
    con.close()

if __name__ == "__main__":
    main()