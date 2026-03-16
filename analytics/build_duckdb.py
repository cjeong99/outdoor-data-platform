import duckdb

def run():

    con = duckdb.connect("data/camping.db")

    con.execute("""
    CREATE OR REPLACE TABLE campsites_per_recarea AS
    SELECT *
    FROM read_parquet('data/gold/campsites_per_recarea.parquet')
    """)

    con.execute("""
    CREATE OR REPLACE TABLE campsites_per_facility AS
    SELECT *
    FROM read_parquet('data/gold/campsites_per_facility.parquet')
    """)

    con.execute("""
    CREATE OR REPLACE TABLE rv_sites AS
    SELECT *
    FROM read_parquet('data/gold/rv_friendly_sites.parquet')
    """)

    print("DuckDB database built")

if __name__ == "__main__":
    run()
