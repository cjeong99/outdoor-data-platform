import duckdb

def run():

    con = duckdb.connect()

    df = con.execute("""
        SELECT *
        FROM 'data/gold/campsites_per_recarea.parquet'
        ORDER BY total_campsites DESC
        LIMIT 10
    """).df()

    print("Top Parks by Campsites")
    print(df)


if __name__ == "__main__":
    run()
