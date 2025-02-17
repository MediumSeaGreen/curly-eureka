import duckdb
import pandas as pd


def connect():
    pipeline_name = "ny_taxi_pipeline"
    dataset_name = "ny_taxi_data"
    conn = duckdb.connect(f"{pipeline_name}.duckdb")
    conn.execute(f"SET search_path = '{dataset_name}'")
    return conn


def question_two(conn):
    df = conn.execute("DESCRIBE").fetchdf()
    pd.set_option("display.max_columns", None)
    print(df)


def question_three(conn):
    result = conn.execute("SELECT COUNT(*) FROM rides").fetchone()
    print(result[0])


def question_four(conn):
    result = conn.execute("""
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM rides;
            """).fetchone()
    print(result[0])


if __name__ == "__main__":
    # question_two(connect())
    # question_three(connect())
    question_four(connect())
