import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_dataframe(path):
    """
    Loads a CSV file into a pandas DataFrame.
    """
    return pd.read_csv(path)


def merge_dataframes_on_siren(df_inpi, df_insee):
    """
    Merges two DataFrames on the 'siren' column.
    """
    return pd.merge(df_inpi, df_insee, on="siren", how="inner")


def save_dataframe_to_postgres(df):
    """
    Saves the DataFrame to a PostgreSQL database using SQLAlchemy.
    """
    postgres_user = "postgres"
    postgres_password = "pw"
    postgres_db_name = "project_data_env"
    postgres_port = 5432
    postgres_host = "postgres"

    connection_string = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db_name}"
    engine = create_engine(connection_string)

    df.to_sql("merged_data", engine, if_exists="replace", index=False)
