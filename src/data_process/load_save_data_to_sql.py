import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()  # Load environment variables from .env

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

    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db_name = os.getenv("POSTGRES_DB")
    postgres_port = 5432
    postgres_host = "localhost"
    if not (postgres_user and postgres_password and postgres_db_name) :
        raise ValueError("Database login not found in environment variables.")

    connection_string = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db_name}"
    engine = create_engine(connection_string)

    df.to_sql("entreprise", engine, if_exists="replace", index=False)
