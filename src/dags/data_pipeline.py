"""Airflow DAG for ETL/ELT data pipeline."""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

DB_URI = "postgresql://postgres:pwd@postgres_app_data:5432/app_data"

# =========================
# FUNCTIONS
# =========================

def extract_raw_data():
    """
    Extract raw data from the database and save to temporary CSV files.
    """
    engine = create_engine(DB_URI)
    unite_legale = pd.read_sql("SELECT * FROM unite_legale_raw WHERE etatAdministratifUniteLegale = 'A';", engine)
    etablissements = pd.read_sql("SELECT * FROM etablissements_raw WHERE etatAdministratifEtablissement = 'A';", engine)
    donnees_financieres = pd.read_sql("SELECT * FROM donnees_financieres_raw;", engine)

    unite_legale.to_csv("/tmp/unite_legale.csv", index=False)
    etablissements.to_csv("/tmp/etablissements.csv", index=False)
    donnees_financieres.to_csv("/tmp/donnees_financieres.csv", index=False)


def transform_clean_data():
    """ 
    Clean and transform raw data, then save cleaned data to temporary CSV files."""
    # Chargement
    unite_legale = pd.read_csv("/tmp/unite_legale.csv")
    etablissements = pd.read_csv("/tmp/etablissements.csv")
    donnees_financieres = pd.read_csv("/tmp/donnees_financieres.csv")

    # Nettoyage établissements
    etablissements = etablissements[etablissements["codePostalEtablissement"].astype(str).str.len() <= 5]
    etablissements = etablissements.dropna(subset=["coordonneeLambertAbscisseEtablissement",
                                                   "coordonneeLambertOrdonneeEtablissement"])

    # Nettoyage INPI : ex. enlever les lignes avec CA négatif
    donnees_financieres = donnees_financieres[donnees_financieres["Chiffre_d_affaires"] >= 0]

    # Sauvegarde
    unite_legale.to_csv("/tmp/unite_legale_clean.csv", index=False)
    etablissements.to_csv("/tmp/etablissements_clean.csv", index=False)
    donnees_financieres.to_csv("/tmp/donnees_financieres_clean.csv", index=False)


def load_final_tables():
    """
    Load cleaned data into final database tables.
    """
    engine = create_engine(DB_URI)
    # Lecture fichiers nettoyés
    unite_legale = pd.read_csv("/tmp/unite_legale_clean.csv")
    etablissements = pd.read_csv("/tmp/etablissements_clean.csv")
    donnees_financieres = pd.read_csv("/tmp/donnees_financieres_clean.csv")

    # Insert optimisé (par chunks)
    unite_legale.to_sql("unite_legale", engine, if_exists="append", index=False, chunksize=10000, method="multi")
    etablissements.to_sql("etablissements", engine, if_exists="append", index=False, chunksize=10000, method="multi")
    donnees_financieres.to_sql("donnees_financieres", engine, if_exists="append", index=False, chunksize=10000, method="multi")


# =========================
# DAG DEFINITION
# =========================
with DAG(
    "data_pipeline",
    default_args={"owner": "airflow", "retries": 1},
    description="Pipeline ETL/ELT des données entreprises",
    schedule_interval="@daily",  # planification quotidienne
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_raw_data
    )

    t2 = PythonOperator(
        task_id="transform_clean_data",
        python_callable=transform_clean_data
    )

    t3 = PythonOperator(
        task_id="load_final_tables",
        python_callable=load_final_tables
    )

    t1 >> t2 >> t3
