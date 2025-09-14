"""Airflow DAG for ETL/ELT data pipeline with Spark."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import length

DB_URI = "jdbc:postgresql://postgres_app_data:5432/app_data"
DB_USER = "postgres"
DB_PWD = "pwd"


def get_spark():
    return SparkSession.builder.appName("data-pipeline").getOrCreate()


# =========================
# FUNCTIONS
# =========================


def extract_raw_data():
    """Extract raw data from the database and save to temporary CSV files using Spark."""
    spark = get_spark()
    unite_legale = spark.read.format("jdbc") \
        .option("url", DB_URI) \
        .option("dbtable", "(SELECT * FROM unite_legale_raw WHERE etatAdministratifUniteLegale = 'A') as sub") \
        .option("user", DB_USER) \
        .option("password", DB_PWD) \
        .load()
    etablissements = spark.read.format("jdbc") \
        .option("url", DB_URI) \
        .option("dbtable", "(SELECT * FROM etablissements_raw WHERE etatAdministratifEtablissement = 'A') as sub") \
        .option("user", DB_USER) \
        .option("password", DB_PWD) \
        .load()
    donnees_financieres = spark.read.format("jdbc") \
        .option("url", DB_URI) \
        .option("dbtable", "donnees_financieres_raw") \
        .option("user", DB_USER) \
        .option("password", DB_PWD) \
        .load()
    unite_legale.write.csv("/tmp/unite_legale.csv", header=True, mode="overwrite")
    etablissements.write.csv("/tmp/etablissements.csv", header=True, mode="overwrite")
    donnees_financieres.write.csv("/tmp/donnees_financieres.csv", header=True, mode="overwrite")


def transform_clean_data():
    """Clean and transform raw data, then save cleaned data to temporary CSV files using Spark."""
    spark = get_spark()
    unite_legale = spark.read.csv("/tmp/unite_legale.csv", header=True, inferSchema=True)
    etablissements = spark.read.csv("/tmp/etablissements.csv", header=True, inferSchema=True)
    donnees_financieres = spark.read.csv("/tmp/donnees_financieres.csv", header=True, inferSchema=True)

    # Nettoyage établissements - code postal <= 5, coordonnées non NA
    etablissements = etablissements.filter(etablissements["codePostalEtablissement"].isNotNull()) \
        .filter(length(etablissements["codePostalEtablissement"].cast("string")) <= 5)
    etablissements = etablissements.dropna(subset=["coordonneeLambertAbscisseEtablissement", "coordonneeLambertOrdonneeEtablissement"])

    # Nettoyage INPI : CA >= 0
    from pyspark.sql.functions import col
    donnees_financieres = donnees_financieres.filter(col("Chiffre_d_affaires") >= 0)

    unite_legale.write.csv("/tmp/unite_legale_clean.csv", header=True, mode="overwrite")
    etablissements.write.csv("/tmp/etablissements_clean.csv", header=True, mode="overwrite")
    donnees_financieres.write.csv("/tmp/donnees_financieres_clean.csv", header=True, mode="overwrite")


def load_final_tables():
    """Load cleaned data into final database tables using Spark."""
    spark = get_spark()
    unite_legale = spark.read.csv("/tmp/unite_legale_clean.csv", header=True, inferSchema=True)
    etablissements = spark.read.csv("/tmp/etablissements_clean.csv", header=True, inferSchema=True)
    donnees_financieres = spark.read.csv("/tmp/donnees_financieres_clean.csv", header=True, inferSchema=True)
    for df, table in [
        (unite_legale, "unite_legale"),
        (etablissements, "etablissements"),
        (donnees_financieres, "donnees_financieres"),
    ]:
        df.write.format("jdbc") \
            .option("url", DB_URI) \
            .option("dbtable", table) \
            .option("user", DB_USER) \
            .option("password", DB_PWD) \
            .mode("append") \
            .save()


# =========================
# DAG DEFINITION
# =========================


with DAG(
    "data_pipeline",
    default_args={"owner": "airflow", "retries": 1},
    description="Pipeline ETL/ELT des données entreprises",
    schedule_interval="@daily", 
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
