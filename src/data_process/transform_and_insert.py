"""Module to transform and insert data into final tables using PySpark."""


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, to_date, when
from typing import Set


# =========================
# CREATE SPARK SESSION & DB CONFIG
# =========================


def get_spark():
    """Create and configures a Spark session."""
    return SparkSession.builder.appName("transform-and-insert").getOrCreate()


DB_URI = "jdbc:postgresql://localhost:5432/app_data"
DB_USER = "postgres"
DB_PWD = "pwd"


# =========================
# ETABLISSEMENTS PROCESSING
# =========================


def process_etablissements(spark: SparkSession, communes_cibles=("MARSEILLE", "AIX-EN-PROVENCE")):
    """
    Clean and insert etablissements_raw into etablissements table.

    Keep only active establishments and filter by commune.
    Returns Spark DataFrame filtered.
    """
    query = """
    (
        SELECT * FROM etablissements_raw
        WHERE etatAdministratifEtablissement = 'A'
        AND libelleCommuneEtablissement IN ('MARSEILLE', 'AIX-EN-PROVENCE')
    ) AS filtered_etablissements
    """
    df = (
        spark.read.format("jdbc")
        .option("url", DB_URI)
        .option("dbtable", query)
        .option("user", DB_USER)
        .option("password", DB_PWD)
        .load()
    )

    wanted_cols = [
        "siret",
        "siren",
        "etatAdministratifEtablissement",
        "dateCreationEtablissement",
        "trancheEffectifsEtablissement",
        "anneeEffectifsEtablissement",
        "complementAdresseEtablissement",
        "numeroVoieEtablissement",
        "typeVoieEtablissement",
        "libelleVoieEtablissement",
        "codePostalEtablissement",
        "libelleCommuneEtablissement",
        "coordonneeLambertAbscisseEtablissement",
        "coordonneeLambertOrdonneeEtablissement",
    ]
    cols = [c for c in wanted_cols if c in df.columns]
    df = df.select(*cols)

    # Clean date
    df = df.withColumn("dateCreationEtablissement", to_date(col("dateCreationEtablissement")))

    # Code postal valid 5 digits or null
    df = df.withColumn("codePostalEtablissement", col("codePostalEtablissement").cast("string"))
    df = df.withColumn(
        "codePostalEtablissement",
        when(length(col("codePostalEtablissement")) == 5, col("codePostalEtablissement")),
    )

    # Cast coordinates
    df = df.withColumn("coordonneeLambertAbscisseEtablissement", col("coordonneeLambertAbscisseEtablissement").cast("double"))
    df = df.withColumn("coordonneeLambertOrdonneeEtablissement", col("coordonneeLambertOrdonneeEtablissement").cast("double"))

    # Drop rows with null siren or siret
    df = df.dropna(subset=["siret", "siren"])

    return df


# =========================
# UNITE LEGALE PROCESSING
# =========================


def process_unite_legale(spark: SparkSession, siren_filter_set: Set[str]):
    """
    Clean and insert unite_legale_raw into unite_legale table.

    Keep only active and filtered by siren.
    """
    if not siren_filter_set:
        print("No siren filter defined. Skipping unite_legale processing.")
        return

    df = (
        spark.read.format("jdbc")
        .option("url", DB_URI)
        .option("dbtable", "unite_legale_raw")
        .option("user", DB_USER)
        .option("password", DB_PWD)
        .load()
    )

    df = df.filter((col("etatAdministratifUniteLegale") == "A") & (col("siren").isin(list(siren_filter_set))))

    wanted_cols = [
        "siren",
        "denominationUniteLegale",
        "dateCreationUniteLegale",
        "activitePrincipaleUniteLegale",
        "categorieEntreprise",
        "anneeCategorieEntreprise",
        "trancheEffectifsUniteLegale",
        "anneeEffectifsUniteLegale",
        "etatAdministratifUniteLegale",
    ]
    cols = [c for c in wanted_cols if c in df.columns]
    df = df.select(*cols)

    df = df.withColumn("dateCreationUniteLegale", to_date(col("dateCreationUniteLegale")))
    df = df.dropna(subset=["siren"])

    df.write.format("jdbc").option("url", DB_URI).option("dbtable", "unite_legale").option("user", DB_USER).option(
        "password", DB_PWD
    ).mode("append").save()

    print(f"Inserted {df.count()} rows into unite_legale")


# =========================
# DONNEES FINANCIERES PROCESSING
# =========================


def process_donnees_financieres(spark: SparkSession, siren_filter_set: Set[str]):
    """
    Clean and insert donnees_financieres_raw into donnees_financieres table.

    Filter on siren.
    """
    if not siren_filter_set:
        print("No siren filter defined. Skipping donnees_financieres processing.")
        return

    df = (
        spark.read.format("jdbc")
        .option("url", DB_URI)
        .option("dbtable", "donnees_financieres_raw")
        .option("user", DB_USER)
        .option("password", DB_PWD)
        .load()
    )

    df = df.filter(col("siren").isin(list(siren_filter_set)))

    wanted_cols = ["siren", "date_cloture_exercice", "chiffre_d_affaires", "resultat_net"]
    cols = [c for c in wanted_cols if c in df.columns]
    df = df.select(*cols)

    df = df.withColumn("date_cloture_exercice", to_date(col("date_cloture_exercice")))
    df = df.withColumn("chiffre_d_affaires", col("chiffre_d_affaires").cast("double"))
    df = df.withColumn("resultat_net", col("resultat_net").cast("double"))
    df = df.dropna(subset=["siren"])

    df.write.format("jdbc").option("url", DB_URI).option("dbtable", "donnees_financieres").option("user", DB_USER).option(
        "password", DB_PWD
    ).mode("append").save()

    print(f"Inserted {df.count()} rows into donnees_financieres")


# =========================
# MAIN PIPELINE
# =========================


def run_pipeline():
    """
    Run the full data processing pipeline.

    1 Process etablissements_raw to etablissements.
    2 Process unite_legale_raw to unite_legale using sirens from etablissements.
    3 Process donnees_financieres_raw to donnees_financieres using sirens from etablissements.
    4 Insert filtered etablissements into final table.
    """
    spark = get_spark()

    print("Processing etablissements ...")
    df_etablissements = process_etablissements(spark)

    siren_filter_set = set(row["siren"] for row in df_etablissements.select("siren").distinct().collect())

    print("Processing unite_legale ...")
    process_unite_legale(spark, siren_filter_set)

    print("Processing donnees_financieres ...")
    process_donnees_financieres(spark, siren_filter_set)

    print("Inserting etablissements ...")
    df_etablissements.write.format("jdbc").option("url", DB_URI).option("dbtable", "etablissements").option(
        "user", DB_USER
    ).option("password", DB_PWD).mode("append").save()
    print(f"Inserted {df_etablissements.count()} rows into etablissements")

    print("All data cleaned and inserted into final tables")


if __name__ == "__main__":
    run_pipeline()
