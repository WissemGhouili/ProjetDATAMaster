"""Module to transform and insert data into final tables."""
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# =========================
# DB CONNECTION
# =========================
def get_engine() -> Engine:
    """
    Create SQLAlchemy engine for PostgreSQL.
    """
    return create_engine("postgresql://postgres:pwd@localhost:5432/app_data")

# Global variable to store siren sets between functions
#siren_filter_set = set()

# =========================
# ETABLISSEMENTS
# =========================
def process_etablissements(engine: Engine, chunksize: int = 10_000):
    """
    Clean and insert etablissements_raw into etablissements table in batches.
    Keep only active establishments (etatAdministratifEtablissement = 'A') and filter
    by libelleCommuneEtablissement.
    Return the DataFrame filtered for next steps.
    """
    #global siren_filter_set

    communes_cibles = ("MARSEILLE", "AIX-EN-PROVENCE")
    df_final = pd.DataFrame()

    query = """
        SELECT * 
        FROM etablissements_raw
        WHERE etatAdministratifEtablissement = 'A'
          AND libelleCommuneEtablissement IN ('MARSEILLE', 'AIX-EN-PROVENCE');
    """

    for chunk in pd.read_sql(query, engine, chunksize=chunksize):
        print(chunk.columns.tolist())
        df = chunk[[
            "siret", "siren", "etatadministratifetablissement", "datecreationetablissement", "trancheeffectifsetablissement",
            "anneeeffectifsetablissement", "complementadresseetablissement", "numerovoieetablissement",
            "typevoieetablissement", "libellevoieetablissement", "codepostaletablissement",
            "libellecommuneetablissement", "coordonneelambertabscisseetablissement",
            "coordonneelambertordonneeetablissement"
        ]].copy()

        print("fin du import etablissement")
        
        # Clean dates
        df["datecreationetablissement"] = pd.to_datetime(df["datecreationetablissement"], errors="coerce")

        # Keep only valid postal codes (5 digits)
        df.loc[df["codepostaletablissement"].str.len() != 5, "codepostaletablissement"] = None

        # Drop rows with invalid coordinates (non-numeric or null)
        df["coordonneelambertabscisseetablissement"] = pd.to_numeric(
            df["coordonneelambertabscisseetablissement"], errors="coerce"
        )
        df["coordonneelambertordonneeetablissement"] = pd.to_numeric(
            df["coordonneelambertordonneeetablissement"], errors="coerce"
        )

        df = df.dropna(subset=["siret", "siren"])
        
        # Append chunk to final DataFrame
        df_final = pd.concat([df_final, df], ignore_index=True)
        print("fin du clean etablissement")
        
    # Save set of siren to filter
    #siren_filter_set = set(df_final['siren'].unique())
    
    return df_final

# =========================
# UNITE LEGALE
# =========================
def process_unite_legale(engine: Engine, siren_filter_set: set, chunksize: int = 10_000):
    """
    Clean and insert unite_legale_raw into unite_legale table in batches.
    Keep only active companies (etatAdministratifUniteLegale = 'A') and filter on siren from etablissements.
    """
    #global siren_filter_set

    # If siren_filter_set is empty, no data to process
    if not siren_filter_set:
        print("No siren filter defined. Skipping unite_legale processing.")
        return

    siren_list = tuple(siren_filter_set)
    # Create a parameterized query with these sirens and etatAdministratif
    # Use list formatting for IN clause safely via SQLAlchemy (or restrict size if too large)
    # Here is a safe sliced approach if siren_list is too long (not shown for simplicity)
    query = f"""
        SELECT * 
        FROM unite_legale_raw
        WHERE etatAdministratifUniteLegale = 'A'
          AND siren IN {siren_list};
    """

    for chunk in pd.read_sql(query, engine, chunksize=chunksize):
        df = chunk[[
            "siren", "denominationunitelegale", "datecreationunitelegale",
            "activiteprincipaleunitelegale", "categorieentreprise",
            "anneecategorieentreprise", "trancheeffectifsunitelegale",
            "anneeeffectifsunitelegale", "etatadministratifunitelegale"
        ]].copy()
        print("fin import unite")
        # Clean
        df["datecreationunitelegale"] = pd.to_datetime(df["datecreationunitelegale"], errors="coerce")
        df = df.dropna(subset=["siren"])
        print("fin du clean unite")
        # Insert
        df.to_sql("unite_legale", engine, if_exists="append", index=False, method="multi")
        print(f"Inserted {len(df)} rows into unite_legale")

# =========================
# DONNEES FINANCIERES
# =========================
def process_donnees_financieres(engine: Engine, siren_filter_set: set, chunksize: int = 200_000):
    """
    Clean and insert donnees_financieres_raw into donnees_financieres table in batches.
    Filter on siren from etablissements.
    """
    #global siren_filter_set

    if not siren_filter_set:
        print("No siren filter defined. Skipping donnees_financieres processing.")
        return

    siren_list = tuple(siren_filter_set)

    query = f"""
        SELECT * 
        FROM donnees_financieres_raw
        WHERE siren IN {siren_list};
    """

    for chunk in pd.read_sql(query, engine, chunksize=chunksize):
        df = chunk[[
            "siren", "date_cloture_exercice", "chiffre_d_affaires", "resultat_net"
        ]].copy()

        print("fin du import inpi")

        # Clean
        df["date_cloture_exercice"] = pd.to_datetime(df["date_cloture_exercice"], errors="coerce")
        df["chiffre_d_affaires"] = pd.to_numeric(df["chiffre_d_affaires"], errors="coerce")
        df["resultat_net"] = pd.to_numeric(df["resultat_net"], errors="coerce")

        df = df.dropna(subset=["siren"])
        print("fin du clean inpi")
        # Insert
        df.to_sql("donnees_financieres", engine, if_exists="append", index=False, method="multi")
        print(f"Inserted {len(df)} rows into donnees_financieres")

# =========================
# MAIN PIPELINE
# =========================
def run_pipeline():
    """
    Run the full data processing pipeline
    1 Process etablissements_raw to etablissements
    2 Process unite_legale_raw to unite_legale using sirens from etablissements
    3 Process donnees_financieres_raw to donnees_financieres using sirens from etablissements
    4 Insert filtered etablissements into final table
    """
    engine = get_engine()

    print("Processing etablissements ...")
    df_etablissements = process_etablissements(engine)
    siren_filter_set = set(df_etablissements['siren'].unique())

    print("Processing unite_legale ...")
    process_unite_legale(engine, siren_filter_set)

    # Insert into the final table
    df_etablissements.to_sql("etablissements", engine, if_exists="append", index=False, method="multi")
    print(f"Inserted {len(df_etablissements)} rows into etablissements")

    print("Processing donnees_financieres ...")
    process_donnees_financieres(engine, siren_filter_set)
    print("All data cleaned and inserted into final tables")

if __name__ == "__main__":
    run_pipeline()
