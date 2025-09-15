"""Airflow DAG to fetch data from SIRENE API, process with Spark, and store in PostgreSQL."""
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import requests
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import StringIO
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env


API_BASE = "https://api.insee.fr/api-sirene/3.11"
TOKEN = os.getenv("INSEE_API_KEY")

PG_CONN_STRING = "postgresql+psycopg2://postgres:pwd@localhost:5432/app_data"

HEADERS = {
    "X-INSEE-Api-Key-Integration": TOKEN,
}

# Respecte limitation 1 requête toutes les 2 secondes
API_SLEEP_SEC = 2



default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(dag_id="data_pipeline_api", schedule="@daily", default_args=default_args, catchup=False) as dag:

    @task
    def get_last_update():
        """Get last update date from Airflow Variable or default to today."""
        # Récupère la date de dernière mise à jour (format YYYY-MM-DD)
        last_update = Variable.get("last_update", default_var=None)
        if not last_update:
            last_update = datetime.today().strftime("%Y-%m-%d")
        return last_update

    @task
    def fetch_new_etablissements(last_update: str):
        """Fetch new or updated etablissements since last_update."""
        etablissements = []
        nombre = 100  # nombre max par page 
        debut = 0

        while True:
            q = f"periode(etatAdministratifEtablissement:A) AND dateCreationEtablissement:[{last_update} TO *]"
            url = f"{API_BASE}/siret"
            params = {
                "q": q,
                "nombre": nombre,
                "debut": debut,
            }
            resp = requests.get(url, headers=HEADERS, params=params)
            resp.raise_for_status()
            data = resp.json()

            etablissements.extend(data.get("etablissements", []))

            # Pagination
            total = data.get("total", 0)
            debut += nombre
            if debut >= total:
                break

            time.sleep(API_SLEEP_SEC)
        return etablissements

    @task
    def extract_unique_sirens(etablissements: list):
        """Extract unique sirens from etablissements list."""
        sirens = set()
        for etab in etablissements:
            siren = etab.get("siren")
            if siren:
                sirens.add(siren)
        return list(sirens)

    @task
    def fetch_unites_legales(sirens: list):
        """Fetch unites_legales details for given sirens."""
        unites_legales = []
        batch_size = 50  # lot raisonnable pour éviter surcharge API
        for i in range(0, len(sirens), batch_size):
            batch = sirens[i : i + batch_size]
            q = " OR ".join(f"siren:{siren}" for siren in batch)
            url = f"{API_BASE}/siren"
            params = {"q": q}
            resp = requests.get(url, headers=HEADERS, params=params)
            resp.raise_for_status()
            data = resp.json()
            unites_legales.extend(data.get("unitesLegales", []))
            time.sleep(API_SLEEP_SEC)
        return unites_legales

    @task
    def process_and_save_data(etablissements: list, unites_legales: list):
        """ 
        Traite les données récupérées avec Spark, filtre les actifs, et sauvegarde dans PostgreSQL.

        Args:
            etablissements (list): Liste des établissements.
            unites_legales (list): Liste des unités légales.
        """
        log = logging.getLogger("airflow.task")

        if not etablissements or not unites_legales:
            log.warning("Pas de données à traiter.")
            return

        etab_info = etablissements[0]
        siren = etab_info.get("siren", "N/A")
        nom = etab_info.get("denominationUniteLegale", "N/A")
        log.info(f"Début traitement et import de l'établissement SIREN={siren}, Nom={nom}")

        keep_etab = [
            "siret", "siren", "etatAdministratifEtablissement",
            "dateCreationEtablissement", "trancheEffectifsEtablissement",
            "anneeEffectifsEtablissement", "complementAdresseEtablissement",
            "numeroVoieEtablissement", "typeVoieEtablissement",
            "libelleVoieEtablissement", "codePostalEtablissement",
            "coordonneeLambertAbscisseEtablissement",
            "coordonneeLambertOrdonneeEtablissement",
        ]
        keep_unite = [
            "siren", "denominationniteLegale", "dateCreationUniteLegale",
            "activitePrincipaleUniteLegale", "categorieEntreprise",
            "anneeCategorieEntreprise", "trancheEffectifsUniteLegale",
            "anneeEffectifsUniteLegale", "nomenclatureactiviteprincipaleunitelegale",
            "etatAdministratifUniteLegale",
        ]

        df_etab = pd.json_normalize(etablissements).reindex(columns=keep_etab)
        df_unite = pd.json_normalize(unites_legales).reindex(columns=keep_unite)

        df_etab["dateCreationEtablissement"] = pd.to_datetime(
            df_etab["dateCreationEtablissement"], errors="coerce"
        )
        df_etab = df_etab[df_etab["etatAdministratifEtablissement"] == "A"]

        df_unite["dateCreationUniteLegale"] = pd.to_datetime(
            df_unite["dateCreationUniteLegale"], errors="coerce"
        )

        # Connexion via PostgresHook (défini dans Airflow Connections UI sous "postgres_default")
        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Sauvegarde df_etab
        if not df_etab.empty:
            buffer = StringIO()
            df_etab.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(
                "COPY etablissements FROM STDIN WITH CSV",
                buffer
            )

        # Sauvegarde df_unite
        if not df_unite.empty:
            buffer = StringIO()
            df_unite.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cursor.copy_expert(
                "COPY unite_legale FROM STDIN WITH CSV",
                buffer
            )

        conn.commit()
        cursor.close()
        conn.close()

        log.info(f"Import terminé de l'établissement SIREN={siren}")

    @task
    def update_last_update():
        """Update last_update Airflow Variable to today."""
        today = datetime.today().strftime("%Y-%m-%d")
        Variable.set("last_update", today)

    last_update = get_last_update()
    etablissements = fetch_new_etablissements(last_update)
    sirens = extract_unique_sirens(etablissements)
    unites_legales = fetch_unites_legales(sirens)
    process_and_save_data(etablissements, unites_legales)
    update_last_update()
