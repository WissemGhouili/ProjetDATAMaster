"""Airflow DAG to fetch one establishment from SIRENE API, process with Spark, and store in PostgreSQL, with logging."""
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
import logging
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

API_BASE = "https://api.insee.fr/api-sirene/3.11"
TOKEN = "141228f3-ce06-4d71-9228-f3ce06dd7187"

PG_CONN_STRING = "postgresql+psycopg2://postgres:pwd@localhost:5432/app_data"

HEADERS = {
    "X-INSEE-Api-Key-Integration": TOKEN,
}

# Limitation : 1 requête toutes les 2 secondes
API_SLEEP_SEC = 2

log = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "start_date": datetime.now() - timedelta(days=1),
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="data_pipeline_api_example",
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:

    @task
    def get_last_update():
        """
        Récupère la date de dernière mise à jour stockée dans Variable Airflow 'last_update'.
        Si aucune valeur n'est définie, utilise la date du jour.

        Returns:
            str: Date au format 'YYYY-MM-DD'
        """
        last_update = Variable.get("last_update", default_var=None)
        if not last_update:
            last_update = datetime.today().strftime("%Y-%m-%d")
        log.info(f"Last update date utilisée : {last_update}")
        return last_update

    @task
    def fetch_one_etablissement(last_update: str):
        """
        Interroge l'API Sirene pour récupérer un établissement actif créé à partir de la date last_update.

        Args:
            last_update (str): Date limite sous format 'YYYY-MM-DD'

        Returns:
            list: Liste contenant au maximum un établissement (dict).
        """
        q = f"periode(etatAdministratifEtablissement:A) AND dateCreationEtablissement:[{last_update} TO *]"
        url = f"{API_BASE}/siret"
        params = {
            "q": q,
            "nombre": "1",  # un seul établissement
        }

        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        etablissements = data.get("etablissements", [])
        log.info(f"{len(etablissements)} établissement(s) récupéré(s) depuis l'API.")
        time.sleep(API_SLEEP_SEC)
        return etablissements

    @task
    def fetch_unite_legale_from_etab(etablissements: list):
        """
        Pour l'établissement récupéré, interroge l'API Sirene /siren pour obtenir l'unité légale correspondante.

        Args:
            etablissements (list): Liste d'établissements (dict).

        Returns:
            list: Liste contenant une unité légale (dict).
        """
        if not etablissements:
            log.warning("Aucun établissement à traiter pour unité légale.")
            return []

        siren = etablissements[0].get("siren")
        if not siren:
            log.warning("SIREN absent dans l'établissement récupéré.")
            return []

        url = f"{API_BASE}/siren"
        params = {
            "q": f"siren:{siren}",
            "nombre": "1",
        }
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        unites_legales = data.get("unitesLegales", [])
        log.info(f"{len(unites_legales)} unité(s) légale(s) récupérée(s) pour le SIREN {siren}.")
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
        """
        Met à jour la Variable Airflow 'last_update' avec la date du jour.
        """
        today = datetime.today().strftime("%Y-%m-%d")
        Variable.set("last_update", today)
        log.info(f"Variable 'last_update' mise à jour avec la date {today}")

    last_update = get_last_update()
    etablissements = fetch_one_etablissement(last_update)
    unites_legales = fetch_unite_legale_from_etab(etablissements)
    process_and_save_data(etablissements, unites_legales)
    update_last_update()
