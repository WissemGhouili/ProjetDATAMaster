"""Module for visualizations."""

import streamlit as st
import pandas as pd


# ==============================
# Chargement des données
# ==============================

@st.cache_data
def load_data():
    """Load and merge establishment and legal unit data, then convert coordinates.

    Returns:
        pd.DataFrame: Processed DataFrame with latitude and longitude columns.
    """
    etablissements = pd.read_csv(
        "../../data/processed/etablissements.csv",
        dtype={
            "siren": str,
            "libellecommuneetablissement": str,
            "coordonneelambertabscisseetablissement": float,
            "coordonneelambertordonneeetablissement": float,
            "code_naf": str,
        },
        low_memory=False,
    )

    unite_legale = pd.read_csv(
        "../../data/processed/unite_legale.csv",
        dtype={
            "siren": str,
            "activiteprincipaleunitelegale": str,
            "denominationunitelegale": str,
        },
        low_memory=False,
    )

    # Jointure
    df = etablissements.merge(
        unite_legale[
            ["siren", "activiteprincipaleunitelegale", "denominationunitelegale"]
        ],
        on="siren",
        how="left",
    )

    # Conversion Lambert 93 -> WGS84
    from pyproj import Transformer

    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    df["longitude"], df["latitude"] = transformer.transform(
        df["coordonneelambertabscisseetablissement"].values,
        df["coordonneelambertordonneeetablissement"].values,
    )

    return df.dropna(subset=["latitude", "longitude"])


etablissements_full = load_data()


# ==============================
# UI Streamlit
# ==============================

st.title("Dashboard établissements - Bouches-du-Rhône")

st.sidebar.header("Filtres")
icon_url = "https://cdn-icons-png.flaticon.com/512/684/684908.png"

# Filtres NAF
code_naf_list = sorted(etablissements_full["activiteprincipaleunitelegale"].dropna().unique())
code_naf_selected = st.sidebar.selectbox("Code NAF", ["-- Aucun --"] + code_naf_list)

# Filtres communes
communes = ["AIX-EN-PROVENCE", "MARSEILLE"]
commune_selected = st.sidebar.selectbox("Commune", ["-- Toutes --"] + communes)

# Bouton reset
if st.sidebar.button("Réinitialiser les filtres"):
    code_naf_selected = "-- Aucun --"
    commune_selected = "-- Toutes --"

# Application des filtres
df_filtered = etablissements_full.copy()

if code_naf_selected != "-- Aucun --":
    df_filtered = df_filtered
