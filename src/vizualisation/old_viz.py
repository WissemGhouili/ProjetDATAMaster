import streamlit as st
import pandas as pd
import pydeck as pdk

# ==============================
# Chargement des données
# ==============================
@st.cache_data
def load_data():
    etablissements = pd.read_csv(
        "../../data/processed/etablissements.csv",
        dtype={
            "siren": str,
            "libellecommuneetablissement": str,
            "coordonneelambertabscisseetablissement": float,
            "coordonneelambertordonneeetablissement": float,
            "code_naf": str
        },
        low_memory=False
    )

    unite_legale = pd.read_csv(
        "../../data/processed/unite_legale.csv",
        dtype={
            "siren": str,
            "activiteprincipaleunitelegale": str,
            "denominationunitelegale": str
        },
        low_memory=False
    )

    # Jointure
    df = etablissements.merge(
        unite_legale[["siren", "activiteprincipaleunitelegale", "denominationunitelegale"]],
        on="siren",
        how="left"
    )

    # Conversion Lambert 93 -> WGS84
    from pyproj import Transformer
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    df["longitude"], df["latitude"] = transformer.transform(
        df["coordonneelambertabscisseetablissement"].values,
        df["coordonneelambertordonneeetablissement"].values
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
    df_filtered = df_filtered[df_filtered["activiteprincipaleunitelegale"] == code_naf_selected]
if commune_selected != "-- Toutes --":
    df_filtered = df_filtered[df_filtered["libellecommuneetablissement"] == commune_selected]

st.write(f"{df_filtered.shape[0]} établissements affichés")

df_filtered["icon_data"] = [{
    "url": icon_url,
    "width": 24,
    "height": 24,
    "anchorY": 24  
}] * len(df_filtered)

# ==============================
# Carte pydeck
# ==============================
layer = pdk.Layer(
    "IconLayer",
    data=df_filtered,
    get_icon="icon_data",
    get_size=2,
    size_scale=10,
    get_position=["longitude", "latitude"],
    get_fill_color=[255, 0, 0, 160],  # rouge semi-transparent
    get_radius=30,  # rayon des points
    pickable=True
)

view_state = pdk.ViewState(
    latitude=43.5, longitude=5.4, zoom=9, pitch=0
)

r = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "{denominationunitelegale}"}
)

st.pydeck_chart(r)
