import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt
from streamlit_option_menu import option_menu

st.set_page_config(layout="wide")

# ======
# Chargement des données
# ======
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
            "siret": str
        },
        low_memory=False
    )
    unite_legale = pd.read_csv(
        "../../data/processed/unite_legale.csv",
        dtype={
            "siren": str,
            "activiteprincipaleunitelegale": str,
            "denominationunitelegale": str,
            "categorieentreprise": str,
            "anneecategorieentreprise": float,
        },
        low_memory=False
    )
    naf_lib = pd.read_csv(
        "../../data/external/code_naf.csv", sep=";",
        dtype={"Code": str, "Libelle": str},
        encoding="latin1"
    )
    donnees_fin = pd.read_csv(
        "../../data/processed/donnees_financieres.csv",
        dtype={"siren": str, "chiffre_d_affaires": float, "resultat_net": float},
        parse_dates=["date_cloture_exercice"],
        low_memory=False
    )
    tranche_lib = pd.read_csv(
        "../../data/external/tranches_effectifs.csv", sep=";",
        dtype={"Code": str, "Libelle": str},
        encoding="utf-8"
    )
    df = etablissements.merge(
        unite_legale[["siren", "activiteprincipaleunitelegale", "denominationunitelegale", "categorieentreprise", "anneecategorieentreprise"]],
        on="siren",
        how="left"
    )
    df["denominationunitelegale"].fillna(df["siret"], inplace=True)
    df = df.merge(
        naf_lib, how="left",
        left_on="activiteprincipaleunitelegale",
        right_on="Code"
    )
    from pyproj import Transformer
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    df["longitude"], df["latitude"] = transformer.transform(
        df["coordonneelambertabscisseetablissement"].values,
        df["coordonneelambertordonneeetablissement"].values
    )
    return df.dropna(subset=["latitude", "longitude"]), naf_lib, donnees_fin, tranche_lib

df, naf_lib, donnees_fin, tranche_lib = load_data()

coords_communes = {
    "AIX-EN-PROVENCE": [43.53, 5.45],
    "MARSEILLE": [43.30, 5.37]
}
icon_url = "https://cdn-icons-png.flaticon.com/512/684/684908.png"
df["icon_data"] = [{
    "url": icon_url,
    "width": 24,
    "height": 24,
    "anchorY": 24
}] * len(df)

# ======
# Barre latérale/navigation à onglets
# ======
with st.sidebar:
    onglet = option_menu(
        "BizRadar",
        ["Exploration", "À propos", "Guide d'utilisation"],
        icons=["map", "info-circle", "book"],
        menu_icon="cast",
        default_index=0,
    )

# ======
# PAGE PRINCIPALE : Exploration
# ======
if onglet == "Exploration":

    # ======
    # Titre centré
    # ======
    st.markdown("<h1 style='text-align: center;'>BizRadar</h1>", unsafe_allow_html=True)

    # ======
    # Filtres horizontaux
    # ======
    colA, colB = st.columns(2)
    with colA:
        commune_selected = st.selectbox(
            "Commune", ["-- Choisir une commune --"] + list(coords_communes.keys())
        )
    codes_naf_unq = sorted(df["activiteprincipaleunitelegale"].dropna().unique())
    code_naf_labels = naf_lib[naf_lib["Code"].isin(codes_naf_unq)][["Code", "Libelle"]].drop_duplicates()
    naf_select_list = ["-- Tous --"] + [
        f"{row['Code']} - {row['Libelle']}" for _, row in code_naf_labels.iterrows()
    ]
    with colB:
        code_naf_selected = st.selectbox(
            "Secteur d'activité (Code NAF)",
            naf_select_list if commune_selected != "-- Choisir une commune --" else ["-- Tous --"]
        )
    code_naf_sel_code = (
        code_naf_selected.split(" - ")[0]
        if code_naf_selected != "-- Tous --"
        else "-- Tous --"
    )
    code_naf_sel_lib = (
        code_naf_selected.split(" - ")[1]
        if code_naf_selected != "-- Tous --" and " - " in code_naf_selected
        else ""
    )

    # ======
    # Pré-filtrage données
    # ======
    if commune_selected != "-- Choisir une commune --":
        df_filtered = df[df["libellecommuneetablissement"] == commune_selected]
        if code_naf_sel_code != "-- Tous --":
            df_filtered = df_filtered[
                df_filtered["activiteprincipaleunitelegale"] == code_naf_sel_code
            ]
    else:
        df_filtered = pd.DataFrame()

    def evolution_top_secteurs(df_filtered, donnees_fin, naf_lib, n_years=3):
        """
        Compute the top 5 sectors with the highest revenue growth over a specified number of years.

        Args:
            df_filtered (pd.DataFrame): Filtered establishments DataFrame.
            donnees_fin (pd.DataFrame): Financial data DataFrame with revenue information.
            naf_lib (pd.DataFrame): NAF code to label mapping DataFrame.
            n_years (int, optional): Number of years to consider for evolution. Defaults to 3.

        Returns:
            top5_evol (pd.DataFrame): Top 5 sectors sorted by revenue growth percentage.
            df_ca_hist (pd.DataFrame): Historical revenue data for top sectors.
        """
        df_merge = (
            df_filtered[["siren", "activiteprincipaleunitelegale"]]
            .drop_duplicates()
            .merge(donnees_fin, how="left", on="siren")
        )
        df_merge["annee"] = df_merge["date_cloture_exercice"].dt.year
        ca_par_secteur_annee = (
            df_merge.groupby(["activiteprincipaleunitelegale", "annee"])["chiffre_d_affaires"]
            .sum()
            .reset_index()
        )
        max_annee = ca_par_secteur_annee["annee"].max()
        annee_passee = max_annee - n_years
        ca_annee_max = ca_par_secteur_annee[ca_par_secteur_annee["annee"] == max_annee]
        ca_annee_passee = ca_par_secteur_annee[ca_par_secteur_annee["annee"] == annee_passee]
        evolution = ca_annee_max.merge(
            ca_annee_passee,
            on="activiteprincipaleunitelegale",
            suffixes=("_recent", "_past"),
            how="left",
        )
        evolution["evol_ca"] = (
            evolution["chiffre_d_affaires_recent"] - evolution["chiffre_d_affaires_past"]
        )
        evolution["evol_pct"] = (
            evolution["evol_ca"] / evolution["chiffre_d_affaires_past"] * 100
        )
        evolution = evolution.dropna(subset=["evol_pct"])
        top5_evol = evolution.sort_values("evol_pct", ascending=False).head(5)
        top5_evol = top5_evol.merge(
            naf_lib,
            how="left",
            left_on="activiteprincipaleunitelegale",
            right_on="Code",
        )
        top_secteurs_codes = top5_evol["activiteprincipaleunitelegale"].tolist()
        df_ca_hist = ca_par_secteur_annee[
            ca_par_secteur_annee["activiteprincipaleunitelegale"].isin(top_secteurs_codes)
        ]
        df_ca_hist = df_ca_hist.merge(
            naf_lib,
            how="left",
            left_on="activiteprincipaleunitelegale",
            right_on="Code",
        )
        df_ca_hist["secteur_libelle"] = df_ca_hist["Libelle"]
        return top5_evol, df_ca_hist

    def show_map_and_get_selection():
        """
        Display the Pydeck map with establishment icons and return the selected establishment's SIREN.

        Returns:
            str or None: The SIREN of the selected establishment, or None if no selection.
        """
        if commune_selected != "-- Choisir une commune --":
            initial_view_map = pdk.ViewState(
                latitude=coords_communes[commune_selected][0],
                longitude=coords_communes[commune_selected][1],
                zoom=12
            )
            layer = pdk.Layer(
                "IconLayer",
                data=df_filtered,
                get_icon="icon_data",
                get_size=2,
                size_scale=10,
                get_position=["longitude", "latitude"],
                pickable=True,
                id="etab-layer"
            )
            r = pdk.Deck(layers=[layer], initial_view_state=initial_view_map, tooltip={"text": "{denominationunitelegale}"})
            st.write(df_filtered.shape[0], "établissements affichés")
        else:
            st.write("Sélectionnez une commune pour afficher les établissements")
            r = pdk.Deck(initial_view_state=pdk.ViewState(latitude=43.5, longitude=5.4, zoom=8, pitch=0), layers=[])
        event = st.pydeck_chart(r, on_select="rerun", selection_mode="single-object")
        siren_selected = None
        if event and event.selection and "objects" in event.selection:
            selected_objects = event.selection["objects"].get("etab-layer", [])
            if selected_objects:
                siren_selected = selected_objects[0].get("siren")
        return siren_selected

    selected_obj_siren = show_map_and_get_selection()

    if selected_obj_siren:
        tab_labels = ["Sélection", "Général"]
    else:
        tab_labels = ["Général", "Sélection"]

    tabs = st.tabs(tab_labels)
    index_general = tab_labels.index("Général")
    index_selection = tab_labels.index("Sélection")


    with tabs[index_general]:
        #st.write(df_filtered.shape[0], "établissements affichés")
        if df_filtered.empty:
            st.write("Aucun établissement à afficher pour cette sélection.")
        else:
            st.subheader(f"Analyses pour {commune_selected}" + (f" — {code_naf_sel_lib}" if code_naf_sel_code != "-- Tous --" else ""))
            cols = st.columns(3)
            if code_naf_sel_code == "-- Tous --":
                top_secteurs = (
                    df_filtered["activiteprincipaleunitelegale"]
                    .value_counts()
                    .head(5)
                    .reset_index()
                )
                top_secteurs.columns = ["Code NAF", "Nombre établissements"]
                top_secteurs = top_secteurs.merge(
                    naf_lib, how="left", left_on="Code NAF", right_on="Code"
                )
                top_secteurs["Secteur"] = top_secteurs["Libelle"]
                with cols[0]:
                    st.markdown("### Top secteurs")
                    #st.dataframe(top_secteurs[["Code NAF", "Secteur", "Nombre établissements"]], height=120)
                    st.bar_chart(top_secteurs.set_index("Secteur")[["Nombre établissements"]])

                top5_evol, df_ca_hist = evolution_top_secteurs(df_filtered, donnees_fin, naf_lib, n_years=3)
                with cols[1]:
                    st.markdown("### Évolution CA secteurs (3 ans)")
                    #st.dataframe(top5_evol[["activiteprincipaleunitelegale", "Libelle", "evol_pct", "evol_ca"]], height=120)
                    chart = alt.Chart(df_ca_hist).mark_line(point=True).encode(
                        x="annee:O",
                        y="chiffre_d_affaires:Q",
                        color="secteur_libelle:N",
                        tooltip=["secteur_libelle", "annee", "chiffre_d_affaires"]
                    ).properties(width=230, height=120)
                    st.altair_chart(chart, use_container_width=True)

                with cols[2]:
                    st.markdown("### CA secteur sélectionné")
                    if code_naf_sel_code != "-- Tous --":
                        df_filtered_secteur = df_filtered[df_filtered["activiteprincipaleunitelegale"] == code_naf_sel_code]
                        df_merge = (
                            df_filtered_secteur[["siren", "activiteprincipaleunitelegale"]]
                            .drop_duplicates()
                            .merge(donnees_fin, how="left", on="siren")
                        )
                        df_merge["annee"] = df_merge["date_cloture_exercice"].dt.year
                        ca_par_annee = (
                            df_merge.groupby("annee")["chiffre_d_affaires"]
                            .sum()
                            .reset_index()
                        )
                        chart = alt.Chart(ca_par_annee).mark_line(point=True).encode(
                            x="annee:O",
                            y="chiffre_d_affaires:Q",
                            tooltip=["annee", "chiffre_d_affaires"]
                        ).properties(width=230, height=120)
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        st.info("Sélectionnez un secteur pour voir son évolution CA.")

            else:
                with cols[0]:
                    etab_with_fin = df_filtered.merge(
                        donnees_fin, how="left", on="siren"
                    )
                    idx = etab_with_fin.groupby("siren")["date_cloture_exercice"].idxmax()
                    idx = idx.dropna().astype(int)
                    etab_fin_last = etab_with_fin.loc[idx]
                    top_etabs = etab_fin_last.nlargest(5, "chiffre_d_affaires")[[
                        "denominationunitelegale", "siren", "chiffre_d_affaires"
                    ]]
                    st.markdown("### Top CA établissements")
                    #st.dataframe(top_etabs, height=120)
                    st.bar_chart(top_etabs.set_index("denominationunitelegale")[["chiffre_d_affaires"]])

                with cols[1]:
                    df_filtered_secteur = df_filtered[df_filtered["activiteprincipaleunitelegale"] == code_naf_sel_code]
                    df_merge = (
                        df_filtered_secteur[["siren", "activiteprincipaleunitelegale"]]
                        .drop_duplicates()
                        .merge(donnees_fin, how="left", on="siren")
                    )
                    df_merge["annee"] = df_merge["date_cloture_exercice"].dt.year
                    ca_par_annee = (
                        df_merge.groupby("annee")["chiffre_d_affaires"]
                        .sum()
                        .reset_index()
                    )
                    chart = alt.Chart(ca_par_annee).mark_line(point=True).encode(
                        x="annee:O",
                        y="chiffre_d_affaires:Q",
                        tooltip=["annee", "chiffre_d_affaires"]
                    ).properties(width=230, height=120)
                    st.markdown(f"### Évolution CA {code_naf_sel_lib}")
                    st.altair_chart(chart, use_container_width=True)

                #with cols[2]:
                    #st.markdown("### Placeholder graphique/actions")
                    # Place ici tout autre graphique complémentaire

        # Onglet sélection
        with tabs[index_selection]:
            if selected_obj_siren:
                selected_rows = df[df["siren"] == selected_obj_siren]
                if not selected_rows.empty:
                    etab_info = selected_rows.iloc[0]
                    st.markdown("## Infos établissement")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Dénomination:**", etab_info.get("denominationunitelegale"))
                        st.write("**SIREN:**", etab_info.get("siren"))
                        st.write("**SIRET:**", etab_info.get("siret", "N/A"))
                        st.write("**Catégorie entreprise:**", etab_info.get("categorieentreprise", "N/A"))
                    with col2:
                        adresse_parts = [
                            etab_info.get("complementadresseetablissement", ""),
                            str(etab_info.get("numerovoieetablissement", "")),
                            etab_info.get("typevoieetablissement", ""),
                            etab_info.get("libellevoieetablissement", ""),
                            str(etab_info.get("codepostaletablissement", "")),
                            etab_info.get("libellecommuneetablissement", "")
                        ]
                        adresse_complete = " ".join([str(x) for x in adresse_parts if pd.notna(x) and str(x).strip() != ""])
                        st.write("**Adresse:**", adresse_complete if adresse_complete else "N/A")
                        code_tranche = etab_info.get("trancheeffectifsetablissement", None)
                        libelle = tranche_lib.loc[tranche_lib["Code"] == code_tranche, "Libelle"].values
                        if len(libelle) > 0:
                            st.write("**Tranche effectif:**", libelle[0])
                        else:
                            st.write("**Tranche effectif:** N/A")
                        st.write("**Année effectif:**", etab_info.get("anneeeffectifsetablissement", "N/A"))
                        st.write("**Année catégorie entreprise:**", etab_info.get("anneecategorieentreprise", "N/A"))
                    df_fin = donnees_fin[donnees_fin["siren"] == selected_obj_siren].sort_values("date_cloture_exercice")
                    st.markdown("### Historique financier")
                    st.line_chart(df_fin.set_index("date_cloture_exercice")[["chiffre_d_affaires", "resultat_net"]])
                else:
                    st.warning("Aucun établissement trouvé pour ce SIREN sélectionné.")
            else:
                st.write("Sélectionnez un établissement sur la carte pour voir ses détails ici.")

# ======
# PAGE À propos
# ======
elif onglet == "À propos":
    st.subheader("À propos de BizRadar")
    st.markdown("""
    **BizRadar** est une application web interactive de visualisation des établissements et de leur dynamique économique à l'échelle locale.
    
    - Exploration par commune et par secteur d'activité (code NAF)
    - Analyses graphiques dynamiques : répartition des établissements, évolution sectorielle, top entreprise
    - Carte interactive géolocalisée, mise à jour avec les données SIRENE et INSEE
    - Idéal pour analyse territoriale, urbanisme, prospection commerciale, veille économique
    
    Développé avec Python, pandas, Streamlit et pydeck.
    """)

    st.info("""
    BizRadar s'appuie sur les données ouvertes d'entreprise (SIRENE, INSEE), enrichit l'expérience utilisateur par un design intuitif et une navigation multi-onglets.
    """)

# ======
# PAGE Guide d'utilisation
# ======
elif onglet == "Guide d'utilisation":
    st.subheader("Guide d'utilisation BizRadar")
    st.markdown("""
    **1. Sélectionnez une commune** grâce au filtre horizontal sous le titre.

    **2. Optionnel : ciblez un secteur d’activité (Code NAF)** pour filtrer la carte et les graphiques.

    **3. Explorez la carte** : les établissements apparaissent sous forme de points interactifs.

    **4. Visualisez les graphiques** : top secteurs, évolution des chiffres d'affaires, classement entreprises.

    **5. Cliquez sur un établissement** pour afficher ses détails : identité, adresse, effectif, historique financier.

    Utilisez la barre de navigation latérale pour passer entre :

    - *Exploration* (carte et analyses)
    - *À propos* (présentation du projet)
    - *Guide d'utilisation* (conseils, étapes)

    [Documentation technique, sources de données, contact de l’équipe : disponibles sur demande ou sur le dépôt Git associé.]
    """)
    st.success("Bonne utilisation ! Vos retours et suggestions sont bienvenus.")
