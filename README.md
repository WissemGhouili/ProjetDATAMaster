# ProjetDATAMastere

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

This is my end of year projet of the Mastere Data Engineer in Ynov Aix Campus

## Project Organization

```
├── LICENSE            <- Open-source license if one is chosen
├── Makefile           <- Makefile with convenience commands like `make data` or `make train`
├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
│
├── docs               <- A default mkdocs project; see www.mkdocs.org for details
│
├── models             <- Trained and serialized models, model predictions, or model summaries
│
├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
│                         the creator's initials, and a short `-` delimited description, e.g.
│                         `1.0-jqp-initial-data-exploration`.
│
├── pyproject.toml     <- Project configuration file with package metadata for 
│                         ProjetDATAMastere and configuration for tools like black
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
│   └── figures        <- Generated graphics and figures to be used in reporting
│
├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
│                         generated with `pip freeze > requirements.txt`
│
├── setup.cfg          <- Configuration file for flake8
│
└── src   <- Source code for use in this project.
    │
    ├── __init__.py             <- Makes ProjetDATAMastere a Python module
    │
    ├── config.py               <- Store useful variables and configuration
    │
    ├── dataset.py              <- Scripts to download or generate data
    │
    ├── features.py             <- Code to create features for modeling
    │
    ├── modeling                
    │   ├── __init__.py 
    │   ├── predict.py          <- Code to run model inference with trained models          
    │   └── train.py            <- Code to train models
    │
    └── plots.py                <- Code to create visualizations
```

--------

# Data Pipeline

This document presents the complete documentation of the data pipeline.
---

## General Architecture

The project is structured into multiple layers ensuring the collection, processing, storage, and visualization of establishment and legal unit data via the INSEE Sirene API.

+----------------------+ +----------------------+ +----------------------+ +----------------------+
| | | | | | | |
| API Extraction | --> | Processing & ETL | --> | SQL Database Storage | --> | Visualization & BI |
| (Sirene API / DAGS) | | (pandas, Airflow) | | (PostgreSQL) | | (e.g., dashboards) |
| | | | | | | |
+----------------------+ +----------------------+ +----------------------+ +----------------------+


---

## Project Components

### 1. Data Extraction

- **Source**: INSEE Sirene API.
- **Description**:  
  - Daily extraction of establishments created after a last update date (`last_update`).
  - Retrieval of unique SIREN numbers.
  - Extraction of associated legal units via the same API.
- **Tools**:  
  - Apache Airflow DAGs orchestrate API calls.
  - Compliance with API request limits (one request every 2 seconds).

---

### 2. Data Processing

- **Tech stack**: Pandas (simple test), Spark (for future scalability).
- **Operations performed**:  
  - Cleaning the received JSON data.
  - Keeping only relevant columns for establishments and legal units.
  - Converting date fields to appropriate datetime formats.
  - Filtering active establishments and legal units.
- **Storage**:  
  - Inserted into PostgreSQL relational database.
  - Main tables: `etablissements` (establishments) and `unite_legale` (legal units).

---

### 3. Scheduling with Airflow (DAGs)

- **Organization**:  
  - Main DAG runs daily to collect, process, and update the `last_update` variable.
  - Apache Airflow handles scheduling, logging, and error management.
- **Key features**:  
  - Dynamic management of extraction start date stored as an Airflow variable.
  - Pagination and respect for API limits.
  - Detailed logs of processed establishments to aid debugging.
- **Development**:  
  - Clean Python code, well-commented and structured using Airflow task decorators.

---

### 4. Visualization & Reporting

- **Goal**:  
  - Provide analytical dashboards based on the loaded data.
- **Technologies considered**:  
  - BI tools (Tableau, Power BI, Metabase) connected to PostgreSQL.
  - Custom visualizations (e.g., histograms, geographical maps).
- **Examples**:  
  - Number of active establishments by region.
  - Evolution of legal unit creation over time.

---

## Main Data Structure

| Table           | Key Columns                                                                                                         |
|-----------------|---------------------------------------------------------------------------------------------------------------------|
| **etablissements** | siret, siren, etatAdministratifEtablissement, dateCreationEtablissement, trancheEffectifsEtablissement, anneeEffectifsEtablissement, complementAdresseEtablissement, numeroVoieEtablissement, typeVoieEtablissement, libelleVoieEtablissement, codePostalEtablissement, coordonneeLambertAbscisseEtablissement, coordonneeLambertOrdonneeEtablissement  |
| **unite_legale**   | siren, denominationUniteLegale, dateCreationUniteLegale, activitePrincipaleUniteLegale, categorieEntreprise, anneeCategorieEntreprise, trancheEffectifsUniteLegale, anneeEffectifsUniteLegale |
| **donnees_financieres**   | siren, chiffre_d_affaires, resultat_net, date_cloture_exercice |

---

## Main Airflow DAGs

- **fetch_one_etablissement_example_with_logging**  
  A simple example DAG extracting one active establishment created after the last known date, followed by its associated legal unit, processing, and storage.

- **fetch_sirene_data**  
  Full DAG performing daily extraction of all recent establishments since the last run, batch processing, and updating PostgreSQL.

### DAG Features

- Airflow variable `last_update` managing the last extraction date.
- Detailed logging within Airflow for traceability.
- Smart API call control (pagination, minimum 2-seconds delay between requests).
- Use of pandas only for initial data processing, extensible to Spark.
- Secure storage in PostgreSQL via SQLAlchemy.

---

## Installation & Running

1. **Environment setup**  
   - Set the environment variable `AIRFLOW_HOME` to your Airflow working directory.  
   - Place DAG files in the `${AIRFLOW_HOME}/dags` folder.

2. **Start Airflow services**  
   - `airflow webserver --port 8080`  
   - `airflow scheduler`

3. **Trigger DAGs**  
   - Via Airflow Web UI (http://localhost:8080)  
   - Or via CLI: `airflow dags trigger <dag_id>`

4. **Monitoring & Logs**  
   - Logs available in Airflow Web UI for each task instance.

---

## Conclusion

This project delivers a complete, reliable pipeline for ingestion, processing, and analysis of French economic data from Sirene.  
The modular design and use of Airflow ensure easy extensibility and maintainability, suitable for production and analytics environments.

---

## Contact

For any questions or contributions, please contact:  
**your.email@example.com**

---

*Documentation automatically generated on 09/15/2025*


