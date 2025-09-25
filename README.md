# Pipeline ETL : Traiterment des données en temps réel

##  🚍 Projet ETL GTFS – Métropole de Nice / Lignes d’Azur

Ce projet vise à construire un pipeline ETL pour exploiter les données de transport en open data (GTFS / GTFS-RT) fournies par la métropole de Nice et Lignes d’Azur. Il permet :

- la collecte et transformation des données statiques et temps réel,

- leur chargement dans Snowflake,

- la préparation de jeux de données pour l’analyse et le suivi (retards, ponctualité, état du service).

Des tableaux de bord analytiques illustrent les usages possibles pour la visualisation et l’aide à la décision.

## Pipeline ELT (vue d’ensemble)

![ELT](https://github.com/IZAK-M/AIRFLOW-SNOWFLAK-POWER-BI/blob/main/images/ELT.png)

#### Le pipeline suit l’architecture MEDALLION :
- *Bronze* : Ingestion brute des GTFS static et realtime, avec horodatage et source.

- *Silver* : Normalisation des horaires, jointures entre trips/routes/stops, gestion des fuseaux horaires.

- *Gold* : KPIs comme ponctualité, fréquence, temps d’attente, par ligne/jour/heure, prêts pour Power BI.

## 🛠️ Stack technique

 - **Apache Airflow** : orchestration des workflows ETL via des DAGs modulaires et maintenables

 - **Snowflake** : entrepôt de données cloud pour le stockage, la transformation et l’analyse des données

 - **Docker Compose** : environnement local reproductible pour le déploiement d’Airflow et de ses services associés

 - **Python** : scripts d’ingestion, parsing des flux Protobuf (GTFS-RT), et transformation des données au format CSV

## Dépendances principales

Voir [`requirements.txt`]() :

* `pandas`
* `requests`
* `gtfs-realtime-bindings`
* `snowflake-connector-python`
* `apache-airflow-providers-snowflake==6.5.2`

## License

[MIT](https://choosealicense.com/licenses/mit/)