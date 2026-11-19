"""Connexion a la base PostgreSQL des donnees marche (Bronze / Silver / Gold).

Les identifiants viennent des variables d'environnement MARKET_DB_* (fichier .env),
jamais ecrits en dur dans le code.
"""
import logging
import os

import psycopg2

log = logging.getLogger(__name__)

REQUIRED_SCHEMAS = {"bronze", "silver", "gold"}


def get_connection():
    """Ouvre une connexion a la base marche. A utiliser par les taches des DAGs."""
    return psycopg2.connect(
        host=os.environ["MARKET_DB_HOST"],
        port=os.environ.get("MARKET_DB_PORT", "5432"),
        user=os.environ["MARKET_DB_USER"],
        password=os.environ["MARKET_DB_PASSWORD"],
        dbname=os.environ["MARKET_DB_NAME"],
        connect_timeout=5,
    )


def check_connection(**context):
    """Premiere tache de chaque DAG : echoue si la base est injoignable."""
    host = os.environ.get("MARKET_DB_HOST", "?")
    dbname = os.environ.get("MARKET_DB_NAME", "?")
    try:
        conn = get_connection()
    except KeyError as exc:
        raise RuntimeError(f"Variable d'environnement manquante : {exc}") from exc
    except psycopg2.OperationalError as exc:
        # Airflow reessaiera (retries) puis marquera la tache en echec
        raise RuntimeError(f"Connexion impossible a {host}/{dbname} : {exc}") from exc

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name = ANY(%s)",
                (list(REQUIRED_SCHEMAS),),
            )
            found = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    log.info("Connexion OK a %s/%s", host, dbname)
    missing = REQUIRED_SCHEMAS - found
    if missing:
        log.warning("Schemas absents (a creer par le Database Engineer) : %s", sorted(missing))
