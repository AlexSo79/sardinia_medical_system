import os
import logging
from retry import retry
from neo4j import GraphDatabase

# Paths to CSV files
PAZIENTI_CSV_PATH = os.getenv("PAZIENTI_CSV_PATH")
MEDICI_CSV_PATH = os.getenv("MEDICI_CSV_PATH")
DOCUMENTI_CSV_PATH = os.getenv("DOCUMENTI_CSV_PATH")
PRESCRIZIONI_CSV_PATH = os.getenv("PRESCRIZIONI_CSV_PATH")
REFERTI_CSV_PATH = os.getenv("REFERTI_CSV_PATH")
VACCINAZIONI_CSV_PATH = os.getenv("VACCINAZIONI_CSV_PATH")
PRENOTAZIONI_CSV_PATH = os.getenv("PRENOTAZIONI_CSV_PATH")
TELEMEDICINA_CSV_PATH = os.getenv("TELEMEDICINA_CSV_PATH")

# Neo4j config
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
LOGGER = logging.getLogger(__name__)

NODES = ["Paziente", "Medico", "Documento", "Prescrizione", "Referto", "Vaccinazione", "Prenotazione", "SessioneTelemedicina"]


def _set_uniqueness_constraints(tx, node):
    query = f"""
    CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
    REQUIRE n.id IS UNIQUE;
    """
    tx.run(query)


@retry(tries=5, delay=5)
def load_fse_graph_from_csv():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    with driver.session(database="neo4j") as session:
        for node in NODES:
            session.execute_write(_set_uniqueness_constraints, node)

    def run_query(label, query):
        LOGGER.info(f"Carico i nodi/relazioni per: {label}")
        with driver.session(database="neo4j") as session:
            session.run(query)

    # Pazienti
    run_query("Paziente", f"""
        LOAD CSV WITH HEADERS FROM '{PAZIENTI_CSV_PATH}' AS row
        MERGE (p:Paziente {{
            id: row.paziente_id,
            nome: row.nome,
            cognome: row.cognome,
            codice_fiscale: row.codice_fiscale,
            data_nascita: row.data_nascita,
            sesso: row.sesso,
            indirizzo: row.indirizzo,
            email: row.email,
            telefono: row.telefono
        }});
    """)

    # Medici
    run_query("Medico", f"""
        LOAD CSV WITH HEADERS FROM '{MEDICI_CSV_PATH}' AS row
        MERGE (m:Medico {{
            id: row.medico_id,
            nome: row.nome,
            cognome: row.cognome,
            codice_fiscale: row.codice_fiscale,
            specializzazione: row.specializzazione,
            asl_appartenenza: row.asl_appartenenza,
            email: row.email,
            telefono: row.telefono
        }});
    """)

    # Documenti
    run_query("Documento", f"""
        LOAD CSV WITH HEADERS FROM '{DOCUMENTI_CSV_PATH}' AS row
        MERGE (d:Documento {{
            id: row.documento_id,
            tipo: row.tipo_documento,
            data_emissione: row.data_emissione,
            struttura: row.struttura_emittente,
            descrizione: row.descrizione,
            allegato_url: row.allegato_pdf_url,
            visibile: row.visibile_al_paziente
        }})
        WITH d, row
        MATCH (p:Paziente {{id: row.paziente_id}})
        MERGE (p)-[:HA_DOCUMENTO]->(d)
        WITH d, row
        MATCH (m:Medico {{id: row.medico_id}})
        MERGE (m)-[:REDAZIONE]->(d);
    """)

    # Prescrizioni
    run_query("Prescrizione", f"""
        LOAD CSV WITH HEADERS FROM '{PRESCRIZIONI_CSV_PATH}' AS row
        MERGE (pr:Prescrizione {{
            id: row.prescrizione_id,
            tipo: row.tipo,
            nome: row.nome,
            posologia: row.posologia,
            data: row.data_prescrizione,
            stato: row.stato
        }})
        WITH pr, row
        MATCH (p:Paziente {{id: row.paziente_id}})
        MERGE (p)-[:HA_PRESCRIZIONE]->(pr)
        WITH pr, row
        MATCH (m:Medico {{id: row.medico_id}})
        MERGE (m)-[:PRESCRITTORE]->(pr);
    """)

    # Referti
    run_query("Referto", f"""
        LOAD CSV WITH HEADERS FROM '{REFERTI_CSV_PATH}' AS row
        MERGE (r:Referto {{
            id: row.referto_id,
            tipo_esame: row.tipo_esame,
            data: row.data_esame,
            esito: row.esito,
            valori_numerici: row.valori_numerici
        }})
        WITH r, row
        MATCH (p:Paziente {{id: row.paziente_id}})
        MERGE (p)-[:HA_REFERTATO]->(r)
        WITH r, row
        MATCH (m:Medico {{id: row.medico_refertante}})
        MERGE (m)-[:REFERTANTE]->(r);
    """)

    # Vaccinazioni
    run_query("Vaccinazione", f"""
        LOAD CSV WITH HEADERS FROM '{VACCINAZIONI_CSV_PATH}' AS row
        MERGE (v:Vaccinazione {{
            id: row.vaccinazione_id,
            tipo: row.tipo_vaccino,
            data: row.data_somministrazione,
            dose: row.dose,
            luogo: row.luogo
        }})
        WITH v, row
        MATCH (p:Paziente {{id: row.paziente_id}})
        MERGE (p)-[:HA_VACCINAZIONE]->(v);
    """)

    # Prenotazioni CUP
    run_query("Prenotazione", f"""
        LOAD CSV WITH HEADERS FROM '{PRENOTAZIONI_CSV_PATH}' AS row
        MERGE (pr:Prenotazione {{
            id: row.prenotazione_id,
            nre: row.nre,
            prestazione: row.prestazione,
            sede: row.sede,
            data_ora: row.data_ora,
            stato: row.stato,
            canale: row.canale_prenotazione
        }})
        WITH pr, row
        MATCH (p:Paziente {{id: row.paziente_id}})
        MERGE (p)-[:HA_PRENOTATO]->(pr)
        WITH pr, row
        MATCH (m:Medico {{id: row.medico_id}})
        MERGE (pr)-[:MEDICO_PRENOTAZIONE]->(m);
    """)

    # Sessioni di Telemedicina
    run_query("SessioneTelemedicina", f"""
        LOAD CSV WITH HEADERS FROM '{TELEMEDICINA_CSV_PATH}' AS row
        MERGE (s:SessioneTelemedicina {{
            id: row.sessione_id,
            tipo: row.tipo_sessione,
            data_ora: row.data_ora,
            durata_minuti: row.durata_minuti,
            canale: row.canale_utilizzato,
            esito: row.esito,
            allegato_referto: row.allegato_referto,
            stato: row.stato
        }})
        WITH s, row
        MATCH (p:Paziente {{id: row.paziente_id}})
        MERGE (p)-[:PARTECIPA_SESSIONE]->(s)
        WITH s, row
        MATCH (m:Medico {{id: row.medico_id}})
        MERGE (m)-[:CONDUCE_SESSIONE]->(s);
    """)

    LOGGER.info("Caricamento completo del grafo sanitario.")


if __name__ == "__main__":
    load_fse_graph_from_csv()
