import os
import logging
from retry import retry
from neo4j import GraphDatabase

# Paths to CSV files (impostati tramite variabili d'ambiente)
PAZIENTI_CSV_PATH = os.getenv("PAZIENTI_CSV_PATH")
MEDICI_CSV_PATH = os.getenv("MEDICI_CSV_PATH")
DOCUMENTI_CSV_PATH = os.getenv("DOCUMENTI_CSV_PATH")
PRESCRIZIONI_CSV_PATH = os.getenv("PRESCRIZIONI_CSV_PATH")
REFERTI_CSV_PATH = os.getenv("REFERTI_CSV_PATH")
VACCINAZIONI_CSV_PATH = os.getenv("VACCINAZIONI_CSV_PATH")
PRENOTAZIONI_CSV_PATH = os.getenv("PRENOTAZIONI_CSV_PATH")
TELEMEDICINA_CSV_PATH = os.getenv("TELEMEDICINA_CSV_PATH")
ACCESSI_CSV_PATH = os.getenv("ACCESSI_CSV_PATH")
RICETTE_CSV_PATH = os.getenv("RICETTE_CSV_PATH")
OSPEDALI_SARDEGNA_CSV_PATH = os.getenv("OSPEDALI_SARDEGNA_CSV_PATH")
LISTA_DI_ATTESA_CSV_PATH = os.getenv("LISTA_DI_ATTESA_CSV_PATH")

# Neo4j config
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Logging config
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
LOGGER = logging.getLogger(__name__)

# Definizione dei nomi dei nodi (inclusi ListaAttesa e Ospedale)
NODES = [
    "Paziente", "Medico", "Documento", "Prescrizione", "Referto",
    "Vaccinazione", "Prenotazione", "SessioneTelemedicina",
    "Accesso", "Ricetta", "ListaAttesa", "Ospedale"
]

def _set_uniqueness_constraints(tx, node):
    """
    Crea un constraint di unicità su (n.id)
    per ogni tipo di nodo definito nella lista NODES.
    """
    query = f"""
    CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
    REQUIRE n.id IS UNIQUE;
    """
    tx.run(query)

@retry(tries=5, delay=5)
def load_fse_graph_from_csv():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    # 1) Imposta i constraint di unicità per ogni nodo
    with driver.session(database="neo4j") as session:
        for node in NODES:
            session.execute_write(_set_uniqueness_constraints, node)

    def run_query(label, query):
        LOGGER.info(f"Carico i nodi/relazioni per: {label}")
        with driver.session(database="neo4j") as session:
            session.run(query)

    # 2) Importo i dati dai CSV (Pazienti, Medici, Documenti, ecc.)
    # --- Puoi riutilizzare o modificare le parti già presenti nel tuo script. ---

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
            ente: row.ente, 
            ospedale: row.ospedale,
            email: row.email,
            telefono: row.telefono,
            nome_completo_medico: row.nome_completo_medico
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
        MERGE (m)-[:HA_EMESSO_DOCUMENTO]->(d);
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
        MERGE (m)-[:HA_EMESSO_REFERT]->(r);
    """)

    # Vaccinazioni
    run_query("Vaccinazione", f"""
        LOAD CSV WITH HEADERS FROM '{VACCINAZIONI_CSV_PATH}' AS row
        MERGE (v:Vaccinazione {{
            id: row.vaccinazione_id,
            tipo: row.tipo_vaccino,
            data: row.data_somministrazione,
            dose: row.dose,
            ente: row.ente,
            ospedale: row.ospedale,
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

    # Accessi
    run_query("Accesso", f"""
        LOAD CSV WITH HEADERS FROM '{ACCESSI_CSV_PATH}' AS row
        WITH row WHERE row.accesso_id IS NOT NULL
        MERGE (a:Accesso {{
            id: row.accesso_id,
            tipo_utente: row.utente_tipo,
            azione: row.azione,
            data_accesso: row.data_accesso,
            ip: row.ip
        }})
        WITH a, row
        CALL {{
            WITH a, row
            WHERE row.utente_tipo = 'Medico'
            MATCH (m:Medico {{id: row.utente_id}})
            MERGE (m)-[:HA_EFFETTUATO_ACCESSO]->(a)
        }}
        CALL {{
            WITH a, row
            WHERE row.utente_tipo = 'Paziente'
            MATCH (p:Paziente {{id: row.utente_id}})
            MERGE (p)-[:HA_EFFETTUATO_ACCESSO]->(a)
        }};
    """)

    # Ricette (dai dati di nre_data.csv)
    run_query("Ricetta", f"""
        LOAD CSV WITH HEADERS FROM '{RICETTE_CSV_PATH}' AS row
        WITH row WHERE row.codice_nre IS NOT NULL
        MERGE (r:Ricetta {{
            id: row.codice_nre,
            tipo: row.tipo_prescrizione,
            descrizione: row.descrizione,
            data_prescrizione: row.data_prescrizione,
            data_scadenza: row.data_scadenza,
            asl_emittente: row.asl_emittente,
            stato_ricetta: row.stato_ricetta,
            canale_invio: row.canale_invio
        }})
        WITH r, row
        MATCH (p:Paziente {{codice_fiscale: row.codice_fiscale_paziente}})
        MERGE (p)-[:HA_RICETTA]->(r)
        WITH r, row
        MATCH (m:Medico)
        WHERE toLower(m.nome) + ' ' + toLower(m.cognome) = toLower(row.medico_prescrittore)
        MERGE (m)-[:PRESCRITTORE]->(r);
    """)

    # Ospedali (da ospedali_sardegna.csv)
    run_query("Ospedale", f"""
        LOAD CSV WITH HEADERS FROM '{OSPEDALI_SARDEGNA_CSV_PATH}' AS row
        MERGE (o:Ospedale {{
            ospedale: row.ospedale,
            ente: row.ente,
            luogo: row.luogo,
            provincia: row.provincia,
            id: row.ente + '_' + row.ospedale
        }});
    """)

    # 3) Import ListaAttesa (nuova struttura)
    run_query("ListaAttesa", f"""
        LOAD CSV WITH HEADERS FROM '{LISTA_DI_ATTESA_CSV_PATH}' AS row
        MERGE (la:ListaAttesa {{
            id: row.ente + '_' + row.prestazione + '_' + row.priorita,
            ente: row.ente,
            prestazione: row.prestazione,
            priorita: row.priorita,
            max_giorni_di_attesa: row.max_giorni_di_attesa
        }})
        WITH la, row
        MATCH (o:Ospedale {{ente: row.ente}})
        MERGE (la)-[:RIFERISCE_A {{
          max_giorni: toInteger(row.max_giorni_di_attesa)
        }}]->(o);
    """)

    LOGGER.info("Caricamento completo del grafo sanitario.")
    driver.close()

if __name__ == "__main__":
    load_fse_graph_from_csv()
