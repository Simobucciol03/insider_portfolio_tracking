import mysql.connector
from mysql.connector import Error
import pandas as pd
import sqlalchemy
from datetime import datetime, date
from typing import Optional, Dict, List, Any, Union
import logging
from tabulate import tabulate
import json

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortfolioManager:
    """
    Sistema unificato per la gestione completa dei dati di portafoglio.
    Include funzionalità di inserimento, estrazione e visualizzazione.
    """
    
    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        """
        Inizializza il gestore portafoglio.
        
        Args:
            host: Indirizzo del server MySQL
            user: Username per la connessione
            password: Password per la connessione
            database: Nome del database
            port: Porta del server MySQL (default: 3306)
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None
        self.engine = None
        
        # Inizializza connessioni
        self._initialize_connections()

    def _initialize_connections(self):
        """Inizializza sia la connessione MySQL che l'engine SQLAlchemy."""
        self.connection = self._create_server_connection()
        if self.connection:
            self.engine = self._create_engine_mysql()

    def _create_server_connection(self) -> Optional[mysql.connector.MySQLConnection]:
        """Crea una connessione al server MySQL."""
        try:
            connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False
            )
            logger.info("✅ Connesso al database MySQL.")
            return connection
        except Error as err:
            logger.error(f"❌ Errore nella connessione: {err}")
            return None

    def _create_engine_mysql(self) -> sqlalchemy.engine.Engine:
        """Crea un engine SQLAlchemy per operazioni avanzate."""
        try:
            connection_string = f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            engine = sqlalchemy.create_engine(connection_string, echo=False)
            logger.info("✅ Engine SQLAlchemy creato.")
            return engine
        except Exception as e:
            logger.error(f"❌ Errore nella creazione dell'engine: {e}")
            return None

    def _execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Esegue una query e restituisce i risultati."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return []
            
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            return results
        except Error as e:
            logger.error(f"❌ Errore nell'esecuzione della query: {e}")
            return []
        finally:
            cursor.close()

    def _execute_query_with_columns(self, query: str, params: tuple = None) -> tuple:
        """Esegue una query e restituisce risultati con nomi delle colonne."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return [], []
            
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return results, columns
        except Error as e:
            logger.error(f"❌ Errore nell'esecuzione della query: {e}")
            return [], []
        finally:
            cursor.close()

    # ==================== FUNZIONI DI SETUP ====================

    def initialize_database(self) -> bool:
        """Inizializza il database creando tutte le tabelle necessarie."""
        logger.info("🔧 Inizializzazione database...")
        return self.create_tables_schema()

    def create_tables_schema(self) -> bool:
        """Crea lo schema delle tabelle nel database."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return False
            
        cursor = self.connection.cursor()
        try:
            # Tabella dei fondi
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS funds (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cik VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """)

            # Tabella dei filing
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS filings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fund_id INT NOT NULL,
                    accession_number VARCHAR(30) UNIQUE NOT NULL,
                    report_date DATE NOT NULL,
                    filed_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (fund_id) REFERENCES funds(id) ON DELETE CASCADE,
                    INDEX idx_report_date (report_date),
                    INDEX idx_filed_date (filed_date)
                );
            """)

            # Tabella dei titoli
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS securities (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cusip VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """)

            # Tabella delle posizioni
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS positions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    filing_id INT NOT NULL,
                    security_id INT NOT NULL,
                    value BIGINT NOT NULL DEFAULT 0,
                    shares BIGINT NOT NULL DEFAULT 0,
                    share_type VARCHAR(50),
                    investment_discretion VARCHAR(50),
                    voting_authority VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (filing_id) REFERENCES filings(id) ON DELETE CASCADE,
                    FOREIGN KEY (security_id) REFERENCES securities(id) ON DELETE CASCADE,
                    INDEX idx_filing_security (filing_id, security_id),
                    INDEX idx_value (value),
                    UNIQUE KEY unique_filing_security (filing_id, security_id)
                );
            """)

            self.connection.commit()
            logger.info("✅ Schema database creato con successo.")
            return True
            
        except Error as e:
            logger.error(f"❌ Errore nella creazione delle tabelle: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    # ==================== FUNZIONI DI INSERIMENTO ====================

    def insert_fund(self, cik: str, name: str) -> Optional[int]:
        """Inserisce un nuovo fondo o restituisce l'ID se già esiste."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            # Verifica se esiste già
            cursor.execute("SELECT id FROM funds WHERE cik = %s", (cik,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Fondo con CIK {cik} già esistente, ID: {result[0]}")
                return result[0]
            
            # Inserisce nuovo fondo
            cursor.execute("INSERT INTO funds (cik, name) VALUES (%s, %s)", (cik, name))
            self.connection.commit()
            fund_id = cursor.lastrowid
            logger.info(f"✅ Fondo inserito con ID: {fund_id}")
            return fund_id
            
        except Error as e:
            logger.error(f"❌ Errore nell'inserimento del fondo: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_security(self, cusip: str, name: str) -> Optional[int]:
        """Inserisce un nuovo titolo o restituisce l'ID se già esiste."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None

        cursor = self.connection.cursor()
        try:
            # Verifica se esiste già
            cursor.execute("SELECT id FROM securities WHERE cusip = %s", (cusip,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Titolo con CUSIP {cusip} già esistente, ID: {result[0]}")
                return result[0]
            
            # Inserisce nuovo titolo
            cursor.execute("INSERT INTO securities (cusip, name) VALUES (%s, %s)", (cusip, name))
            self.connection.commit()
            security_id = cursor.lastrowid
            logger.info(f"✅ Titolo inserito con ID: {security_id}")
            return security_id
            
        except Error as e:
            logger.error(f"❌ Errore nell'inserimento del titolo: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_filing(self, fund_id: int, accession_number: str, 
                    report_date: Union[datetime, date], 
                    filed_date: Union[datetime, date]) -> Optional[int]:
        """Inserisce un nuovo filing o restituisce l'ID se già esiste."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            # Verifica se esiste già
            cursor.execute("SELECT id FROM filings WHERE accession_number = %s", (accession_number,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Filing con numero {accession_number} già esistente, ID: {result[0]}")
                return result[0]
            
            # Converte date se necessario
            if isinstance(report_date, datetime):
                report_date = report_date.date()
            if isinstance(filed_date, datetime):
                filed_date = filed_date.date()
            
            # Inserisce nuovo filing
            cursor.execute("""
                INSERT INTO filings (fund_id, accession_number, report_date, filed_date)
                VALUES (%s, %s, %s, %s)
            """, (fund_id, accession_number, report_date, filed_date))
            self.connection.commit()
            filing_id = cursor.lastrowid
            logger.info(f"✅ Filing inserito con ID: {filing_id}")
            return filing_id
            
        except Error as e:
            logger.error(f"❌ Errore nell'inserimento del filing: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()
    def insert_position(self, filing_id: int, security_id: int, value: int, 
                    shares: int, share_type: str = None, 
                    investment_discretion: str = None, 
                    voting_authority: str = None) -> Optional[int]:
        """Inserisce una nuova posizione."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            # Controlla se la posizione esiste già
            cursor.execute("""
                SELECT id FROM positions 
                WHERE filing_id = %s AND security_id = %s
            """, (filing_id, security_id))
            
            existing_position = cursor.fetchone()
            
            if existing_position:
                # Se esiste, aggiorna la posizione esistente sommando i valori
                cursor.execute("""
                    UPDATE positions 
                    SET value = value + %s, shares = shares + %s
                    WHERE filing_id = %s AND security_id = %s
                """, (value, shares, filing_id, security_id))
                
                logger.info(f"✅ Posizione esistente aggiornata. Filing ID: {filing_id}, Security ID: {security_id}")
                self.connection.commit()
                return existing_position[0]
            else:
                # Se non esiste, inserisce una nuova posizione
                cursor.execute("""
                    INSERT INTO positions (
                        filing_id, security_id, value, shares, share_type,
                        investment_discretion, voting_authority
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (filing_id, security_id, value, shares, share_type, 
                    investment_discretion, voting_authority))
                
                self.connection.commit()
                position_id = cursor.lastrowid
                logger.info(f"✅ Posizione inserita con ID: {position_id}")
                return position_id
                
        except Error as e:
            logger.error(f"❌ Errore nell'inserimento della posizione: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_complete_portfolio_data(self, portfolio_data: Dict[str, Any]) -> bool:
        """
        Inserisce un set completo di dati del portafoglio.
        Versione semplificata che gestisce posizioni multiple aggregandole.
        """
        try:
            # Inserisce il fondo
            fund_id = self.insert_fund(
                portfolio_data['fund']['cik'],
                portfolio_data['fund']['name']
            )
            if not fund_id:
                logger.error("❌ Impossibile inserire il fondo")
                return False
            
            # Inserisce il filing
            filing_id = self.insert_filing(
                fund_id,
                portfolio_data['filing']['accession_number'],
                portfolio_data['filing']['report_date'],
                portfolio_data['filing']['filed_date']
            )
            if not filing_id:
                logger.error("❌ Impossibile inserire il filing")
                return False
            
            # Inserisce le posizioni
            successful_positions = 0
            total_positions = len(portfolio_data['positions'])
            
            for i, position_data in enumerate(portfolio_data['positions']):
                # Inserisce il titolo
                security_id = self.insert_security(
                    position_data['security']['cusip'],
                    position_data['security']['name']
                )
                if not security_id:
                    logger.warning(f"⚠️ Impossibile inserire il titolo per la posizione {i+1}")
                    continue
                
                # Inserisce la posizione
                position_id = self.insert_position(
                    filing_id,
                    security_id,
                    position_data['position']['value'],
                    position_data['position']['shares'],
                    position_data['position'].get('share_type'),
                    position_data['position'].get('investment_discretion'),
                    position_data['position'].get('voting_authority')
                )
                
                if position_id:
                    successful_positions += 1
                else:
                    logger.warning(f"⚠️ Impossibile inserire la posizione {i+1}")
            
            logger.info(f"✅ Inserite {successful_positions}/{total_positions} posizioni con successo")
            return successful_positions > 0
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento dei dati completi: {e}")
            return False

    def debug_position_insertion(self, filing_id: int, security_id: int) -> None:
        """Debug per verificare l'inserimento di una specifica posizione."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return
            
        cursor = self.connection.cursor()
        try:
            logger.info(f"🔍 DEBUG: Verifica posizione Filing ID: {filing_id}, Security ID: {security_id}")
            
            # Verifica se il filing esiste
            cursor.execute("SELECT id, accession_number FROM filings WHERE id = %s", (filing_id,))
            filing = cursor.fetchone()
            if filing:
                logger.info(f"   ✅ Filing trovato: ID {filing[0]}, Accession: {filing[1]}")
            else:
                logger.error(f"   ❌ Filing con ID {filing_id} non trovato")
                return
            
            # Verifica se il security esiste
            cursor.execute("SELECT id, cusip, name FROM securities WHERE id = %s", (security_id,))
            security = cursor.fetchone()
            if security:
                logger.info(f"   ✅ Security trovato: ID {security[0]}, CUSIP: {security[1]}, Nome: {security[2]}")
            else:
                logger.error(f"   ❌ Security con ID {security_id} non trovato")
                return
            
            # Verifica se la posizione esiste
            cursor.execute("""
                SELECT id, value, shares FROM positions 
                WHERE filing_id = %s AND security_id = %s
            """, (filing_id, security_id))
            position = cursor.fetchone()
            
            if position:
                logger.info(f"   ✅ Posizione trovata: ID {position[0]}, Valore: {position[1]}, Shares: {position[2]}")
            else:
                logger.warning(f"   ⚠️ Posizione non trovata per Filing {filing_id}, Security {security_id}")
                
        except Exception as e:
            logger.error(f"❌ Errore nel debug: {e}")
        finally:
            cursor.close()

    def filing_exists(self, accession_number: str) -> bool:
        """
        Verifica se un filing esiste già nel database.
        
        Args:
            accession_number: Numero di accesso del filing
            
        Returns:
            True se il filing esiste, False altrimenti
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return False
            
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT id FROM filings WHERE accession_number = %s", (accession_number,))
            result = cursor.fetchone()
            return result is not None
            
        except Exception as e:
            logger.error(f"❌ Errore nel controllo esistenza filing: {e}")
            return False
        finally:
            cursor.close()

    def get_filing_statistics(self) -> Dict[str, Any]:
        """
        Recupera statistiche sui filing nel database.
        
        Returns:
            Dizionario con statistiche
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return {}
            
        cursor = self.connection.cursor()
        try:
            stats = {}
            
            # Conta filing totali
            cursor.execute("SELECT COUNT(*) FROM filings")
            stats['total_filings'] = cursor.fetchone()[0]
            
            # Conta fondi totali
            cursor.execute("SELECT COUNT(*) FROM funds")
            stats['total_funds'] = cursor.fetchone()[0]
            
            # Conta posizioni totali
            cursor.execute("SELECT COUNT(*) FROM positions")
            stats['total_positions'] = cursor.fetchone()[0]
            
            # Conta titoli unici
            cursor.execute("SELECT COUNT(*) FROM securities")
            stats['total_securities'] = cursor.fetchone()[0]
            
            # Ultimo filing
            cursor.execute("SELECT MAX(filed_date) FROM filings")
            last_filing = cursor.fetchone()[0]
            stats['last_filing_date'] = last_filing
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero statistiche: {e}")
            return {}
        finally:
            cursor.close()

    def debug_last_insertion(self) -> None:
        """
        Debug dell'ultima inserzione per verificare cosa è stato inserito.
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return
            
        cursor = self.connection.cursor()
        try:
            logger.info("🔍 DEBUG: Verifica ultima inserzione")
            
            # Ultimo filing inserito
            cursor.execute("""
                SELECT f.id, f.accession_number, f.report_date, fu.name 
                FROM filings f 
                JOIN funds fu ON f.fund_id = fu.id 
                ORDER BY f.id DESC 
                LIMIT 1
            """)
            last_filing = cursor.fetchone()
            
            if last_filing:
                filing_id, accession, report_date, fund_name = last_filing
                logger.info(f"   Ultimo filing ID: {filing_id}")
                logger.info(f"   Accession: {accession}")
                logger.info(f"   Report Date: {report_date}")
                logger.info(f"   Fund: {fund_name}")
                
                # Conta posizioni per questo filing
                cursor.execute("SELECT COUNT(*) FROM positions WHERE filing_id = %s", (filing_id,))
                position_count = cursor.fetchone()[0]
                logger.info(f"   Posizioni per questo filing: {position_count}")
                
                # Mostra alcune posizioni
                if position_count > 0:
                    cursor.execute("""
                        SELECT p.id, s.cusip, s.name, p.value, p.shares, p.sequence_number
                        FROM positions p
                        JOIN securities s ON p.security_id = s.id
                        WHERE p.filing_id = %s
                        ORDER BY p.value DESC
                        LIMIT 5
                    """, (filing_id,))
                    
                    positions = cursor.fetchall()
                    logger.info(f"   Top 5 posizioni:")
                    for pos in positions:
                        pos_id, cusip, name, value, shares, seq = pos
                        logger.info(f"     ID:{pos_id} {cusip} - ${value:,} - {shares:,} shares - Seq:{seq}")
            else:
                logger.info("   Nessun filing trovato nel database")
                
        except Exception as e:
            logger.error(f"❌ Errore nel debug: {e}")
        finally:
            cursor.close()

    def test_database_connection(self) -> bool:
        """
        Testa la connessione al database e le tabelle.
        
        Returns:
            True se tutto OK, False altrimenti
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return False
            
        cursor = self.connection.cursor()
        try:
            # Testa connessione
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result[0] != 1:
                logger.error("❌ Test connessione fallito")
                return False
                
            # Verifica tabelle
            tables = ['funds', 'filings', 'securities', 'positions']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info(f"✅ Tabella {table}: {count} record")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nel test database: {e}")
            return False
        finally:
            cursor.close()

    # ==================== FUNZIONE ALTERNATIVA PER AGGREGAZIONE ====================

    def insert_complete_portfolio_data_aggregated(self, portfolio_data: Dict[str, Any]) -> bool:
        """
        Inserisce un set completo di dati del portafoglio.
        Versione ALTERNATIVA che aggrega posizioni duplicate dello stesso titolo.
        
        Usa questa funzione se vuoi sommare le posizioni duplicate invece di mantenerle separate.
        """
        try:
            # Inserisce il fondo
            fund_id = self.insert_fund(
                portfolio_data['fund']['cik'],
                portfolio_data['fund']['name']
            )
            if not fund_id:
                return False
            
            # Inserisce il filing
            filing_id = self.insert_filing(
                fund_id,
                portfolio_data['filing']['accession_number'],
                portfolio_data['filing']['report_date'],
                portfolio_data['filing']['filed_date']
            )
            if not filing_id:
                return False
            
            # Aggrega le posizioni per CUSIP
            aggregated_positions = {}
            
            for position_data in portfolio_data['positions']:
                cusip = position_data['security']['cusip']
                
                if cusip not in aggregated_positions:
                    aggregated_positions[cusip] = {
                        'security': position_data['security'],
                        'value': 0,
                        'shares': 0,
                        'share_type': position_data['position'].get('share_type'),
                        'investment_discretion': position_data['position'].get('investment_discretion'),
                        'voting_authority': position_data['position'].get('voting_authority')
                    }
                
                # Aggrega valore e shares
                aggregated_positions[cusip]['value'] += position_data['position']['value']
                aggregated_positions[cusip]['shares'] += position_data['position']['shares']
            
            # Inserisce le posizioni aggregate
            for cusip, agg_data in aggregated_positions.items():
                # Inserisce il titolo
                security_id = self.insert_security(
                    cusip,
                    agg_data['security']['name']
                )
                if not security_id:
                    continue
                
                # Inserisce la posizione aggregata
                position_id = self.insert_position(
                    filing_id,
                    security_id,
                    agg_data['value'],
                    agg_data['shares'],
                    agg_data['share_type'],
                    agg_data['investment_discretion'],
                    agg_data['voting_authority']
                )
                if not position_id:
                    logger.warning(f"❌ Impossibile inserire posizione aggregata per CUSIP {cusip}")
                    continue
            
            logger.info("✅ Dati completi del portafoglio inseriti con successo (aggregati).")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento dei dati completi: {e}")
            return False


    # ==================== FUNZIONI DI ESTRAZIONE ====================

    def get_all_funds(self) -> pd.DataFrame:
        """Restituisce tutti i fondi nel database."""
        query = """
            SELECT 
                id, cik, name, created_at, updated_at
            FROM funds
            ORDER BY name
        """
        
        results, columns = self._execute_query_with_columns(query)
        
        if results:
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Recuperati {len(df)} fondi.")
            return df
        else:
            logger.warning("⚠️ Nessun fondo trovato.")
            return pd.DataFrame()

    def get_portfolio_positions(self, fund_cik: str = None, 
                               report_date: date = None,
                               min_value: int = None,
                               security_name: str = None) -> pd.DataFrame:
        """Restituisce le posizioni del portafoglio con filtri opzionali."""
        query = """
            SELECT 
                f.name as fund_name,
                f.cik as fund_cik,
                fi.accession_number,
                fi.report_date,
                fi.filed_date,
                s.name as security_name,
                s.cusip,
                p.value,
                p.shares,
                p.share_type,
                p.investment_discretion,
                p.voting_authority,
                p.created_at as position_created_at
            FROM positions p
            JOIN filings fi ON p.filing_id = fi.id
            JOIN funds f ON fi.fund_id = f.id
            JOIN securities s ON p.security_id = s.id
        """
        
        conditions = []
        params = []
        
        if fund_cik:
            conditions.append("f.cik = %s")
            params.append(fund_cik)
        
        if report_date:
            conditions.append("fi.report_date = %s")
            params.append(report_date)
        
        if min_value:
            conditions.append("p.value >= %s")
            params.append(min_value)
        
        if security_name:
            conditions.append("s.name LIKE %s")
            params.append(f"%{security_name}%")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY f.name, fi.report_date DESC, p.value DESC"
        
        results, columns = self._execute_query_with_columns(query, tuple(params))
        
        if results:
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Recuperate {len(df)} posizioni.")
            return df
        else:
            logger.warning("⚠️ Nessuna posizione trovata.")
            return pd.DataFrame()

    def get_portfolio_summary(self) -> pd.DataFrame:
        """Restituisce un riassunto aggregato per fondo basato sui soli ultimi filings."""
        query = """
        SELECT
            f.name as fund_name,
            f.cik as fund_cik,
            COUNT(DISTINCT fi.id) as total_filings,
            COUNT(DISTINCT s.id) as total_securities,
            COUNT(p.id) as total_positions,
            SUM(p.value) as total_value,
            SUM(p.shares) as total_shares,
            MAX(fi.report_date) as latest_report_date,
            MIN(fi.report_date) as earliest_report_date
        FROM funds f
        LEFT JOIN filings fi ON f.id = fi.fund_id
        LEFT JOIN positions p ON fi.id = p.filing_id
        LEFT JOIN securities s ON p.security_id = s.id
        WHERE fi.id IN (
            SELECT fi2.id 
            FROM filings fi2 
            WHERE fi2.fund_id = f.id 
            AND fi2.report_date = (
                SELECT MAX(fi3.report_date) 
                FROM filings fi3 
                WHERE fi3.fund_id = f.id
            )
        )
        GROUP BY f.id, f.name, f.cik
        ORDER BY total_value DESC
        """
        
        results, columns = self._execute_query_with_columns(query)
        if results:
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Recuperato riassunto per {len(df)} fondi.")
            return df
        else:
            logger.warning("⚠️ Nessun dato trovato per il riassunto.")
            return pd.DataFrame()

    def get_top_positions(self, limit: int = 10) -> pd.DataFrame:
        """Restituisce le posizioni con valore più alto."""
        query = """
            SELECT 
                f.name as fund_name,
                s.name as security_name,
                s.cusip,
                p.value,
                p.shares,
                p.share_type,
                fi.report_date,
                RANK() OVER (ORDER BY p.value DESC) as value_rank
            FROM positions p
            JOIN filings fi ON p.filing_id = fi.id
            JOIN funds f ON fi.fund_id = f.id
            JOIN securities s ON p.security_id = s.id
            ORDER BY p.value DESC
            LIMIT %s
        """
        
        results, columns = self._execute_query_with_columns(query, (limit,))
        
        if results:
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Recuperate top {len(df)} posizioni per valore.")
            return df
        else:
            logger.warning("⚠️ Nessuna posizione trovata.")
            return pd.DataFrame()

    # ==================== FUNZIONI DI VISUALIZZAZIONE ====================

    def display_data(self, df: pd.DataFrame, title: str = "Dati", 
                    format_money: List[str] = None, 
                    format_numbers: List[str] = None):
        """Visualizza i dati in formato tabellare formattato."""
        if df.empty:
            print(f"\n❌ {title}: Nessun dato disponibile")
            return
        
        # Crea una copia per la formattazione
        display_df = df.copy()
        
        # Formattazione valuta
        if format_money:
            for col in format_money:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(
                        lambda x: f"${x:,.2f}" if pd.notna(x) and x != 0 else "$0.00"
                    )
        
        # Formattazione numeri
        if format_numbers:
            for col in format_numbers:
                if col in display_df.columns:
                    display_df[col] = display_df[col].apply(
                        lambda x: f"{x:,}" if pd.notna(x) else "0"
                    )
        
        print(f"\n{'='*100}")
        print(f"📊 {title}")
        print(f"{'='*100}")
        print(f"Totale record: {len(df)}")
        print(f"{'='*100}")
        
        # Visualizza la tabella
        print(tabulate(display_df, headers='keys', tablefmt='grid', showindex=False))
        print(f"{'='*100}\n")

    def export_data(self, df: pd.DataFrame, filename: str, format_type: str = 'csv'):
        """Esporta i dati in formato CSV o JSON."""
        if df.empty:
            logger.warning("⚠️ Nessun dato da esportare.")
            return
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format_type.lower() == 'csv':
            filename_with_ext = f"{filename}_{timestamp}.csv"
            df.to_csv(filename_with_ext, index=False)
        elif format_type.lower() == 'json':
            filename_with_ext = f"{filename}_{timestamp}.json"
            df.to_json(filename_with_ext, orient='records', indent=2, date_format='iso')
        else:
            logger.error(f"❌ Formato {format_type} non supportato.")
            return
        
        logger.info(f"✅ Dati esportati in: {filename_with_ext}")

    # ==================== FUNZIONI INTEGRATE ====================

    def quick_insert_and_view(self, portfolio_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Inserisce i dati e restituisce immediatamente la vista delle posizioni.
        
        Args:
            portfolio_data: Dati del portafoglio da inserire
            
        Returns:
            DataFrame con le posizioni appena inserite
        """
        logger.info("🔄 Inserimento rapido e visualizzazione...")
        
        # Inserisce i dati
        success = self.insert_complete_portfolio_data(portfolio_data)
        
        if success:
            # Recupera e visualizza le posizioni per il fondo appena inserito
            fund_cik = portfolio_data['fund']['cik']
            positions = self.get_portfolio_positions(fund_cik=fund_cik)
            
            if not positions.empty:
                self.display_data(
                    positions,
                    f"💼 Posizioni per fondo {fund_cik}",
                    format_money=['value'],
                    format_numbers=['shares']
                )
            
            return positions
        else:
            logger.error("❌ Inserimento fallito.")
            return pd.DataFrame()

    def generate_complete_report(self, export_format: str = None) -> Dict[str, pd.DataFrame]:
        """
        Genera un report completo con tutti i dati del portafoglio.
        
        Args:
            export_format: Formato di esportazione ('csv' o 'json', opzionale)
            
        Returns:
            Dizionario con tutti i DataFrames del report
        """
        logger.info("📊 Generazione report completo...")
        
        report = {}
        
        # 1. Tutti i fondi
        funds_df = self.get_all_funds()
        report['funds'] = funds_df
        self.display_data(funds_df, "📋 Fondi nel Database")
        
        # 2. Tutte le posizioni
        positions_df = self.get_portfolio_positions()
        report['positions'] = positions_df
        self.display_data(
            positions_df,
            "💼 Tutte le Posizioni",
            format_money=['value'],
            format_numbers=['shares']
        )
        
        # 3. Riassunto per fondo
        summary_df = self.get_portfolio_summary()
        report['summary'] = summary_df
        self.display_data(
            summary_df,
            "📊 Riassunto per Fondo",
            format_money=['total_value'],
            format_numbers=['total_filings', 'total_securities', 'total_positions', 'total_shares']
        )
        
        # 4. Top posizioni
        top_positions_df = self.get_top_positions(limit=10)
        report['top_positions'] = top_positions_df
        self.display_data(
            top_positions_df,
            "🏆 Top 10 Posizioni per Valore",
            format_money=['value'],
            format_numbers=['shares']
        )
        
        # Esportazione se richiesta
        if export_format:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            for name, df in report.items():
                if not df.empty:
                    self.export_data(df, f"portfolio_report_{name}_{timestamp}", export_format)
        
        return report

    def interactive_menu(self):
        """Menu interattivo per gestire il portafoglio."""
        while True:
            print("\n" + "="*80)
            print("🚀 PORTFOLIO MANAGER - Menu Principale")
            print("="*80)
            print("1. 🔧 Inizializza Database")
            print("2. 📥 Inserisci Dati Portafoglio")
            print("3. 📊 Visualizza Tutti i Dati")
            print("4. 🔍 Ricerca Posizioni")
            print("5. 📈 Riassunto per Fondo")
            print("6. 🏆 Top Posizioni")
            print("7. 📁 Esporta Report Completo")
            print("8. 🔄 Inserisci e Visualizza (Rapido)")
            print("9. ❌ Esci")
            print("10. 🔧 Inizializza Tabelle Insider")
            print("11. 📊 Visualizza Transazioni Insider")
            print("12. 🏢 Riassunto Insider per Company")
            print("13. 📈 Statistiche Insider")
            print("="*80)
            
            choice = input("Seleziona un'opzione (1-13): ").strip()
            
            if choice == '1':
                self.initialize_database()
            
            elif choice == '2':
                self._interactive_insert()
            
            elif choice == '3':
                self.generate_complete_report()
            
            elif choice == '4':
                self._interactive_search()
            
            elif choice == '5':
                summary = self.get_portfolio_summary()
                self.display_data(
                    summary,
                    "📊 Riassunto per Fondo",
                    format_money=['total_value'],
                    format_numbers=['total_filings', 'total_securities', 'total_positions', 'total_shares']
                )
            
            elif choice == '6':
                limit = int(input("Quante posizioni mostrare? (default 10): ") or "10")
                top_positions = self.get_top_positions(limit=limit)
                self.display_data(
                    top_positions,
                    f"🏆 Top {limit} Posizioni per Valore",
                    format_money=['value'],
                    format_numbers=['shares']
                )
            
            elif choice == '7':
                format_choice = input("Formato esportazione (csv/json): ").lower()
                if format_choice in ['csv', 'json']:
                    self.generate_complete_report(export_format=format_choice)
                else:
                    print("❌ Formato non valido.")
            
            elif choice == '8':
                self._interactive_quick_insert()
            
            elif choice == '9':
                print("👋 Arrivederci!")
                break
            
            elif choice == '10':
                self.initialize_insider_tables()
        
            elif choice == '11':
                company_cik = input("CIK company (opzionale): ").strip() or None
                transactions = self.get_insider_transactions(company_cik=company_cik, limit=20)
                self.display_insider_transactions(transactions, "Transazioni Insider Recenti")
            
            elif choice == '12':
                company_cik = input("CIK company (opzionale): ").strip() or None
                summary = self.get_insider_summary_by_company(company_cik)
                self.display_data(summary, "Riassunto Insider per Company", 
                                format_money=['total_purchases', 'total_sales'],
                                format_numbers=['total_insiders', 'total_filings', 'total_transactions'])
            
            elif choice == '13':
                stats = self.get_insider_statistics()
                print(f"\n📊 Statistiche Insider:")
                for key, value in stats.items():
                    print(f"   {key}: {value}")
            
            else:
                print("❌ Opzione non valida. Riprova.")

    def _interactive_insert(self):
                """Inserimento interattivo dei dati."""
                print("\n📥 Inserimento Dati Portafoglio")
                print("-" * 50)
                
                # Dati del fondo
                fund_cik = input("CIK del fondo: ").strip()
                fund_name = input("Nome del fondo: ").strip()
                
                # Dati del filing
                accession_number = input("Numero di accesso: ").strip()
                report_date_str = input("Data report (YYYY-MM-DD): ").strip()
                filed_date_str = input("Data deposito (YYYY-MM-DD): ").strip()
                
                try:
                    report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
                    filed_date = datetime.strptime(filed_date_str, '%Y-%m-%d').date()
                except ValueError:
                    print("❌ Formato data non valido. Usa YYYY-MM-DD")
                    return
                
                # Dati delle posizioni
                positions = []
                print("\nInserimento posizioni (premi Enter vuoto per terminare):")
                
                while True:
                    print(f"\n--- Posizione {len(positions) + 1} ---")
                    cusip = input("CUSIP del titolo: ").strip()
                    if not cusip:
                        break
                    
                    security_name = input("Nome del titolo: ").strip()
                    if not security_name:
                        break
                    
                    try:
                        value = int(input("Valore posizione: ").strip())
                        shares = int(input("Numero azioni: ").strip())
                    except ValueError:
                        print("❌ Valore o numero azioni non valido")
                        continue
                    
                    share_type = input("Tipo azioni (opzionale): ").strip() or None
                    investment_discretion = input("Discrezionalità investimento (opzionale): ").strip() or None
                    voting_authority = input("Autorità di voto (opzionale): ").strip() or None
                    
                    positions.append({
                        'security': {'cusip': cusip, 'name': security_name},
                        'position': {
                            'value': value,
                            'shares': shares,
                            'share_type': share_type,
                            'investment_discretion': investment_discretion,
                            'voting_authority': voting_authority
                        }
                    })
                    
                    print(f"✅ Posizione {len(positions)} aggiunta")
                
                if not positions:
                    print("❌ Nessuna posizione inserita")
                    return
                
                # Crea il dizionario dei dati
                portfolio_data = {
                    'fund': {'cik': fund_cik, 'name': fund_name},
                    'filing': {
                        'accession_number': accession_number,
                        'report_date': report_date,
                        'filed_date': filed_date
                    },
                    'positions': positions
                }
                
                # Inserisce i dati
                success = self.insert_complete_portfolio_data(portfolio_data)
                
                if success:
                    print(f"✅ Portafoglio inserito con successo! ({len(positions)} posizioni)")
                else:
                    print("❌ Errore nell'inserimento del portafoglio")

    def _interactive_search(self):
                """Ricerca interattiva delle posizioni."""
                print("\n🔍 Ricerca Posizioni")
                print("-" * 50)
                
                # Filtri opzionali
                fund_cik = input("CIK del fondo (opzionale): ").strip() or None
                
                report_date_str = input("Data report (YYYY-MM-DD, opzionale): ").strip()
                report_date = None
                if report_date_str:
                    try:
                        report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        print("❌ Formato data non valido, ignorato")
                
                min_value_str = input("Valore minimo posizione (opzionale): ").strip()
                min_value = None
                if min_value_str:
                    try:
                        min_value = int(min_value_str)
                    except ValueError:
                        print("❌ Valore minimo non valido, ignorato")
                
                security_name = input("Nome titolo (ricerca parziale, opzionale): ").strip() or None
                
                # Esegue la ricerca
                positions = self.get_portfolio_positions(
                    fund_cik=fund_cik,
                    report_date=report_date,
                    min_value=min_value,
                    security_name=security_name
                )
                
                if not positions.empty:
                    self.display_data(
                        positions,
                        "🔍 Risultati Ricerca",
                        format_money=['value'],
                        format_numbers=['shares']
                    )
                    
                    # Opzione esportazione
                    export_choice = input("\nEsportare i risultati? (y/n): ").lower()
                    if export_choice == 'y':
                        format_choice = input("Formato (csv/json): ").lower()
                        if format_choice in ['csv', 'json']:
                            self.export_data(positions, "search_results", format_choice)
                else:
                    print("❌ Nessuna posizione trovata con i criteri specificati")

    # def _interactive_quick_insert(self):
    #             """Inserimento rapido con dati di esempio."""
    #             print("\n🔄 Inserimento Rapido con Dati di Esempio")
    #             print("-" * 50)
                
    #             # Dati di esempio
    #             portfolio_data = {
    #                 'fund': {
    #                     'cik': '0001067983',
    #                     'name': 'Berkshire Hathaway Inc'
    #                 },
    #                 'filing': {
    #                     'accession_number': '0001067983-24-000017',
    #                     'report_date': date(2024, 3, 31),
    #                     'filed_date': date(2024, 5, 15)
    #                 },
    #                 'positions': [
    #                     {
    #                         'security': {
    #                             'cusip': '030420103',
    #                             'name': 'American Express Co'
    #                         },
    #                         'position': {
    #                             'value': 25000000000,
    #                             'shares': 151610700,
    #                             'share_type': 'SH',
    #                             'investment_discretion': 'SOLE',
    #                             'voting_authority': 'SOLE'
    #                         }
    #                     },
    #                     {
    #                         'security': {
    #                             'cusip': '037833100',
    #                             'name': 'Apple Inc'
    #                         },
    #                         'position': {
    #                             'value': 162000000000,
    #                             'shares': 915560000,
    #                             'share_type': 'SH',
    #                             'investment_discretion': 'SOLE',
    #                             'voting_authority': 'SOLE'
    #                         }
    #                     },
    #                     {
    #                         'security': {
    #                             'cusip': '042735100',
    #                             'name': 'Coca-Cola Co'
    #                         },
    #                         'position': {
    #                             'value': 23000000000,
    #                             'shares': 400000000,
    #                             'share_type': 'SH',
    #                             'investment_discretion': 'SOLE',
    #                             'voting_authority': 'SOLE'
    #                         }
    #                     }
    #                 ]
    #             }
                
    #             confirm = input("Inserire i dati di esempio di Berkshire Hathaway? (y/n): ").lower()
    #             if confirm == 'y':
    #                 positions = self.quick_insert_and_view(portfolio_data)
    #                 if not positions.empty:
    #                     print(f"✅ Inseriti {len(portfolio_data['positions'])} posizioni di esempio")
    #                 else:
    #                     print("❌ Errore nell'inserimento dei dati di esempio")
    #             else:
    #                 print("❌ Inserimento annullato")

    def close_connection(self):
                """Chiude la connessione al database."""
                if self.connection:
                    self.connection.close()
                    logger.info("🔒 Connessione database chiusa")

    def __del__(self):
                """Destructor per chiudere automaticamente la connessione."""
                self.close_connection()

    def __enter__(self):
                """Context manager entry."""
                return self

    def __exit__(self, exc_type, exc_val, exc_tb):
                """Context manager exit."""
                self.close_connection()
    def initialize_insider_tables(self) -> bool:
        """Crea le tabelle per i dati insider nel database."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return False
            
        cursor = self.connection.cursor()
        try:
            logger.info("🔧 Creazione tabelle insider...")
            
            # Tabella degli insider
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS insiders (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cik VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    title VARCHAR(255),
                    is_director BOOLEAN DEFAULT FALSE,
                    is_officer BOOLEAN DEFAULT FALSE,
                    is_ten_percent_owner BOOLEAN DEFAULT FALSE,
                    is_other BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """)

            # Tabella delle companies (per le insider transactions)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    cik VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    ticker VARCHAR(10),
                    exchange VARCHAR(50),
                    sector VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                );
            """)

            # Tabella dei filing insider (Form 4)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS insider_filings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    accession_number VARCHAR(30) UNIQUE NOT NULL,
                    insider_id INT NOT NULL,
                    company_id INT NOT NULL,
                    filed_date DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (insider_id) REFERENCES insiders(id) ON DELETE CASCADE,
                    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
                    INDEX idx_filed_date (filed_date),
                    INDEX idx_insider_company (insider_id, company_id)
                );
            """)

            # Tabella delle transazioni insider
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS insider_transactions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    filing_id INT NOT NULL,
                    security_title VARCHAR(255) NOT NULL,
                    transaction_date DATE,
                    transaction_code VARCHAR(10),
                    transaction_shares DECIMAL(15,2) DEFAULT 0,
                    transaction_price DECIMAL(10,4) DEFAULT 0,
                    shares_owned_after DECIMAL(15,2) DEFAULT 0,
                    direct_indirect VARCHAR(10) DEFAULT 'D',
                    is_derivative BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (filing_id) REFERENCES insider_filings(id) ON DELETE CASCADE,
                    INDEX idx_transaction_date (transaction_date),
                    INDEX idx_transaction_code (transaction_code),
                    INDEX idx_security_title (security_title)
                );
            """)

            # Tabella degli holdings insider
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS insider_holdings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    filing_id INT NOT NULL,
                    security_title VARCHAR(255) NOT NULL,
                    shares_owned DECIMAL(15,2) DEFAULT 0,
                    direct_indirect VARCHAR(10) DEFAULT 'D',
                    is_derivative BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (filing_id) REFERENCES insider_filings(id) ON DELETE CASCADE,
                    INDEX idx_security_title (security_title)
                );
            """)

            self.connection.commit()
            logger.info("✅ Tabelle insider create con successo.")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nella creazione delle tabelle insider: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()

    def insert_insider(self, cik: str, name: str, title: str = None, 
                    relationship_info: Dict[str, Any] = None) -> Optional[int]:
        """Inserisce un nuovo insider o restituisce l'ID se già esiste."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            # Verifica se esiste già
            cursor.execute("SELECT id FROM insiders WHERE cik = %s", (cik,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Insider con CIK {cik} già esistente, ID: {result[0]}")
                return result[0]
            
            # Estrai informazioni dalla relationship se disponibile
            is_director = False
            is_officer = False
            is_ten_percent = False
            is_other = False
            
            if relationship_info:
                is_director = relationship_info.get('isDirector', False)
                is_officer = relationship_info.get('isOfficer', False) 
                is_ten_percent = relationship_info.get('isTenPercentOwner', False)
                is_other = relationship_info.get('isOther', False)
                if not title and 'title' in relationship_info:
                    title = relationship_info['title']
            
            # Inserisce nuovo insider
            cursor.execute("""
                INSERT INTO insiders (cik, name, title, is_director, is_officer, 
                                    is_ten_percent_owner, is_other) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (cik, name, title, is_director, is_officer, is_ten_percent, is_other))
            
            self.connection.commit()
            insider_id = cursor.lastrowid
            logger.info(f"✅ Insider inserito con ID: {insider_id}")
            return insider_id
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento dell'insider: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_company(self, cik: str, name: str, ticker: str = None, 
                    exchange: str = None, sector: str = None) -> Optional[int]:
        """Inserisce una nuova company o restituisce l'ID se già esiste."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            # Verifica se esiste già
            cursor.execute("SELECT id FROM companies WHERE cik = %s", (cik,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Company con CIK {cik} già esistente, ID: {result[0]}")
                return result[0]
            
            # Inserisce nuova company
            cursor.execute("""
                INSERT INTO companies (cik, name, ticker, exchange, sector) 
                VALUES (%s, %s, %s, %s, %s)
            """, (cik, name, ticker, exchange, sector))
            
            self.connection.commit()
            company_id = cursor.lastrowid
            logger.info(f"✅ Company inserita con ID: {company_id}")
            return company_id
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento della company: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_insider_filing(self, accession_number: str, insider_id: int, 
                            company_id: int, filed_date: date) -> Optional[int]:
        """Inserisce un nuovo filing insider o restituisce l'ID se già esiste."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            # Verifica se esiste già
            cursor.execute("SELECT id FROM insider_filings WHERE accession_number = %s", 
                        (accession_number,))
            result = cursor.fetchone()
            if result:
                logger.info(f"Filing insider con numero {accession_number} già esistente, ID: {result[0]}")
                return result[0]
            
            # Inserisce nuovo filing
            cursor.execute("""
                INSERT INTO insider_filings (accession_number, insider_id, company_id, filed_date)
                VALUES (%s, %s, %s, %s)
            """, (accession_number, insider_id, company_id, filed_date))
            
            self.connection.commit()
            filing_id = cursor.lastrowid
            logger.info(f"✅ Filing insider inserito con ID: {filing_id}")
            return filing_id
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento del filing insider: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_insider_transaction(self, filing_id: int, transaction_data: Dict[str, Any]) -> Optional[int]:
        """Inserisce una nuova transazione insider."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO insider_transactions (
                    filing_id, security_title, transaction_date, transaction_code,
                    transaction_shares, transaction_price, shares_owned_after,
                    direct_indirect, is_derivative
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                filing_id,
                transaction_data.get('security_title', ''),
                transaction_data.get('transaction_date'),
                transaction_data.get('transaction_code', ''),
                transaction_data.get('transaction_shares', 0),
                transaction_data.get('transaction_price', 0),
                transaction_data.get('shares_owned_after', 0),
                transaction_data.get('direct_indirect', 'D'),
                transaction_data.get('is_derivative', False)
            ))
            
            self.connection.commit()
            transaction_id = cursor.lastrowid
            logger.info(f"✅ Transazione insider inserita con ID: {transaction_id}")
            return transaction_id
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento della transazione insider: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_insider_holding(self, filing_id: int, holding_data: Dict[str, Any]) -> Optional[int]:
        """Inserisce un nuovo holding insider."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return None
            
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO insider_holdings (
                    filing_id, security_title, shares_owned, direct_indirect, is_derivative
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                filing_id,
                holding_data.get('security_title', ''),
                holding_data.get('shares_owned', 0),
                holding_data.get('direct_indirect', 'D'),
                holding_data.get('is_derivative', False)
            ))
            
            self.connection.commit()
            holding_id = cursor.lastrowid
            logger.info(f"✅ Holding insider inserito con ID: {holding_id}")
            return holding_id
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento dell'holding insider: {e}")
            self.connection.rollback()
            return None
        finally:
            cursor.close()

    def insert_insider_data(self, parsed_data: Dict[str, Any]) -> bool:
        """
        Inserisce un set completo di dati insider (filing, transazioni, holdings).
        
        Args:
            parsed_data: Dati parsati dal Form 4
            
        Returns:
            True se successo, False altrimenti
        """
        try:
            # Estrai informazioni
            insider_info = parsed_data.get('insider_info', {})
            issuer_info = parsed_data.get('issuer_info', {})
            filing_info = parsed_data.get('filing_info', {})
            transactions = parsed_data.get('transactions', [])
            holdings = parsed_data.get('holdings', [])
            
            if not insider_info or not issuer_info:
                logger.error("❌ Informazioni insider o issuer mancanti")
                return False
            
            # 1. Inserisci insider
            insider_id = self.insert_insider(
                insider_info.get('cik', ''),
                insider_info.get('name', ''),
                insider_info.get('relationship', {}).get('title'),
                insider_info.get('relationship', {})
            )
            if not insider_id:
                logger.error("❌ Impossibile inserire l'insider")
                return False
            
            # 2. Inserisci company
            company_id = self.insert_company(
                issuer_info.get('cik', ''),
                issuer_info.get('name', ''),
                issuer_info.get('ticker')
            )
            if not company_id:
                logger.error("❌ Impossibile inserire la company")
                return False
            
            # 3. Inserisci filing
            filing_id = self.insert_insider_filing(
                filing_info.get('accession_number', ''),
                insider_id,
                company_id,
                filing_info.get('filed_date')
            )
            if not filing_id:
                logger.error("❌ Impossibile inserire il filing")
                return False
            
            # 4. Inserisci transazioni
            successful_transactions = 0
            for transaction_data in transactions:
                transaction_id = self.insert_insider_transaction(filing_id, transaction_data)
                if transaction_id:
                    successful_transactions += 1
            
            # 5. Inserisci holdings
            successful_holdings = 0
            for holding_data in holdings:
                holding_id = self.insert_insider_holding(filing_id, holding_data)
                if holding_id:
                    successful_holdings += 1
            
            logger.info(f"✅ Insider data inseriti: {successful_transactions} transazioni, {successful_holdings} holdings")
            return True
            
        except Exception as e:
            logger.error(f"❌ Errore nell'inserimento dati insider completi: {e}")
            return False

    def insider_filing_exists(self, accession_number: str) -> bool:
        """
        Verifica se un filing insider esiste già nel database.
        
        Args:
            accession_number: Numero di accesso del filing
            
        Returns:
            True se il filing esiste, False altrimenti
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return False
            
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT id FROM insider_filings WHERE accession_number = %s", 
                        (accession_number,))
            result = cursor.fetchone()
            return result is not None
            
        except Exception as e:
            logger.error(f"❌ Errore nel controllo esistenza filing insider: {e}")
            return False
        finally:
            cursor.close()

    def get_insider_statistics(self) -> Dict[str, Any]:
        """
        Recupera statistiche sui dati insider nel database.
        
        Returns:
            Dizionario con statistiche
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database disponibile.")
            return {}
            
        cursor = self.connection.cursor()
        try:
            stats = {}
            
            # Conta insider totali
            cursor.execute("SELECT COUNT(*) FROM insiders")
            stats['total_insiders'] = cursor.fetchone()[0]
            
            # Conta companies totali
            cursor.execute("SELECT COUNT(*) FROM companies")
            stats['total_companies'] = cursor.fetchone()[0]
            
            # Conta filing insider totali
            cursor.execute("SELECT COUNT(*) FROM insider_filings")
            stats['total_insider_filings'] = cursor.fetchone()[0]
            
            # Conta transazioni totali
            cursor.execute("SELECT COUNT(*) FROM insider_transactions")
            stats['total_insider_transactions'] = cursor.fetchone()[0]
            
            # Conta holdings totali
            cursor.execute("SELECT COUNT(*) FROM insider_holdings")
            stats['total_insider_holdings'] = cursor.fetchone()[0]
            
            # Ultimo filing insider
            cursor.execute("SELECT MAX(filed_date) FROM insider_filings")
            last_filing = cursor.fetchone()[0]
            stats['last_insider_filing_date'] = last_filing
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero statistiche insider: {e}")
            return {}
        finally:
            cursor.close()

    def get_insider_transactions(self, company_cik: str = None, insider_cik: str = None,
                            start_date: date = None, end_date: date = None,
                            transaction_code: str = None, limit: int = 100) -> pd.DataFrame:
        """
        Recupera transazioni insider con filtri opzionali.
        
        Args:
            company_cik: CIK della company
            insider_cik: CIK dell'insider  
            start_date: Data di inizio
            end_date: Data di fine
            transaction_code: Codice transazione (P, S, A, etc.)
            limit: Limite risultati
            
        Returns:
            DataFrame con le transazioni
        """
        query = """
            SELECT 
                i.name as insider_name,
                i.cik as insider_cik,
                i.title as insider_title,
                c.name as company_name,
                c.cik as company_cik,
                c.ticker as company_ticker,
                if.accession_number,
                if.filed_date,
                it.security_title,
                it.transaction_date,
                it.transaction_code,
                it.transaction_shares,
                it.transaction_price,
                it.shares_owned_after,
                it.direct_indirect,
                it.is_derivative,
                (it.transaction_shares * it.transaction_price) as transaction_value
            FROM insider_transactions it
            JOIN insider_filings if ON it.filing_id = if.id
            JOIN insiders i ON if.insider_id = i.id
            JOIN companies c ON if.company_id = c.id
        """
        
        conditions = []
        params = []
        
        if company_cik:
            conditions.append("c.cik = %s")
            params.append(company_cik)
        
        if insider_cik:
            conditions.append("i.cik = %s")
            params.append(insider_cik)
            
        if start_date:
            conditions.append("it.transaction_date >= %s")
            params.append(start_date)
            
        if end_date:
            conditions.append("it.transaction_date <= %s")
            params.append(end_date)
            
        if transaction_code:
            conditions.append("it.transaction_code = %s")
            params.append(transaction_code)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY it.transaction_date DESC, if.filed_date DESC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        results, columns = self._execute_query_with_columns(query, tuple(params))
        
        if results:
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Recuperate {len(df)} transazioni insider.")
            return df
        else:
            logger.warning("⚠️ Nessuna transazione insider trovata.")
            return pd.DataFrame()

    def get_insider_summary_by_company(self, company_cik: str = None) -> pd.DataFrame:
        """
        Restituisce un riassunto delle transazioni insider per company.
        """
        query = """
            SELECT
                c.name as company_name,
                c.cik as company_cik,
                c.ticker as company_ticker,
                COUNT(DISTINCT i.id) as total_insiders,
                COUNT(DISTINCT if.id) as total_filings,
                COUNT(it.id) as total_transactions,
                COALESCE(SUM(CASE WHEN it.transaction_code IN ('P', 'A') THEN it.transaction_shares * it.transaction_price ELSE 0 END), 0) as total_purchases,
                COALESCE(SUM(CASE WHEN it.transaction_code = 'S' THEN it.transaction_shares * it.transaction_price ELSE 0 END), 0) as total_sales,
                MAX(if.filed_date) as latest_filing_date,
                MIN(if.filed_date) as earliest_filing_date
            FROM companies c
            LEFT JOIN insider_filings if ON c.id = if.company_id
            LEFT JOIN insiders i ON if.insider_id = i.id
            LEFT JOIN insider_transactions it ON if.id = it.filing_id
        """
        
        params = []
        if company_cik:
            query += " WHERE c.cik = %s"
            params.append(company_cik)
        
        query += """
            GROUP BY c.id, c.name, c.cik, c.ticker
            HAVING total_transactions > 0
            ORDER BY total_transactions DESC
        """
        
        results, columns = self._execute_query_with_columns(query, tuple(params))
        
        if results:
            df = pd.DataFrame(results, columns=columns)
            logger.info(f"✅ Recuperato riassunto insider per {len(df)} companies.")
            return df
        else:
            logger.warning("⚠️ Nessun dato insider trovato.")
            return pd.DataFrame()

    def display_insider_transactions(self, df: pd.DataFrame, title: str = "Transazioni Insider"):
        """Visualizza le transazioni insider in formato tabellare formattato."""
        if df.empty:
            print(f"\n❌ {title}: Nessun dato disponibile")
            return
        
        # Crea una copia per la formattazione
        display_df = df.copy()
        
        # Formattazione valori monetari
        if 'transaction_value' in display_df.columns:
            display_df['transaction_value'] = display_df['transaction_value'].apply(
                lambda x: f"${x:,.2f}" if pd.notna(x) and x != 0 else "$0.00"
            )
        
        if 'transaction_price' in display_df.columns:
            display_df['transaction_price'] = display_df['transaction_price'].apply(
                lambda x: f"${x:.2f}" if pd.notna(x) and x != 0 else "$0.00"
            )
        
        # Formattazione numeri
        if 'transaction_shares' in display_df.columns:
            display_df['transaction_shares'] = display_df['transaction_shares'].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else "0"
            )
        
        if 'shares_owned_after' in display_df.columns:
            display_df['shares_owned_after'] = display_df['shares_owned_after'].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else "0"
            )
        
        print(f"\n{'='*120}")
        print(f"📊 {title}")
        print(f"{'='*120}")
        print(f"Totale record: {len(df)}")
        print(f"{'='*120}")
        
        # Mostra solo colonne più importanti per il display
        display_columns = ['insider_name', 'company_ticker', 'transaction_date', 'transaction_code', 
                        'transaction_shares', 'transaction_price', 'transaction_value']
        
        available_columns = [col for col in display_columns if col in display_df.columns]
        
        if available_columns:
            from tabulate import tabulate
            print(tabulate(display_df[available_columns], headers='keys', tablefmt='grid', showindex=False))
        else:
            from tabulate import tabulate
            print(tabulate(display_df, headers='keys', tablefmt='grid', showindex=False))
        
        print(f"{'='*120}\n")


# Esempio 
if __name__ == "__main__":
    # Configurazione database
    DB_CONFIG = {
        'host': 'xxxxx',
        'user': 'xxxxx',
        'password': 'xxxxxxx',
        'database': 'xxxxx',
        'port': xxxx
    }
            
    # Utilizzo con context manager
    with PortfolioManager(**DB_CONFIG) as pm:
        # Avvia il menu interattivo
        pm.interactive_menu()
            
    # Oppure utilizzo diretto
    # pm = PortfolioManager(**DB_CONFIG)
    # try:
    #     pm.interactive_menu()
    # finally:
    #     pm.close_connection()
