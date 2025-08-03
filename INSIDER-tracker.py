import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
import logging
from typing import List, Dict, Any, Optional
import time
import xml.etree.ElementTree as ET
import json

# Importa il PortfolioManager dal file separato
from login_mysql import PortfolioManager

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExtendedPortfolioManager(PortfolioManager):
    """
    Estensione del PortfolioManager con funzionalità insider.
    """
    
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
        
        # Usa il metodo _execute_query_with_columns se esiste, altrimenti fallback
        try:
            results, columns = self._execute_query_with_columns(query, tuple(params))
            if results:
                df = pd.DataFrame(results, columns=columns)
                logger.info(f"✅ Recuperate {len(df)} transazioni insider.")
                return df
        except AttributeError:
            # Fallback se _execute_query_with_columns non esiste
            cursor = self.connection.cursor()
            try:
                cursor.execute(query, tuple(params))
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                if results:
                    df = pd.DataFrame(results, columns=columns)
                    logger.info(f"✅ Recuperate {len(df)} transazioni insider.")
                    return df
            finally:
                cursor.close()
        
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
        
        # Usa il metodo _execute_query_with_columns se esiste, altrimenti fallback
        try:
            results, columns = self._execute_query_with_columns(query, tuple(params))
            if results:
                df = pd.DataFrame(results, columns=columns)
                logger.info(f"✅ Recuperato riassunto insider per {len(df)} companies.")
                return df
        except AttributeError:
            # Fallback se _execute_query_with_columns non esiste
            cursor = self.connection.cursor()
            try:
                cursor.execute(query, tuple(params))
                results = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                if results:
                    df = pd.DataFrame(results, columns=columns)
                    logger.info(f"✅ Recuperato riassunto insider per {len(df)} companies.")
                    return df
            finally:
                cursor.close()
        
        logger.warning("⚠️ Nessun dato insider trovato.")
        return pd.DataFrame()


class SECInsiderDownloader:
    """
    Classe per scaricare dati insider transactions (Form 4) dalla SEC e integrarli nel database.
    Integrato con il PortfolioManager esistente.
    """
    
    def __init__(self, portfolio_manager: ExtendedPortfolioManager, user_agent: str = 'info.cashcrew@gmail.com'):
        """
        Inizializza il downloader.
        
        Args:
            portfolio_manager: Istanza del PortfolioManager esteso
            user_agent: User agent per le richieste HTTP
        """
        self.portfolio_manager = portfolio_manager
        self.headers = {'User-Agent': user_agent}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    # 1. Correzione nel metodo get_company_info_by_cik (linea ~548)
    def get_company_info_by_cik(self, cik: str) -> Dict[str, str]:
        """
        Recupera informazioni base della company tramite CIK.
        
        Args:
            cik: CIK della company (senza zeri iniziali)
            
        Returns:
            Dizionario con informazioni della company
        """
        cik_padded = cik.zfill(10)
        
        try:
            response = self.session.get(
                f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
            )
            response.raise_for_status()
            
            data = response.json()
            # print(data.keys())
                        
            return {  # CORREZIONE: era "return data{"
                'cik': cik,
                'name': data.get('name', f'Unknown Company {cik}'),
                'sic': data.get('sic', ''),
                'sicDescription': data.get('sicDescription', ''),
                'ein': data.get('ein', ''),
                'ticker': data.get('tickers', [None])[0] if data.get('tickers') else None,
                'exchange': data.get('exchanges', [None])[0] if data.get('exchanges') else None,
                'addresses': data.get('addresses', {})
            }
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero info company per CIK {cik}: {e}")
            return {
                'cik': cik,
                'name': f'Unknown Company {cik}',
                'sic': '',
                'sicDescription': '',
                'ein': '',
                'ticker': None,
                'exchange': None,
                'addresses': {}
            }
    def get_form4_filings_by_cik(self, cik: str, start_date: Optional[date] = None, 
                                end_date: Optional[date] = None) -> pd.DataFrame:
        """
        VERSIONE CORRETTA - Recupera tutti i filing Form 4 per un CIK specifico.
        """
        cik_padded = cik.zfill(10)
        
        try:
            logger.info(f"🔍 Recupero filing Form 4 per CIK: {cik}")
            
            response = self.session.get(
                f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
            )
            response.raise_for_status()
            
            recent = response.json()['filings']['recent']
            filings_df = pd.DataFrame.from_dict(recent)
            
            # Filtra solo Form 4
            form4_filings = filings_df[filings_df['form'] == '4'].reset_index(drop=True)
            
            if form4_filings.empty:
                logger.info(f"ℹ️ Nessun Form 4 trovato per CIK {cik}")
                return pd.DataFrame()
            
            # Converte le date per il filtraggio
            form4_filings['filingDate'] = pd.to_datetime(form4_filings['filingDate']).dt.date
            
            # Applica filtri di data se specificati
            if start_date:
                form4_filings = form4_filings[form4_filings['filingDate'] >= start_date]
            if end_date:
                form4_filings = form4_filings[form4_filings['filingDate'] <= end_date]
            
            form4_filings = form4_filings.reset_index(drop=True)
            
            # FIX: URL corretto per i documenti EDGAR
            form4_filings['accession_withdash'] = form4_filings['accessionNumber']
            form4_filings['accession_nodash'] = form4_filings['accessionNumber'].str.replace('-', '')
            
            # URL corretto per il documento principale (spesso .xml o .txt)
            form4_filings['url_document'] = [
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{nodash}/{withdash}.txt"
                for nodash, withdash in zip(form4_filings['accession_nodash'], form4_filings['accession_withdash'])
            ]
            
            logger.info(f"✅ Trovati {len(form4_filings)} filing Form 4")
            return form4_filings
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero filing Form 4: {e}")
            return pd.DataFrame()    

    def parse_form4_content(self, document_url: str, content: bytes, filing_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        NUOVO METODO - Parser più robusto per Form 4 (gestisce SGML e XML)
        """
        try:
            logger.info(f"📄 Analisi contenuto Form 4: {document_url}")
            
            # Prova prima come XML
            try:
                soup = BeautifulSoup(content, 'xml')
                logger.info("✅ Documento parsato come XML")
            except:
                # Fallback su parser HTML per SGML
                soup = BeautifulSoup(content, 'html.parser')
                logger.info("✅ Documento parsato come SGML/HTML")
            
            parsed_data = {
                'insider_info': {},
                'issuer_info': {},
                'transactions': [],
                'holdings': [],
                'filing_info': filing_info
            }
            
            # DEBUG: Mostra tag disponibili
            all_tags = set([tag.name for tag in soup.find_all() if tag.name])
            logger.info(f"🏷️ Tag trovati nel documento: {sorted(list(all_tags))[:20]}...")  # primi 20
            
            # Metodo più robusto per trovare informazioni
            # Cerca possibili varianti dei tag
            reporting_owner_tags = ['reportingOwner', 'reporting-owner', 'REPORTING-OWNER']
            issuer_tags = ['issuer', 'ISSUER']
            
            # Cerca reporting owner con varianti
            reporting_owner = None
            for tag_name in reporting_owner_tags:
                reporting_owner = soup.find(tag_name)
                if reporting_owner:
                    logger.info(f"✅ Trovato reporting owner con tag: {tag_name}")
                    break
            
            if reporting_owner:
                parsed_data['insider_info'] = self._extract_insider_info(reporting_owner)
            else:
                logger.warning("⚠️ Nessun reporting owner trovato")
            
            # Cerca issuer con varianti
            issuer = None
            for tag_name in issuer_tags:
                issuer = soup.find(tag_name)
                if issuer:
                    logger.info(f"✅ Trovato issuer con tag: {tag_name}")
                    break
            
            if issuer:
                parsed_data['issuer_info'] = self._extract_issuer_info(issuer)
            else:
                logger.warning("⚠️ Nessun issuer trovato")
            
            # Cerca transazioni con metodo più robusto
            transactions = self._extract_transactions(soup)
            parsed_data['transactions'] = transactions
            
            # Cerca holdings
            holdings = self._extract_holdings(soup)
            parsed_data['holdings'] = holdings
            
            logger.info(f"✅ Estratte {len(parsed_data['transactions'])} transazioni e {len(parsed_data['holdings'])} holdings")
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"❌ Errore nel parsing Form 4 {document_url}: {e}")
            import traceback
            traceback.print_exc()
            return {
                'insider_info': {},
                'issuer_info': {},
                'transactions': [],
                'holdings': [],
                'filing_info': filing_info
            }

    def _extract_insider_info(self, reporting_owner) -> Dict[str, Any]:
        """Estrai informazioni insider con metodo più robusto"""
        insider_info = {}
        
        # Possibili varianti dei tag
        cik_tags = ['reportingOwnerCik', 'reporting-owner-cik', 'cik', 'CIK']
        name_tags = ['reportingOwnerName', 'reporting-owner-name', 'name', 'NAME']
        
        # Cerca CIK
        for tag_name in cik_tags:
            elem = reporting_owner.find(tag_name)
            if elem:
                insider_info['cik'] = elem.get_text(strip=True)
                break
        
        # Cerca nome
        for tag_name in name_tags:
            elem = reporting_owner.find(tag_name)
            if elem:
                insider_info['name'] = elem.get_text(strip=True)
                break
        
        # Cerca relationship info
        relationship_tags = ['reportingOwnerRelationship', 'relationship', 'RELATIONSHIP']
        for tag_name in relationship_tags:
            relationship = reporting_owner.find(tag_name)
            if relationship:
                relationship_info = {}
                
                # Estrai campi boolean
                bool_fields = ['isDirector', 'isOfficer', 'isTenPercentOwner', 'isOther']
                for field in bool_fields:
                    elem = relationship.find(field) or relationship.find(field.upper()) or relationship.find(field.lower())
                    if elem:
                        relationship_info[field] = elem.get_text(strip=True) == '1'
                
                # Title
                title_elem = relationship.find('officerTitle') or relationship.find('OFFICER-TITLE')
                if title_elem:
                    relationship_info['title'] = title_elem.get_text(strip=True)
                
                insider_info['relationship'] = relationship_info
                break
        
        return insider_info

    def _extract_issuer_info(self, issuer) -> Dict[str, Any]:
        """Estrai informazioni issuer con metodo più robusto"""
        issuer_info = {}
        
        # Possibili varianti dei tag
        cik_tags = ['issuerCik', 'issuer-cik', 'cik', 'CIK']
        name_tags = ['issuerName', 'issuer-name', 'name', 'NAME']
        ticker_tags = ['issuerTradingSymbol', 'issuer-trading-symbol', 'trading-symbol', 'ticker']
        
        # Cerca CIK
        for tag_name in cik_tags:
            elem = issuer.find(tag_name)
            if elem:
                issuer_info['cik'] = elem.get_text(strip=True)
                break
        
        # Cerca nome
        for tag_name in name_tags:
            elem = issuer.find(tag_name)
            if elem:
                issuer_info['name'] = elem.get_text(strip=True)
                break
        
        # Cerca ticker
        for tag_name in ticker_tags:
            elem = issuer.find(tag_name)
            if elem:
                issuer_info['ticker'] = elem.get_text(strip=True)
                break
        
        return issuer_info
    # 2. Correzione del metodo _extract_transactions 
    def _extract_transactions(self, soup) -> List[Dict[str, Any]]:
        """Estrai transazioni con metodo più robusto"""
        transactions = []
        
        # Cerca tabelle transazioni con diversi tag possibili
        table_tags = [
            'nonDerivativeTable', 'non-derivative-table', 'NON-DERIVATIVE-TABLE',
            'derivativeTable', 'derivative-table', 'DERIVATIVE-TABLE'
        ]
        
        transaction_tags = [
            'nonDerivativeTransaction', 'non-derivative-transaction', 'NON-DERIVATIVE-TRANSACTION',
            'derivativeTransaction', 'derivative-transaction', 'DERIVATIVE-TRANSACTION'
        ]
        
        for table_tag in table_tags:
            table = soup.find(table_tag)
            if table:
                logger.info(f"✅ Trovata tabella transazioni: {table_tag}")
                
                is_derivative = 'derivative' in table_tag.lower() and 'non' not in table_tag.lower()
                
                for trans_tag in transaction_tags:
                    trans_elements = table.find_all(trans_tag)
                    for trans_elem in trans_elements:
                        # CORREZIONE: cambiato da _parse_transaction_element_robust a _parse_transaction_element
                        trans_data = self._parse_transaction_element(trans_elem, is_derivative)
                        #trans_data = self._parse_transaction_element_robust(trans_elem, is_derivative)
                        if trans_data:
                            transactions.append(trans_data)
        
        return transactions

    def _extract_holdings(self, soup) -> List[Dict[str, Any]]:
        """Estrai holdings con metodo più robusto"""
        holdings = []
        
        # Implementazione simile a _extract_transactions ma per holdings
        # ...
        
        return holdings

    def _parse_transaction_element(self, transaction_elem, is_derivative: bool = False) -> Optional[Dict[str, Any]]:
        """
        Parsa una singola transazione dal XML Form 4.
        """
        try:
            trans_data = {
                'is_derivative': is_derivative,
                'security_title': '',
                'transaction_date': None,
                'transaction_code': '',
                'transaction_shares': 0,
                'transaction_price': 0,
                'shares_owned_after': 0,
                'direct_indirect': 'D'
            }
            
            # Titolo del security
            title_elem = transaction_elem.find('securityTitle')
            if title_elem:
                trans_data['security_title'] = title_elem.get_text(strip=True)
            
            # Data transazione
            date_elem = transaction_elem.find('transactionDate')
            if date_elem:
                try:
                    trans_data['transaction_date'] = datetime.strptime(date_elem.get_text(strip=True), '%Y-%m-%d').date()
                except ValueError:
                    trans_data['transaction_date'] = None
            
            # Codice transazione
            code_elem = transaction_elem.find('transactionCode')
            if code_elem:
                trans_data['transaction_code'] = code_elem.get_text(strip=True)
            
            # Numero di azioni/shares
            shares_elem = transaction_elem.find('transactionShares')
            if shares_elem:
                try:
                    trans_data['transaction_shares'] = float(shares_elem.get_text(strip=True))
                except (ValueError, TypeError):
                    trans_data['transaction_shares'] = 0
            
            # Prezzo per azione
            price_elem = transaction_elem.find('transactionPricePerShare')
            if price_elem:
                try:
                    trans_data['transaction_price'] = float(price_elem.get_text(strip=True))
                except (ValueError, TypeError):
                    trans_data['transaction_price'] = 0
            
            # Azioni possedute dopo la transazione
            owned_elem = transaction_elem.find('sharesOwnedFollowingTransaction')
            if owned_elem:
                try:
                    trans_data['shares_owned_after'] = float(owned_elem.get_text(strip=True))
                except (ValueError, TypeError):
                    trans_data['shares_owned_after'] = 0
            
            # Direct/Indirect ownership
            direct_elem = transaction_elem.find('directOrIndirectOwnership')
            if direct_elem:
                trans_data['direct_indirect'] = direct_elem.get_text(strip=True)
            
            return trans_data
            
        except Exception as e:
            logger.warning(f"⚠️ Errore nel parsing della transazione: {e}")
            return None
    # 3. OPZIONALE: Aggiungere il metodo _parse_transaction_element_robust se vuoi mantenerlo
    def _parse_transaction_element_robust(self, transaction_elem, is_derivative: bool = False) -> Optional[Dict[str, Any]]:
        """
        Versione più robusta del parsing delle transazioni con fallback multipli.
        """
        try:
            trans_data = {
                'is_derivative': is_derivative,
                'security_title': '',
                'transaction_date': None,
                'transaction_code': '',
                'transaction_shares': 0,
                'transaction_price': 0,
                'shares_owned_after': 0,
                'direct_indirect': 'D'
            }
            
            # Lista di possibili tag per ogni campo (varianti maiuscole/minuscole)
            title_tags = ['securityTitle', 'security-title', 'SECURITY-TITLE']
            date_tags = ['transactionDate', 'transaction-date', 'TRANSACTION-DATE']
            code_tags = ['transactionCode', 'transaction-code', 'TRANSACTION-CODE']
            shares_tags = ['transactionShares', 'transaction-shares', 'TRANSACTION-SHARES']
            price_tags = ['transactionPricePerShare', 'transaction-price-per-share', 'TRANSACTION-PRICE-PER-SHARE']
            owned_tags = ['sharesOwnedFollowingTransaction', 'shares-owned-following-transaction', 'SHARES-OWNED-FOLLOWING-TRANSACTION']
            direct_tags = ['directOrIndirectOwnership', 'direct-or-indirect-ownership', 'DIRECT-OR-INDIRECT-OWNERSHIP']
            
            # Titolo del security
            for tag in title_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    trans_data['security_title'] = elem.get_text(strip=True)
                    break
            
            # Data transazione
            for tag in date_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    try:
                        date_text = elem.get_text(strip=True)
                        trans_data['transaction_date'] = datetime.strptime(date_text, '%Y-%m-%d').date()
                        break
                    except ValueError:
                        continue
            
            # Codice transazione
            for tag in code_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    trans_data['transaction_code'] = elem.get_text(strip=True)
                    break
            
            # Numero di azioni/shares
            for tag in shares_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    try:
                        trans_data['transaction_shares'] = float(elem.get_text(strip=True))
                        break
                    except (ValueError, TypeError):
                        continue
            
            # Prezzo per azione
            for tag in price_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    try:
                        trans_data['transaction_price'] = float(elem.get_text(strip=True))
                        break
                    except (ValueError, TypeError):
                        continue
            
            # Azioni possedute dopo la transazione
            for tag in owned_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    try:
                        trans_data['shares_owned_after'] = float(elem.get_text(strip=True))
                        break
                    except (ValueError, TypeError):
                        continue
            
            # Direct/Indirect ownership
            for tag in direct_tags:
                elem = transaction_elem.find(tag)
                if elem:
                    trans_data['direct_indirect'] = elem.get_text(strip=True)
                    break
            
            return trans_data
            
        except Exception as e:
            logger.warning(f"⚠️ Errore nel parsing robusto della transazione: {e}")
            return None

    def _parse_holding_element(self, holding_elem, is_derivative: bool = False) -> Optional[Dict[str, Any]]:
                """
                Parsa un singolo holding dal XML Form 4.
                """
                try:
                    holding_data = {
                        'is_derivative': is_derivative,
                        'security_title': '',
                        'shares_owned': 0,
                        'direct_indirect': 'D'
                    }
                    
                    # Titolo del security
                    title_elem = holding_elem.find('securityTitle')
                    if title_elem:
                        holding_data['security_title'] = title_elem.get_text(strip=True)
                    
                    # Numero di azioni possedute
                    shares_elem = holding_elem.find('sharesOwned')
                    if shares_elem:
                        try:
                            holding_data['shares_owned'] = float(shares_elem.get_text(strip=True))
                        except (ValueError, TypeError):
                            holding_data['shares_owned'] = 0
                    
                    # Direct/Indirect ownership
                    direct_elem = holding_elem.find('directOrIndirectOwnership')
                    if direct_elem:
                        holding_data['direct_indirect'] = direct_elem.get_text(strip=True)
                    
                    return holding_data
                    
                except Exception as e:
                    logger.warning(f"⚠️ Errore nel parsing dell'holding: {e}")
                    return None
    def debug_insider_summary(self, parsed_data: Dict[str, Any]) -> None:
                """
                Crea un riassunto dei dati insider prima dell'inserimento.
                """
                logger.info(f"\n🔍 RIASSUNTO DATI INSIDER:")
                
                # Info insider
                insider_info = parsed_data.get('insider_info', {})
                logger.info(f"Insider: {insider_info.get('name', 'N/A')} (CIK: {insider_info.get('cik', 'N/A')})")
                
                # Info issuer
                issuer_info = parsed_data.get('issuer_info', {})
                logger.info(f"Company: {issuer_info.get('name', 'N/A')} (CIK: {issuer_info.get('cik', 'N/A')})")
                logger.info(f"Ticker: {issuer_info.get('ticker', 'N/A')}")
                
                # Transazioni
                transactions = parsed_data.get('transactions', [])
                logger.info(f"Transazioni: {len(transactions)}")
                
                if transactions:
                    # Raggruppa per codice transazione
                    trans_codes = {}
                    total_value = 0
                    
                    for trans in transactions:
                        code = trans.get('transaction_code', 'Unknown')
                        shares = trans.get('transaction_shares', 0)
                        price = trans.get('transaction_price', 0)
                        value = shares * price
                        
                        if code not in trans_codes:
                            trans_codes[code] = {'count': 0, 'total_shares': 0, 'total_value': 0}
                        
                        trans_codes[code]['count'] += 1
                        trans_codes[code]['total_shares'] += shares
                        trans_codes[code]['total_value'] += value
                        total_value += value
                    
                    logger.info(f"Valore totale transazioni: ${total_value:,.2f}")
                    
                    for code, data in trans_codes.items():
                        logger.info(f"  {code}: {data['count']} transazioni, {data['total_shares']:,.0f} azioni, ${data['total_value']:,.2f}")
                
                # Holdings
                holdings = parsed_data.get('holdings', [])
                logger.info(f"Holdings: {len(holdings)}")
                
                logger.info(f"🔍 Fine riassunto dati insider\n")

    def download_and_store_form4_data(self, cik: str, limit_filings: Optional[int] = None,
                                    start_date: Optional[date] = None, 
                                    end_date: Optional[date] = None) -> bool:
        """
        VERSIONE CORRETTA - Scarica e memorizza tutti i dati Form 4 per un CIK.
        """
        try:
            logger.info(f"🏢 Processamento CIK: {cik}")
            
            # 1. Recupera filing Form 4
            filings_df = self.get_form4_filings_by_cik(cik, start_date, end_date)
            if filings_df.empty:
                logger.warning(f"⚠️ Nessun filing Form 4 trovato per CIK {cik}")
                return False
            
            # 2. Limita il numero di filing se specificato
            if limit_filings:
                filings_df = filings_df.head(limit_filings)
                logger.info(f"📋 Processamento limitato a {limit_filings} filing più recenti")
            
            # 3. Processa ogni filing
            total_transactions = 0
            total_holdings = 0
            successful_filings = 0
            
            for idx, filing_row in filings_df.iterrows():
                try:
                    logger.info(f"\n📄 Processing filing {idx+1}/{len(filings_df)}")
                    logger.info(f"   Accession: {filing_row['accessionNumber']}")
                    logger.info(f"   Filing Date: {filing_row['filingDate']}")
                    
                    # Verifica se il filing esiste già
                    if self.portfolio_manager.insider_filing_exists(filing_row['accessionNumber']):
                        logger.info(f"⚠️ Filing {filing_row['accessionNumber']} già presente nel database, skip")
                        continue
                    
                    # FIX: Prova prima il documento diretto
                    document_url = filing_row['url_document']
                    
                    # Scarica il documento
                    document_response = self.session.get(document_url)
                    logger.info(f"🌐 Tentativo download: {document_url}")
                    logger.info(f"📥 Status Code: {document_response.status_code}")
                    
                    if document_response.status_code != 200:
                        logger.warning(f"⚠️ Errore nel download documento: {document_url}")
                        
                        # FALLBACK: Prova con l'index page (metodo originale)
                        index_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{filing_row['accession_nodash']}/{filing_row['accession_withdash']}-index.html"
                        logger.info(f"🔄 Fallback su index page: {index_url}")
                        
                        index_response = self.session.get(index_url)
                        if index_response.status_code != 200:
                            logger.warning(f"⚠️ Anche index page fallita, skip filing")
                            continue
                        
                        # Cerca link XML nell'index
                        soup = BeautifulSoup(index_response.text, 'html.parser')
                        xml_links = [a['href'] for a in soup.find_all('a', href=True) 
                                    if a['href'].endswith('.xml') or a['href'].endswith('.txt')]
                        
                        if not xml_links:
                            logger.warning("⚠️ Nessun file XML/TXT trovato nell'index")
                            continue
                        
                        # Usa il primo file trovato
                        document_url = f"https://www.sec.gov{xml_links[0]}"
                        document_response = self.session.get(document_url)
                    
                    # Prepara informazioni del filing
                    filed_date = filing_row['filingDate']
                    if isinstance(filed_date, str):
                        filed_date = datetime.strptime(filed_date, '%Y-%m-%d').date()
                    
                    filing_info = {
                        'accession_number': filing_row['accessionNumber'],
                        'filed_date': filed_date
                    }
                    
                    # FIX: Debugging del contenuto
                    content = document_response.content
                    logger.info(f"📄 Dimensione documento: {len(content)} bytes")
                    logger.info(f"🔍 Prime 200 caratteri del documento:")
                    logger.info(content[:200].decode('utf-8', errors='ignore'))
                    
                    # Analizza il contenuto - potrebbe essere SGML invece di XML
                    parsed_data = self.parse_form4_content(document_url, content, filing_info)
                    
                    if parsed_data and (parsed_data['transactions'] or parsed_data['holdings']):
                        # Mostra riassunto dati
                        self.debug_insider_summary(parsed_data)
                        
                        # Inserisci nel database
                        logger.info(f"📥 Inizio inserimento dati insider...")
                        
                        success = self.portfolio_manager.insert_insider_data(parsed_data)
                        
                        if success:
                            total_transactions += len(parsed_data['transactions'])
                            total_holdings += len(parsed_data['holdings'])
                            successful_filings += 1
                            logger.info(f"✅ Filing processato con successo!")
                    else:
                        logger.warning("⚠️ Nessuna transazione o holding trovata nel filing")
                    
                    # Pausa per evitare rate limiting
                    time.sleep(0.2)
                    
                except Exception as e:
                    logger.error(f"❌ Errore nel processing del filing {idx+1}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # Riassunto finale
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 RIASSUNTO COMPLETAMENTO INSIDER")
            logger.info(f"{'='*60}")
            logger.info(f"CIK: {cik}")
            logger.info(f"Filing processati con successo: {successful_filings}/{len(filings_df)}")
            logger.info(f"Totale transazioni inserite: {total_transactions}")
            logger.info(f"Totale holdings inseriti: {total_holdings}")
            logger.info(f"{'='*60}")
            
            return successful_filings > 0
            
        except Exception as e:
            logger.error(f"❌ Errore generale nel download insider per CIK {cik}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def download_multiple_insider_data(self, ciks: List[str], limit_filings: Optional[int] = None,
                                      days_back: int = 90) -> Dict[str, bool]:
        """
        Scarica dati Form 4 per multipli CIK.
        
        Args:
            ciks: Lista di CIK da processare
            limit_filings: Limite filing per ogni CIK
            days_back: Giorni indietro per filtrare i filing
            
        Returns:
            Dizionario con risultati per ogni CIK
        """
        results = {}
        
        # Calcola date range
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"🚀 AVVIO DOWNLOAD MULTIPLI INSIDER DATA")
        logger.info(f"CIK da processare: {len(ciks)}")
        logger.info(f"Periodo: {start_date} - {end_date}")
        logger.info(f"Limite filing per CIK: {limit_filings or 'Tutti'}")
        logger.info("="*70)
        
        for i, cik in enumerate(ciks):
            logger.info(f"\n🚀 PROCESSAMENTO CIK {i+1}/{len(ciks)}: {cik}")
            logger.info("="*70)
            
            success = self.download_and_store_form4_data(
                cik, 
                limit_filings=limit_filings,
                start_date=start_date,
                end_date=end_date
            )
            results[cik] = success
            
            if success:
                logger.info(f"✅ CIK {cik} completato con successo")
            else:
                logger.error(f"❌ Errore nel processamento del CIK {cik}")
            
            # Pausa tra CIK
            if i < len(ciks) - 1:
                time.sleep(1)
        
        return results


def main():
    
    # Configurazione database
    DB_CONFIG = {
        'host': 'xxxxxxxxx',
        'user': 'xxxx',
        'password': 'xxxxx',
        'database': 'xxxxx',
        'port': xxxx
    }
    
    #CIKS_TO_PROCESS = ['320193']  # Apple Inc.
    #CIKS_TO_PROCESS = ['1018724'] # Amazon.com Inc.
    #CIKS_TO_PROCESS = ['1318605'] # Tesla Inc.
    CIKS_TO_PROCESS = ['1652044'] # Alphabet Inc.   
    LIMIT_FILINGS = 1000   
    DAYS_BACK = 9000
    
    try:
        # Crea PortfolioManager esteso
        with ExtendedPortfolioManager(**DB_CONFIG) as portfolio_manager:
            
            # FIX: Inizializza tabelle PRIMA di tutto
            logger.info("🔧 Inizializzazione tabelle insider...")
            success = portfolio_manager.initialize_insider_tables()
            if not success:
                logger.error("❌ Impossibile inizializzare tabelle insider")
                return
            
            # Mostra statistiche iniziali
            stats = portfolio_manager.get_insider_statistics()
            logger.info(f"📊 Statistiche insider iniziali:")
            for key, value in stats.items():
                logger.info(f"   {key}: {value}")
            
            # Crea downloader
            downloader = SECInsiderDownloader(portfolio_manager)
            
            # Test con primo CIK
            test_cik = CIKS_TO_PROCESS[0]
            logger.info(f"🧪 TEST: Download insider per CIK {test_cik}...")
            
            # FIX: Aggiungi più debugging
            logger.info("🔍 DEBUG: Test recupero filing...")
            filings_df = downloader.get_form4_filings_by_cik(test_cik)
            logger.info(f"📋 Filing trovati: {len(filings_df)}")
            
            if not filings_df.empty:
                logger.info("📄 Primi 3 filing:")
                for idx, row in filings_df.head(3).iterrows():
                    logger.info(f"   {row['accessionNumber']} - {row['filingDate']}")
            
            # Processa i dati
            result = downloader.download_and_store_form4_data(
                test_cik, 
                limit_filings=LIMIT_FILINGS,
                start_date=date.today() - timedelta(days=DAYS_BACK),
                end_date=date.today()
            )
            
            if result:
                logger.info("✅ Processamento completato con successo!")
                
                # Mostra statistiche finali
                final_stats = portfolio_manager.get_insider_statistics()
                logger.info(f"📊 Statistiche finali:")
                for key, value in final_stats.items():
                    logger.info(f"   {key}: {value}")
            else:
                logger.error("❌ Processamento fallito")
            
    except Exception as e:
        logger.error(f"❌ Errore nell'esecuzione principale: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
