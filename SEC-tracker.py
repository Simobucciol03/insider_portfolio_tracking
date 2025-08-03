import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional
import time

# Importa il PortfolioManager dal file separato
from login_mysql import PortfolioManager

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SEC13FDownloader:
    """
    Classe per scaricare dati 13F-HR dalla SEC e integrarli nel database.
    Versione aggiornata con gestione migliorata delle posizioni multiple.
    """
    
    def __init__(self, portfolio_manager: PortfolioManager, user_agent: str = 'info.cashcrew@gmail.com'):
        """
        Inizializza il downloader.
        
        Args:
            portfolio_manager: Istanza del PortfolioManager
            user_agent: User agent per le richieste HTTP
        """
        self.portfolio_manager = portfolio_manager
        self.headers = {'User-Agent': user_agent}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def get_fund_info_by_cik(self, cik: str) -> Dict[str, str]:
        """
        Recupera informazioni base del fondo tramite CIK.
        
        Args:
            cik: CIK del fondo (senza zeri iniziali)
            
        Returns:
            Dizionario con informazioni del fondo
        """
        cik_padded = cik.zfill(10)
        
        try:
            response = self.session.get(
                f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
            )
            response.raise_for_status()
            
            data = response.json()
            
            return {
                'cik': cik,
                'name': data.get('name', f'Unknown Fund {cik}'),
                'sic': data.get('sic', ''),
                'sicDescription': data.get('sicDescription', ''),
                'ein': data.get('ein', ''),
                'addresses': data.get('addresses', {})
            }
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero info fondo per CIK {cik}: {e}")
            return {
                'cik': cik,
                'name': f'Unknown Fund {cik}',
                'sic': '',
                'sicDescription': '',
                'ein': '',
                'addresses': {}
            }
    
    def get_13f_filings(self, cik: str) -> pd.DataFrame:
        """
        Recupera tutti i filing 13F-HR per un CIK.
        
        Args:
            cik: CIK del fondo (senza zeri iniziali)
            
        Returns:
            DataFrame con i filing 13F-HR
        """
        cik_padded = cik.zfill(10)
        
        try:
            logger.info(f"🔍 Recupero filing 13F-HR per CIK: {cik}")
            
            response = self.session.get(
                f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
            )
            response.raise_for_status()
            
            recent = response.json()['filings']['recent']
            filings13F = pd.DataFrame.from_dict(recent)
            filings13F = filings13F[filings13F['form'] == '13F-HR'].reset_index(drop=True)
            
            # Crea colonne con accessionNumber con e senza trattini
            filings13F['accession_withdash'] = filings13F['accessionNumber']
            filings13F['accession_nodash'] = filings13F['accessionNumber'].str.replace('-', '')
            
            # Costruisci URL corretti all'index.html
            filings13F['url_index'] = [
                f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{nodash}/{withdash}-index.html"
                for nodash, withdash in zip(filings13F['accession_nodash'], filings13F['accession_withdash'])
            ]
            
            logger.info(f"✅ Trovati {len(filings13F)} filing 13F-HR")
            return filings13F
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero filing: {e}")
            return pd.DataFrame()
    
    def parse_13f_xml(self, xml_url: str, filing_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scarica e analizza un file XML 13F, preservando TUTTE le posizioni.
        Versione aggiornata con migliore gestione dei duplicati.
        
        Args:
            xml_url: URL del file XML
            filing_info: Informazioni sul filing
            
        Returns:
            Lista di TUTTE le posizioni estratte (incluse le multiple per stesso CUSIP)
        """
        try:
            logger.info(f"📄 Analisi XML: {xml_url}")
            
            response = self.session.get(xml_url)
            response.raise_for_status()
            
            soup_xml = BeautifulSoup(response.content, 'lxml')
            tables = soup_xml.find_all('infotable')
            
            positions = []
            
            for row in tables:
                try:
                    issuer = row.find('nameofissuer').text.strip()
                    title = row.find('titleofclass').text.strip()
                    cusip = row.find('cusip').text.strip()
                    value = int(row.find('value').text.strip())
                    shares = int(row.find('sshprnamt').text.strip())
                    
                    # Campi opzionali
                    put_call = row.find('putcall')
                    put_call_value = put_call.text.strip() if put_call else ''
                    
                    share_type = row.find('sshprnamttype')
                    share_type_value = share_type.text.strip() if share_type else 'SH'
                    
                    investment_discretion = row.find('investmentdiscretion')
                    investment_discretion_value = investment_discretion.text.strip() if investment_discretion else 'SOLE'
                    
                    voting_authority = row.find('votingauthority')
                    voting_authority_value = voting_authority.text.strip() if voting_authority else 'SOLE'
                    
                    position_data = {
                        'security': {
                            'cusip': cusip,
                            'name': f"{issuer} - {title}",
                            'issuer': issuer,
                            'title': title
                        },
                        'position': {
                            'value': value,
                            'shares': shares,
                            'share_type': share_type_value,
                            'investment_discretion': investment_discretion_value,
                            'voting_authority': voting_authority_value,
                            'put_call': put_call_value
                        },
                        'filing_info': filing_info
                    }
                    
                    positions.append(position_data)
                    
                except Exception as e:
                    logger.warning(f"⚠️ Errore nel parsing della posizione: {e}")
                    continue
            
            # Statistiche sui duplicati
            cusip_counts = {}
            for pos in positions:
                cusip = pos['security']['cusip']
                cusip_counts[cusip] = cusip_counts.get(cusip, 0) + 1
            
            multiple_positions = {cusip: count for cusip, count in cusip_counts.items() if count > 1}
            
            if multiple_positions:
                logger.info(f"📊 Posizioni multiple rilevate per {len(multiple_positions)} titoli:")
                for cusip, count in list(multiple_positions.items())[:5]:  # Mostra solo i primi 5
                    security_name = next(pos['security']['name'] for pos in positions if pos['security']['cusip'] == cusip)
                    logger.info(f"   {cusip} ({security_name}): {count} posizioni")
                if len(multiple_positions) > 5:
                    logger.info(f"   ... e altri {len(multiple_positions) - 5} titoli con posizioni multiple")
            
            logger.info(f"✅ Estratte {len(positions)} posizioni totali dal XML")
            return positions
            
        except Exception as e:
            logger.error(f"❌ Errore nel parsing XML {xml_url}: {e}")
            return []
    
    def debug_positions_summary(self, positions: List[Dict[str, Any]]) -> None:
        """
        Crea un riassunto delle posizioni prima dell'inserimento.
        """
        if not positions:
            return
            
        logger.info(f"\n🔍 RIASSUNTO POSIZIONI:")
        logger.info(f"Totale posizioni: {len(positions)}")
        
        # Raggruppa per CUSIP
        cusip_groups = {}
        total_value = 0
        
        for pos in positions:
            cusip = pos['security']['cusip']
            value = pos['position']['value']
            total_value += value
            
            if cusip not in cusip_groups:
                cusip_groups[cusip] = {
                    'name': pos['security']['name'],
                    'count': 0,
                    'total_value': 0,
                    'total_shares': 0
                }
            
            cusip_groups[cusip]['count'] += 1
            cusip_groups[cusip]['total_value'] += value
            cusip_groups[cusip]['total_shares'] += pos['position']['shares']
        
        logger.info(f"Titoli unici: {len(cusip_groups)}")
        logger.info(f"Valore totale portafoglio: ${total_value:,}")
        
        # Mostra top 5 posizioni per valore
        top_positions = sorted(cusip_groups.items(), 
                              key=lambda x: x[1]['total_value'], 
                              reverse=True)[:5]
        
        logger.info(f"\nTop 5 posizioni per valore:")
        for cusip, data in top_positions:
            logger.info(f"  {cusip}: ${data['total_value']:,} ({data['count']} posizioni)")
        
        logger.info(f"🔍 Fine riassunto posizioni\n")
    
    def download_and_store_13f_data(self, cik: str, limit_filings: Optional[int] = None, 
                                   use_aggregated: bool = False) -> bool:
        """
        Scarica e memorizza tutti i dati 13F per un CIK.
        Versione aggiornata con opzione per aggregazione.
        
        Args:
            cik: CIK del fondo
            limit_filings: Limite numero di filing da processare (None = tutti)
            use_aggregated: Se True, aggrega posizioni duplicate, se False le mantiene separate
            
        Returns:
            True se successo, False altrimenti
        """
        try:
            # 1. Recupera informazioni del fondo
            fund_info = self.get_fund_info_by_cik(cik)
            logger.info(f"🏢 Fondo: {fund_info['name']} (CIK: {cik})")
            
            # 2. Recupera filing 13F-HR
            filings_df = self.get_13f_filings(cik)
            if filings_df.empty:
                logger.warning(f"⚠️ Nessun filing 13F-HR trovato per CIK {cik}")
                return False
            
            # 3. Limita il numero di filing se specificato
            if limit_filings:
                filings_df = filings_df.head(limit_filings)
                logger.info(f"📋 Processamento limitato a {limit_filings} filing più recenti")
            
            # 4. Processa ogni filing
            total_positions = 0
            successful_filings = 0
            
            for idx, filing_row in filings_df.iterrows():
                try:
                    logger.info(f"\n📄 Processing filing {idx+1}/{len(filings_df)}")
                    logger.info(f"   Accession: {filing_row['accessionNumber']}")
                    logger.info(f"   Report Date: {filing_row['reportDate']}")
                    
                    # Verifica se il filing esiste già (opzionale)
                    try:
                        if hasattr(self.portfolio_manager, 'filing_exists') and self.portfolio_manager.filing_exists(filing_row['accessionNumber']):
                            logger.info(f"⚠️ Filing {filing_row['accessionNumber']} già presente nel database, skip")
                            continue
                    except Exception as e:
                        logger.warning(f"⚠️ Errore nel controllo esistenza filing: {e}")
                        # Continua comunque il processamento
                    
                    # Scarica index page
                    index_response = self.session.get(filing_row['url_index'])
                    if index_response.status_code != 200:
                        logger.warning(f"⚠️ Errore nel download index page: {filing_row['url_index']}")
                        continue
                    
                    # Cerca link XML
                    soup = BeautifulSoup(index_response.text, 'html.parser')
                    xml_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.xml')]
                    
                    if not xml_links:
                        logger.warning("⚠️ Nessun file .xml trovato")
                        continue
                    
                    xml_url = f"https://www.sec.gov{xml_links[-1]}"
                    
                    # Converte date
                    report_date = datetime.strptime(filing_row['reportDate'], '%Y-%m-%d').date()
                    filed_date = datetime.strptime(filing_row['filingDate'], '%Y-%m-%d').date()
                    
                    # Informazioni del filing
                    filing_info = {
                        'accession_number': filing_row['accessionNumber'],
                        'report_date': report_date,
                        'filed_date': filed_date
                    }
                    
                    # Analizza XML e estrai TUTTE le posizioni
                    positions = self.parse_13f_xml(xml_url, filing_info)
                    
                    if positions:
                        # Mostra riassunto posizioni
                        self.debug_positions_summary(positions)
                        
                        # Crea struttura dati per PortfolioManager
                        portfolio_data = {
                            'fund': {
                                'cik': cik,
                                'name': fund_info['name']
                            },
                            'filing': filing_info,
                            'positions': positions
                        }
                        
                        # Sceglie il metodo di inserimento
                        logger.info(f"📥 Inizio inserimento {len(positions)} posizioni...")
                        
                        if use_aggregated:
                            logger.info("📊 Utilizzo modalità aggregata (somma posizioni duplicate)")
                            success = self.portfolio_manager.insert_complete_portfolio_data_aggregated(portfolio_data)
                        else:
                            logger.info("📊 Utilizzo modalità separata (mantieni tutte le posizioni)")
                            success = self.portfolio_manager.insert_complete_portfolio_data(portfolio_data)
                        
                        if success:
                            total_positions += len(positions)
                            successful_filings += 1
                            logger.info(f"✅ Filing processato con successo!")
                            logger.info(f"   Posizioni elaborate: {len(positions)}")
                            logger.info(f"   Accession Number: {filing_row['accessionNumber']}")
                            
                            # Verifica che le posizioni siano state effettivamente inserite
                            try:
                                stats = self.portfolio_manager.get_filing_statistics()
                                logger.info(f"   Posizioni totali nel DB: {stats.get('total_positions', 0)}")
                            except Exception as e:
                                logger.warning(f"⚠️ Errore nel recupero statistiche: {e}")
                                
                        else:
                            logger.error(f"❌ Errore nell'inserimento del filing nel database")
                            logger.error(f"   Accession Number: {filing_row['accessionNumber']}")
                            logger.error(f"   Posizioni perse: {len(positions)}")
                            
                            # Debug in caso di errore
                            self.portfolio_manager.debug_last_insertion()
                    else:
                        logger.warning("⚠️ Nessuna posizione trovata nel filing")
                    
                    # Pausa per evitare rate limiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"❌ Errore nel processing del filing {idx+1}: {e}")
                    continue
            
            # 5. Riassunto finale
            logger.info(f"\n{'='*60}")
            logger.info(f"📊 RIASSUNTO COMPLETAMENTO")
            logger.info(f"{'='*60}")
            logger.info(f"Fondo: {fund_info['name']} (CIK: {cik})")
            logger.info(f"Filing processati con successo: {successful_filings}/{len(filings_df)}")
            logger.info(f"Totale posizioni inserite: {total_positions}")
            logger.info(f"Modalità inserimento: {'Aggregata' if use_aggregated else 'Separata'}")
            logger.info(f"{'='*60}")
            
            return successful_filings > 0
            
        except Exception as e:
            logger.error(f"❌ Errore generale nel download per CIK {cik}: {e}")
            return False
    
    def download_multiple_funds(self, ciks: List[str], limit_filings: Optional[int] = None, 
                               use_aggregated: bool = False) -> Dict[str, bool]:
        """
        Scarica dati per multipli fondi.
        
        Args:
            ciks: Lista di CIK da processare
            limit_filings: Limite filing per ogni fondo
            use_aggregated: Se True, aggrega posizioni duplicate
            
        Returns:
            Dizionario con risultati per ogni CIK
        """
        results = {}
        
        logger.info(f"🚀 AVVIO DOWNLOAD MULTIPLI FONDI")
        logger.info(f"Fondi da processare: {len(ciks)}")
        logger.info(f"Limite filing per fondo: {limit_filings or 'Tutti'}")
        logger.info(f"Modalità aggregazione: {'Attiva' if use_aggregated else 'Disattiva'}")
        logger.info("="*70)
        
        for i, cik in enumerate(ciks):
            logger.info(f"\n🚀 PROCESSAMENTO FONDO {i+1}/{len(ciks)}: CIK {cik}")
            logger.info("="*70)
            
            success = self.download_and_store_13f_data(cik, limit_filings, use_aggregated)
            results[cik] = success
            
            if success:
                logger.info(f"✅ Fondo {cik} completato con successo")
            else:
                logger.error(f"❌ Errore nel processamento del fondo {cik}")
            
            # Pausa tra fondi
            if i < len(ciks) - 1:
                time.sleep(1)
        
        return results

def main():
    """Funzione principale per test ed esempi."""
    
    # Configurazione database
    DB_CONFIG = {
        'host': 'xxxxxx',
        'user': 'xxxxxx',
        'password': 'xxxxxx',
        'database': 'xxxxxx',
        'port': xxxx
    }
    
    # Lista di CIK da processare
    CIKS_TO_PROCESS = [
        #'1067983',  # Berkshire Hathaway
        #'1045810',  # NVIDIA CORP
        #'2012383', # blackrock
        '1364742' # BlackRock Inc. pre 2024
        # Aggiungi altri CIK qui
    ]
    
    # Configurazione
    LIMIT_FILINGS = 100  # Limita per test
    USE_AGGREGATED = False  # Cambia a True per aggregare posizioni duplicate
    
    try:
        # Crea PortfolioManager
        with PortfolioManager(**DB_CONFIG) as portfolio_manager:
            
            # TEST: Verifica connessione database
            logger.info("🔧 Test connessione database...")
            if not portfolio_manager.test_database_connection():
                logger.error("❌ Impossibile connettersi al database")
                return
            
            # Inizializza database se necessario
            logger.info("🔧 Inizializzazione database...")
            portfolio_manager.initialize_database()
            
            # Mostra statistiche iniziali
            stats = portfolio_manager.get_filing_statistics()
            logger.info(f"📊 Statistiche database iniziali:")
            logger.info(f"   Fondi: {stats.get('total_funds', 0)}")
            logger.info(f"   Filing: {stats.get('total_filings', 0)}")
            logger.info(f"   Posizioni: {stats.get('total_positions', 0)}")
            logger.info(f"   Titoli: {stats.get('total_securities', 0)}")
            
            # Crea downloader
            downloader = SEC13FDownloader(portfolio_manager)
            
            # TEST: Prova prima con UN solo CIK per debug
            test_cik = CIKS_TO_PROCESS[0]
            logger.info(f"🧪 TEST: Prova download per CIK {test_cik}...")
            
            test_result = downloader.download_and_store_13f_data(
                test_cik, 
                limit_filings=200,  # Solo 1 filing per test
                use_aggregated=USE_AGGREGATED
            )
            
            if test_result:
                logger.info("✅ Test riuscito! Procedo con tutti i fondi...")
                
                # Debug dell'inserzione di test
                portfolio_manager.debug_last_insertion()
                
                # Scarica dati per tutti i fondi
                logger.info("🚀 Avvio download completo dati SEC 13F...")
                results = downloader.download_multiple_funds(
                    CIKS_TO_PROCESS, 
                    limit_filings=LIMIT_FILINGS,
                    use_aggregated=USE_AGGREGATED
                )
                
                # Mostra risultati finali
                logger.info("\n📊 RISULTATI FINALI:")
                logger.info("="*50)
                successful_downloads = 0
                for cik, success in results.items():
                    status = "✅ SUCCESSO" if success else "❌ FALLIMENTO"
                    logger.info(f"CIK {cik}: {status}")
                    if success:
                        successful_downloads += 1
                
                logger.info(f"\nRiepilogo: {successful_downloads}/{len(results)} fondi processati con successo")
                
                # Statistiche finali
                final_stats = portfolio_manager.get_filing_statistics()
                logger.info(f"\n📊 Statistiche finali:")
                logger.info(f"   Fondi: {final_stats.get('total_funds', 0)}")
                logger.info(f"   Filing: {final_stats.get('total_filings', 0)}")
                logger.info(f"   Posizioni: {final_stats.get('total_positions', 0)}")
                logger.info(f"   Titoli: {final_stats.get('total_securities', 0)}")
                
                # Debug finale
                portfolio_manager.debug_last_insertion()
                
            else:
                logger.error("❌ Test fallito! Controllare configurazione e connessione")
                portfolio_manager.debug_last_insertion()
            
    except Exception as e:
        logger.error(f"❌ Errore nell'esecuzione principale: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
