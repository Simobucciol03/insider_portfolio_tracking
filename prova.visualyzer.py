import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import yfinance as yf
import pandas as pd
from datetime import datetime, date, timedelta
import logging
import mysql.connector
from typing import List, Dict, Any, Optional
import numpy as np
import os
import subprocess
import platform


# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InsiderSalesVisualizer:
    """
    Classe per visualizzare vendite e acquisti insider sulla serie storica dei prezzi.
    VERSIONE COMPLETA CON SALES (ROSSO) E PURCHASES (VERDE)
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.connection = None
        self._setup_matplotlib()
        
    def _setup_matplotlib(self):
        """Configura matplotlib per garantire la visualizzazione."""
        import matplotlib
        
        # Prova diversi backend fino a trovarne uno funzionante
        backends_to_try = ['TkAgg', 'Qt5Agg', 'Qt4Agg', 'GTKAgg', 'Agg']
        
        for backend in backends_to_try:
            try:
                matplotlib.use(backend, force=True)
                logger.info(f"🎨 Backend matplotlib impostato: {backend}")
                break
            except:
                continue
        else:
            logger.warning("⚠️ Usando backend matplotlib di default")
            
        # Configura modalità interattiva
        plt.ion()  # Abilita modalità interattiva
        
    def connect_database(self) -> bool:
        """Stabilisce connessione al database."""
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            logger.info("✅ Connessione al database stabilita")
            return True
        except Exception as e:
            logger.error(f"❌ Errore connessione database: {e}")
            return False
    
    def close_connection(self):
        """Chiude la connessione al database."""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("🔌 Connessione database chiusa")
    
    def get_insider_sales_from_db(self, company_ticker: str, 
                                 start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """Estrae le vendite insider dal database."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database")
            return pd.DataFrame()
        
        try:
            query = """
                SELECT 
                    it.transaction_date,
                    it.transaction_shares,
                    it.transaction_price,
                    i.name as insider_name,
                    i.title as insider_title,
                    c.ticker as company_ticker,
                    (it.transaction_shares * it.transaction_price) as transaction_value
                FROM insider_transactions it
                JOIN insider_filings insider_f ON it.filing_id = insider_f.id
                JOIN insiders i ON insider_f.insider_id = i.id
                JOIN companies c ON insider_f.company_id = c.id
                WHERE it.transaction_code = 'S'
                  AND it.transaction_shares IS NOT NULL 
                  AND it.transaction_price IS NOT NULL
                  AND it.transaction_shares > 0
                  AND it.transaction_price > 0
            """
            
            params = []
            
            if company_ticker:
                query += " AND c.ticker = %s"
                params.append(company_ticker)
            
            if start_date:
                query += " AND it.transaction_date >= %s"
                params.append(start_date)
                
            if end_date:
                query += " AND it.transaction_date <= %s"
                params.append(end_date)
            
            query += " ORDER BY it.transaction_date DESC"
            
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            if not results:
                logger.warning("⚠️ Nessuna vendita insider trovata")
                cursor.close()
                return pd.DataFrame()
            
            df = pd.DataFrame(results, columns=columns)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            df = df.dropna(subset=['transaction_shares', 'transaction_price'])
            df = df[(df['transaction_shares'] > 0) & (df['transaction_price'] > 0)]
            
            logger.info(f"✅ Estratte {len(df)} vendite insider dal database")
            cursor.close()
            return df
            
        except Exception as e:
            logger.error(f"❌ Errore nell'estrazione vendite insider: {e}")
            return pd.DataFrame()
    
    def get_insider_purchases_from_db(self, company_ticker: str, 
                                     start_date: date = None, end_date: date = None) -> pd.DataFrame:
        """Estrae gli acquisti insider dal database."""
        if not self.connection:
            logger.error("❌ Nessuna connessione al database")
            return pd.DataFrame()
        
        try:
            query = """
                SELECT 
                    it.transaction_date,
                    it.transaction_shares,
                    it.transaction_price,
                    i.name as insider_name,
                    i.title as insider_title,
                    c.ticker as company_ticker,
                    (it.transaction_shares * it.transaction_price) as transaction_value
                FROM insider_transactions it
                JOIN insider_filings insider_f ON it.filing_id = insider_f.id
                JOIN insiders i ON insider_f.insider_id = i.id
                JOIN companies c ON insider_f.company_id = c.id
                WHERE it.transaction_code = 'P'
                  AND it.transaction_shares IS NOT NULL 
                  AND it.transaction_price IS NOT NULL
                  AND it.transaction_shares > 0
                  AND it.transaction_price > 0
            """
            
            params = []
            
            if company_ticker:
                query += " AND c.ticker = %s"
                params.append(company_ticker)
            
            if start_date:
                query += " AND it.transaction_date >= %s"
                params.append(start_date)
                
            if end_date:
                query += " AND it.transaction_date <= %s"
                params.append(end_date)
            
            query += " ORDER BY it.transaction_date DESC"
            
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(query, params)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            if not results:
                logger.warning("⚠️ Nessun acquisto insider trovato")
                cursor.close()
                return pd.DataFrame()
            
            df = pd.DataFrame(results, columns=columns)
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            df = df.dropna(subset=['transaction_shares', 'transaction_price'])
            df = df[(df['transaction_shares'] > 0) & (df['transaction_price'] > 0)]
            
            logger.info(f"✅ Estratti {len(df)} acquisti insider dal database")
            cursor.close()
            return df
            
        except Exception as e:
            logger.error(f"❌ Errore nell'estrazione acquisti insider: {e}")
            return pd.DataFrame()
    
    def get_stock_price_data(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Recupera i dati storici del prezzo del titolo DIRETTAMENTE DAL DATABASE.
        Usa la tabella sidan.{TICKER} invece di Yahoo Finance.
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database")
            return pd.DataFrame()
            
        try:
            logger.info(f"📈 Recupero dati storici per {ticker} dal DATABASE...")
            
            # Nome tabella dinamico basato sul ticker
            table_name = f"sidan.{ticker}"
            
            # Query per recuperare i dati storici
            query = f"""
                SELECT 
                    Date,
                    Open,
                    High,
                    Low,
                    Close,
                    Volume
                FROM {table_name}
                WHERE Date >= %s AND Date <= %s
                ORDER BY Date ASC
            """
            
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            if not results:
                logger.warning(f"⚠️ Nessun dato storico trovato in {table_name}")
                cursor.close()
                # FALLBACK: Prova Yahoo Finance se la tabella è vuota
                logger.info("🔄 Fallback a Yahoo Finance...")
                return self._get_stock_price_data_fallback(ticker, start_date, end_date)
            
            # Crea DataFrame
            df = pd.DataFrame(results, columns=columns)
            
            # Converte la data se necessario
            if not pd.api.types.is_datetime64_any_dtype(df['Date']):
                df['Date'] = pd.to_datetime(df['Date'])
            
            # Assicurati che i prezzi siano numerici
            price_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Rimuovi righe con valori NaN nei prezzi essenziali
            df = df.dropna(subset=['Close'])
            
            logger.info(f"✅ Recuperati {len(df)} giorni di dati per {ticker} dal database")
            logger.info(f"   Periodo: {df['Date'].min().date()} - {df['Date'].max().date()}")
            
            cursor.close()
            return df
            
        except Exception as e:
            logger.error(f"❌ Errore nel recupero dati storici dal database per {ticker}: {e}")
            logger.info("🔄 Tentativo fallback a Yahoo Finance...")
            return self._get_stock_price_data_fallback(ticker, start_date, end_date)
    
    def _get_stock_price_data_fallback(self, ticker: str, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Fallback a Yahoo Finance se i dati non sono disponibili nel database.
        """
        try:
            logger.info(f"📈 FALLBACK: Recupero dati da Yahoo Finance per {ticker}...")
            
            stock = yf.Ticker(ticker)
            extended_start = start_date - timedelta(days=5)
            hist_data = stock.history(start=extended_start, end=end_date + timedelta(days=1), interval='1d')
            
            if hist_data.empty:
                logger.error(f"❌ Nessun dato storico trovato per {ticker} anche su Yahoo Finance")
                return pd.DataFrame()
            
            hist_data.reset_index(inplace=True)
            hist_data = hist_data[hist_data['Date'] >= pd.to_datetime(start_date)]
            
            logger.info(f"✅ FALLBACK: Recuperati {len(hist_data)} giorni da Yahoo Finance")
            return hist_data
                
        except Exception as e:
            logger.error(f"❌ Errore anche nel fallback Yahoo Finance per {ticker}: {e}")
            return pd.DataFrame()
    
    def create_insider_sales_chart_FIXED(self, ticker: str, days_back: int = 1825) -> None:
        """
        VERSIONE COMPLETA che mostra SALES (rosso) e PURCHASES (verde) individuali.
        """
        try:
            # Calcola periodo
            end_date = date.today()
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"📊 Creazione grafico COMPLETO per {ticker}")
            logger.info(f"   Periodo: {start_date} - {end_date}")
            
            # 1. Recupera dati
            sales_df = self.get_insider_sales_from_db(ticker, start_date, end_date)
            purchases_df = self.get_insider_purchases_from_db(ticker, start_date, end_date)
            price_df = self.get_stock_price_data(ticker, start_date, end_date)
            
            # VERIFICA PRELIMINARE: Controlla se ci sono dati sufficienti
            if price_df.empty and sales_df.empty and purchases_df.empty:
                logger.error(f"❌ NESSUN DATO TROVATO per {ticker}")
                print(f"\n{'='*60}")
                print(f"❌ ERRORE: Nessun dato disponibile per {ticker}")
                print(f"   - Nessun dato di prezzo trovato")
                print(f"   - Nessuna vendita insider trovata")
                print(f"   - Nessun acquisto insider trovato")
                print(f"   - Verifica che il ticker esista nel database")
                print(f"{'='*60}")
                return
            
            if price_df.empty:
                logger.error("❌ Impossibile creare grafico: dati prezzi mancanti")
                print(f"\n❌ ERRORE: Dati prezzi mancanti per {ticker}")
                print(f"   Verifica che esista la tabella sidan.{ticker}")
                return
            
            # 2. FORZA chiusura di eventuali figure esistenti
            plt.close('all')
            
            # 3. Crea il grafico con configurazione specifica per la visualizzazione
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), 
                                          gridspec_kw={'height_ratios': [3, 1]})
            
            # 4. Plotta prezzo
            ax1.plot(price_df['Date'], price_df['Close'], 
                    color='blue', linewidth=1.5, label=f'{ticker} Prezzo Chiusura')
            
            # 5. VENDITE INSIDER (PUNTI ROSSI) - OGNI singola vendita
            sale_points_added = 0
            if not sales_df.empty:
                logger.info(f"🔴 Processamento di {len(sales_df)} vendite individuali...")
                
                for idx, sale in sales_df.iterrows():
                    sale_date = sale['transaction_date']
                    
                    # Trova il prezzo più vicino alla data della vendita
                    price_mask = price_df['Date'] <= sale_date
                    if price_mask.any():
                        closest_price = price_df[price_mask].iloc[-1]['Close']
                        
                        # Calcola dimensione del punto basata sul valore della transazione
                        transaction_value = float(sale['transaction_value'])
                        point_size = min(200, max(30, transaction_value / 1000000 * 50))
                        
                        # Converti a valori scalari
                        plot_date = pd.to_datetime(sale_date)
                        plot_price = float(closest_price)
                        
                        # Plotta il punto per questa specifica vendita
                        ax1.scatter(plot_date, plot_price, 
                                  s=float(point_size), color='red', alpha=0.7, 
                                  edgecolors='darkred', linewidth=1, zorder=5)
                        
                        sale_points_added += 1
                
                logger.info(f"✅ Aggiunti {sale_points_added} punti vendita sul grafico")
            
            # 6. ACQUISTI INSIDER (PUNTI VERDI) - OGNI singolo acquisto
            purchase_points_added = 0
            if not purchases_df.empty:
                logger.info(f"🟢 Processamento di {len(purchases_df)} acquisti individuali...")
                
                for idx, purchase in purchases_df.iterrows():
                    purchase_date = purchase['transaction_date']
                    
                    # Trova il prezzo più vicino alla data dell'acquisto
                    price_mask = price_df['Date'] <= purchase_date
                    if price_mask.any():
                        closest_price = price_df[price_mask].iloc[-1]['Close']
                        
                        # Calcola dimensione del punto basata sul valore della transazione
                        transaction_value = float(purchase['transaction_value'])
                        point_size = min(200, max(30, transaction_value / 1000000 * 50))
                        
                        # Converti a valori scalari
                        plot_date = pd.to_datetime(purchase_date)
                        plot_price = float(closest_price)
                        
                        # Plotta il punto per questo specifico acquisto
                        ax1.scatter(plot_date, plot_price, 
                                  s=float(point_size), color='green', alpha=0.7, 
                                  edgecolors='darkgreen', linewidth=1, zorder=5)
                        
                        purchase_points_added += 1
                
                logger.info(f"✅ Aggiunti {purchase_points_added} punti acquisto sul grafico")
            
            # 7. Crea il titolo con statistiche complete
            total_transactions = sale_points_added + purchase_points_added
            title_parts = [f'{ticker} - Prezzo e Transazioni Insider']
            
            if total_transactions > 0:
                stats_parts = []
                
                if sale_points_added > 0:
                    sales_value = sales_df['transaction_value'].sum()
                    stats_parts.append(f"{sale_points_added} vendite (${sales_value/1000000:.1f}M)")
                
                if purchase_points_added > 0:
                    purchases_value = purchases_df['transaction_value'].sum()
                    stats_parts.append(f"{purchase_points_added} acquisti (${purchases_value/1000000:.1f}M)")
                
                title_parts.append(' | '.join(stats_parts))
            else:
                title_parts.append('Nessuna transazione insider nel periodo')
            
            ax1.set_title('\n'.join(title_parts), fontsize=14, weight='bold')
            
            # 8. Formattazione asse principale
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
            
            ax1.set_ylabel('Prezzo ($)', fontsize=12)
            
            # Legenda con colori
            legend_elements = [plt.Line2D([0], [0], color='blue', lw=2, label=f'{ticker} Prezzo')]
            if sale_points_added > 0:
                legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', 
                                                markerfacecolor='red', markersize=8, 
                                                label=f'Vendite Insider ({sale_points_added})'))
            if purchase_points_added > 0:
                legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', 
                                                markerfacecolor='green', markersize=8, 
                                                label=f'Acquisti Insider ({purchase_points_added})'))
            
            ax1.legend(handles=legend_elements, loc='upper left')
            ax1.grid(True, alpha=0.3)
            
            # 9. Secondo grafico: volume transazioni GIORNALIERO (vendite e acquisti combinati)
            if not sales_df.empty or not purchases_df.empty:
                # Combina vendite e acquisti per il grafico giornaliero
                all_transactions = []
                
                if not sales_df.empty:
                    sales_daily = sales_df.copy()
                    sales_daily['transaction_type'] = 'sale'
                    all_transactions.append(sales_daily)
                
                if not purchases_df.empty:
                    purchases_daily = purchases_df.copy()
                    purchases_daily['transaction_type'] = 'purchase'
                    # Per gli acquisti, rendiamo negativo il valore per distinguerli visivamente
                    purchases_daily['transaction_value'] = -purchases_daily['transaction_value']
                    all_transactions.append(purchases_daily)
                
                if all_transactions:
                    combined_df = pd.concat(all_transactions, ignore_index=True)
                    
                    # Aggregazione giornaliera
                    daily_transactions = combined_df.groupby(
                        [combined_df['transaction_date'].dt.date, 'transaction_type']
                    ).agg({
                        'transaction_value': 'sum',
                        'insider_name': 'count'
                    }).reset_index()
                    
                    # Plotta vendite (rosso, positivo)
                    sales_daily_agg = daily_transactions[daily_transactions['transaction_type'] == 'sale']
                    if not sales_daily_agg.empty:
                        ax2.bar(pd.to_datetime(sales_daily_agg['transaction_date']), 
                               sales_daily_agg['transaction_value'] / 1000000,
                               color='red', alpha=0.6, width=1, label='Vendite')
                    
                    # Plotta acquisti (verde, negativo per distinguerli visivamente)
                    purchases_daily_agg = daily_transactions[daily_transactions['transaction_type'] == 'purchase']
                    if not purchases_daily_agg.empty:
                        ax2.bar(pd.to_datetime(purchases_daily_agg['transaction_date']), 
                               purchases_daily_agg['transaction_value'] / 1000000,  # Già negativo
                               color='green', alpha=0.6, width=1, label='Acquisti')
                    
                    ax2.set_ylabel('Valore Transazioni\nGiornaliere (M$)', fontsize=10)
                    ax2.set_title(f'Volume Transazioni Giornaliero', fontsize=10)
                    ax2.legend()
                    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)  # Linea zero
            else:
                ax2.text(0.5, 0.5, 'Nessuna transazione nel periodo', 
                        transform=ax2.transAxes, ha='center', va='center')
            
            ax2.set_xlabel('Data', fontsize=12)
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            # 10. SALVATAGGIO SEMPRE PRIMA DELLA VISUALIZZAZIONE
            filename = f'{ticker}_insider_transactions_COMPLETE_{start_date}_{end_date}.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='white')
            logger.info(f"💾 Grafico salvato come: {filename}")
            
            # 11. MULTIPLE STRATEGIE PER LA VISUALIZZAZIONE
            logger.info("🖼️ Tentativo visualizzazione grafico...")
            
            success = False
            
            # STRATEGIA 1: plt.show() standard
            try:
                plt.show(block=False)
                plt.pause(1)
                logger.info("✅ Strategia 1: plt.show() riuscita")
                success = True
            except Exception as e:
                logger.warning(f"⚠️ Strategia 1 fallita: {e}")
            
            # STRATEGIA 2: Forzare la figura in primo piano
            if not success:
                try:
                    fig.show()
                    plt.draw()
                    plt.pause(2)
                    logger.info("✅ Strategia 2: fig.show() riuscita")
                    success = True
                except Exception as e:
                    logger.warning(f"⚠️ Strategia 2 fallita: {e}")
            
            # STRATEGIA 3: Backend specifico
            if not success:
                try:
                    mngr = fig.canvas.manager
                    mngr.show()
                    plt.pause(2)
                    logger.info("✅ Strategia 3: canvas manager riuscita")
                    success = True
                except Exception as e:
                    logger.warning(f"⚠️ Strategia 3 fallita: {e}")
            
            # STRATEGIA 4: Aprire file automaticamente
            if not success:
                try:
                    self._open_file_automatically(filename)
                    logger.info("✅ Strategia 4: apertura file automatica riuscita")
                    success = True
                except Exception as e:
                    logger.warning(f"⚠️ Strategia 4 fallita: {e}")
            
            # FALLBACK: Istruzioni manuali
            if not success:
                logger.error("❌ Tutte le strategie di visualizzazione sono fallite")
                logger.info(f"📁 APRI MANUALMENTE IL FILE: {os.path.abspath(filename)}")
            else:
                logger.info("🎉 Grafico visualizzato con successo!")
            
            # 12. Mantieni il grafico aperto e interattivo
            try:
                print(f"\n{'='*60}")
                print(f"📊 GRAFICO CREATO PER {ticker}")
                print(f"🔴 Vendite insider mostrate: {sale_points_added}")
                print(f"🟢 Acquisti insider mostrati: {purchase_points_added}")
                print(f"📁 File salvato: {filename}")
                print(f"{'='*60}")
                
                if success and plt.get_fignums():
                    input("\n🔍 Premi INVIO per continuare (il grafico rimarrà aperto)...")
                else:
                    input(f"\n📁 Apri manualmente: {os.path.abspath(filename)}\nPremi INVIO per continuare...")
            except Exception as e:
                logger.error(f"❌ Errore durante l'attesa input: {e}")
                print("Premi INVIO per chiudere il programma...")
                input()
                
        except Exception as e:
            logger.error(f"❌ Errore nella creazione del grafico: {e}")
            import traceback
            traceback.print_exc()
    
    def check_ticker_exists(self, ticker: str) -> Dict[str, Any]:
        """
        Verifica se un ticker esiste nel database e restituisce informazioni sui dati disponibili.
        Restituisce un dizionario con le informazioni di disponibilità.
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database")
            return {'exists': False, 'error': 'No database connection'}
        
        try:
            cursor = self.connection.cursor(buffered=True)
            result = {
                'exists': False,
                'company_exists': False,
                'price_data_exists': False,
                'insider_data_exists': False,
                'insider_sales_count': 0,
                'insider_purchases_count': 0,
                'price_records': 0,
                'date_range_insider': None,
                'date_range_prices': None,
                'error': None
            }
            
            # 1. Verifica se la company esiste nella tabella companies
            company_query = "SELECT id, name FROM companies WHERE ticker = %s"
            cursor.execute(company_query, (ticker,))
            company_result = cursor.fetchone()
            
            if company_result:
                result['company_exists'] = True
                result['company_id'] = company_result[0]
                result['company_name'] = company_result[1]
                logger.info(f"✅ Company trovata: {company_result[1]} ({ticker})")
            else:
                logger.warning(f"⚠️ Company {ticker} non trovata nella tabella companies")
                cursor.close()
                return result
            
            # 2. Verifica dati insider sales
            sales_query = """
                SELECT COUNT(*) as count, MIN(it.transaction_date) as min_date, MAX(it.transaction_date) as max_date
                FROM insider_transactions it
                JOIN insider_filings if_t ON it.filing_id = if_t.id
                JOIN companies c ON if_t.company_id = c.id
                WHERE c.ticker = %s AND it.transaction_code = 'S'
                  AND it.transaction_shares IS NOT NULL 
                  AND it.transaction_price IS NOT NULL
                  AND it.transaction_shares > 0
                  AND it.transaction_price > 0
            """
            
            cursor.execute(sales_query, (ticker,))
            sales_result = cursor.fetchone()
            
            if sales_result and sales_result[0] > 0:
                result['insider_sales_count'] = sales_result[0]
                logger.info(f"✅ Trovate {sales_result[0]} vendite insider per {ticker}")
            
            # 3. Verifica dati insider purchases
            purchases_query = """
                SELECT COUNT(*) as count, MIN(it.transaction_date) as min_date, MAX(it.transaction_date) as max_date
                FROM insider_transactions it
                JOIN insider_filings if_t ON it.filing_id = if_t.id
                JOIN companies c ON if_t.company_id = c.id
                WHERE c.ticker = %s AND it.transaction_code = 'P'
                  AND it.transaction_shares IS NOT NULL 
                  AND it.transaction_price IS NOT NULL
                  AND it.transaction_shares > 0
                  AND it.transaction_price > 0
            """
            
            cursor.execute(purchases_query, (ticker,))
            purchases_result = cursor.fetchone()
            
            if purchases_result and purchases_result[0] > 0:
                result['insider_purchases_count'] = purchases_result[0]
                logger.info(f"✅ Trovati {purchases_result[0]} acquisti insider per {ticker}")
            
            # Combina date range per insider transactions
            all_dates = []
            if sales_result and sales_result[1]:
                all_dates.extend([sales_result[1], sales_result[2]])
            if purchases_result and purchases_result[1]:
                all_dates.extend([purchases_result[1], purchases_result[2]])
            
            if all_dates:
                result['date_range_insider'] = (min(all_dates), max(all_dates))
                result['insider_data_exists'] = True
            else:
                logger.warning(f"⚠️ Nessuna transazione insider trovata per {ticker}")
            
            # 4. Verifica dati prezzi storici
            try:
                price_query = f"""
                    SELECT COUNT(*) as count, MIN(Date) as min_date, MAX(Date) as max_date
                    FROM sidan.{ticker}
                """
                
                cursor.execute(price_query)
                price_result = cursor.fetchone()
                
                if price_result and price_result[0] > 0:
                    result['price_data_exists'] = True
                    result['price_records'] = price_result[0]
                    result['date_range_prices'] = (price_result[1], price_result[2])
                    logger.info(f"✅ Trovati {price_result[0]} records di prezzo per {ticker}")
                else:
                    logger.warning(f"⚠️ Nessun dato di prezzo trovato nella tabella sidan.{ticker}")
                    
            except Exception as price_error:
                logger.warning(f"⚠️ Tabella sidan.{ticker} non esiste o errore: {price_error}")
            
            # 5. Determina se il ticker è "utilizzabile"
            result['exists'] = result['company_exists'] and (result['price_data_exists'] or result['insider_data_exists'])
            
            cursor.close()
            return result
            
        except Exception as e:
            logger.error(f"❌ Errore nella verifica ticker {ticker}: {e}")
            return {'exists': False, 'error': str(e)}
    
    def test_database_data_availability(self, ticker: str) -> None:
        """
        Testa la disponibilità dei dati sia per insider sales/purchases che per i prezzi storici.
        """
        if not self.connection:
            logger.error("❌ Nessuna connessione al database")
            return
            
        try:
            # Usa la nuova funzione check_ticker_exists
            result = self.check_ticker_exists(ticker)
            
            print(f"\n🧪 REPORT DISPONIBILITÀ DATI PER {ticker}:")
            print(f"{'='*50}")
            
            if result.get('error'):
                print(f"❌ Errore: {result['error']}")
                return
            
            if not result['company_exists']:
                print(f"❌ Company {ticker} NON trovata nel database")
                return
            
            print(f"✅ Company: {result.get('company_name', 'N/A')} ({ticker})")
            
            if result['insider_data_exists']:
                print(f"✅ Transazioni Insider:")
                if result['insider_sales_count'] > 0:
                    print(f"   🔴 Vendite: {result['insider_sales_count']}")
                if result['insider_purchases_count'] > 0:
                    print(f"   🟢 Acquisti: {result['insider_purchases_count']}")
                if result['date_range_insider']:
                    print(f"   Range date: {result['date_range_insider'][0]} - {result['date_range_insider'][1]}")
            else:
                print(f"❌ Nessuna transazione insider trovata")
            
            if result['price_data_exists']:
                print(f"✅ Dati Prezzi: {result['price_records']} records")
                if result['date_range_prices']:
                    print(f"   Range date: {result['date_range_prices'][0]} - {result['date_range_prices'][1]}")
            else:
                print(f"❌ Nessun dato di prezzo trovato")
            
            print(f"{'='*50}")
            
            if result['exists']:
                print(f"🎯 TICKER {ticker} UTILIZZABILE per la visualizzazione")
            else:
                print(f"⚠️ TICKER {ticker} NON UTILIZZABILE - Dati insufficienti")
                
        except Exception as e:
            logger.error(f"❌ Errore nel test disponibilità dati: {e}")
            import traceback
            traceback.print_exc()
    
    def _open_file_automatically(self, filename: str) -> None:
        """Apre automaticamente il file immagine con il programma di default."""
        try:
            system = platform.system()
            abs_path = os.path.abspath(filename)
            
            if system == "Windows":
                os.startfile(abs_path)
            elif system == "Darwin":  # macOS
                subprocess.call(["open", abs_path])
            else:  # Linux
                subprocess.call(["xdg-open", abs_path])
                
            logger.info(f"🖼️ File aperto automaticamente: {abs_path}")
            
        except Exception as e:
            raise Exception(f"Impossibile aprire automaticamente: {e}")
    
    def debug_matplotlib_setup(self) -> None:
        """Debug per verificare la configurazione di matplotlib."""
        import matplotlib
        
        print(f"\n🔧 DEBUG MATPLOTLIB:")
        print(f"   Backend corrente: {matplotlib.get_backend()}")
        print(f"   Modalità interattiva: {plt.isinteractive()}")
        print(f"   Figure attive: {plt.get_fignums()}")
        
        # Test rapido
        try:
            test_fig, test_ax = plt.subplots(figsize=(6, 4))
            test_ax.plot([1, 2, 3], [1, 4, 2])
            test_ax.set_title("Test matplotlib")
            
            plt.savefig("test_matplotlib.png")
            print(f"   ✅ Test creazione grafico: OK")
            
            plt.show(block=False)
            plt.pause(0.5)
            print(f"   ✅ Test visualizzazione: OK")
            
            plt.close(test_fig)
            os.remove("test_matplotlib.png")
            
        except Exception as e:
            print(f"   ❌ Test fallito: {e}")


def get_available_tickers(db_config: Dict[str, Any]) -> List[str]:
    """
    Recupera la lista dei ticker disponibili nel database.
    """
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        
        # Query per ottenere tutti i ticker disponibili
        query = """
        SELECT DISTINCT c.ticker, c.name
        FROM companies c
        WHERE c.ticker IS NOT NULL AND c.ticker != ''
        ORDER BY c.ticker
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        tickers = []
        print(f"\n📋 TICKER DISPONIBILI NEL DATABASE:")
        print(f"{'='*50}")
        
        for ticker, company_name in results:
            tickers.append(ticker)
            print(f"   {ticker:<8} - {company_name}")
        
        print(f"{'='*50}")
        print(f"Totale: {len(tickers)} ticker disponibili")
        
        cursor.close()
        connection.close()
        
        return tickers
        
    except Exception as e:
        logger.error(f"❌ Errore nel recupero ticker disponibili: {e}")
        return []


def main():
    """Funzione principale AGGIORNATA per selezione ticker interattiva."""
    
    # Configurazione database
    DB_CONFIG = {
        'host': 'xxxxxx',
        'user': 'xxxxx', 
        'password': 'xxxxxx',
        'database': 'xxxxxx',
        'port': xxxxxxx
    }
    
    try:
        print("🚀 AVVIO VISUALIZZATORE INSIDER TRANSACTIONS - VENDITE E ACQUISTI")
        
        # Crea il visualizzatore
        visualizer = InsiderSalesVisualizer(DB_CONFIG)
        
        # Debug configurazione matplotlib
        # visualizer.debug_matplotlib_setup()
        
        # Connetti al database
        if not visualizer.connect_database():
            logger.error("❌ Impossibile connettersi al database")
            return
        
        try:
            # 1. Mostra ticker disponibili
            available_tickers = get_available_tickers(DB_CONFIG)
            
            if not available_tickers:
                print("❌ Nessun ticker trovato nel database")
                return
            
            # 2. Richiedi ticker all'utente
            while True:
                ticker = input(f"\n📈 Inserisci TICKER da visualizzare: ").strip().upper()
                
                if not ticker:
                    print("⚠️ Ticker non può essere vuoto. Riprova.")
                    continue
                
                # 3. Verifica esistenza ticker
                print(f"\n🔍 Verifica disponibilità dati per {ticker}...")
                ticker_info = visualizer.check_ticker_exists(ticker)
                
                if not ticker_info['exists']:
                    print(f"\n❌ TICKER {ticker} NON DISPONIBILE")
                    print(f"   Motivo: ", end="")
                    
                    if not ticker_info['company_exists']:
                        print(f"Company non trovata nel database")
                    elif not ticker_info['price_data_exists'] and not ticker_info['insider_data_exists']:
                        print(f"Nessun dato di prezzo o insider disponibile")
                    else:
                        print(f"Dati insufficienti")
                    
                    retry = input(f"\n🔄 Vuoi provare con un altro ticker? (s/n): ").strip().lower()
                    if retry != 's':
                        print("👋 Arrivederci!")
                        return
                    continue
                
                # 4. Ticker valido, mostra dettagli
                print(f"\n✅ TICKER {ticker} DISPONIBILE!")
                print(f"   Company: {ticker_info.get('company_name', 'N/A')}")
                if ticker_info['insider_sales_count'] > 0:
                    print(f"   🔴 Vendite Insider: {ticker_info['insider_sales_count']}")
                if ticker_info['insider_purchases_count'] > 0:
                    print(f"   🟢 Acquisti Insider: {ticker_info['insider_purchases_count']}")
                if ticker_info['price_data_exists']:
                    print(f"   📈 Dati Prezzi: {ticker_info['price_records']} records")
                
                break
            
            # 5. Parametri aggiuntivi
            days_input = input(f"\n📅 Giorni indietro (default 1825 = ~5 anni): ").strip()
            days_back = int(days_input) if days_input else 1825
            
            print(f"\n🎯 Creazione grafico per {ticker} - ultimi {days_back} giorni")
            
            # 6. Mostra test dettagliato (opzionale)
            show_test = input(f"\n🧪 Vuoi vedere il report dettagliato sui dati? (s/n): ").strip().lower()
            if show_test == 's':
                visualizer.test_database_data_availability(ticker)
            
            # 7. Crea il grafico
            visualizer.create_insider_sales_chart_FIXED(ticker=ticker, days_back=days_back)
            
        finally:
            visualizer.close_connection()
            
    except Exception as e:
        logger.error(f"❌ Errore nell'esecuzione principale: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n👋 Programma terminato. I grafici rimangono aperti.")


if __name__ == "__main__":
    main()
