import pandas as pd
import requests
import yfinance
from pushbullet import Pushbullet
from datetime import datetime, date, timedelta
import pycurl
from io import BytesIO
import json
import urllib.parse
import os

# ==============================================================================
# 1. API KEY CONFIGURATION
# ==============================================================================
TDA_API_KEY = os.environ.get('TDA_API_KEY')
PUSHBULLET_API_KEY = os.environ.get('PUSHBULLET_API_KEY')

# ==============================================================================
# 2. STOCK UNIVERSE MODULE
# ==============================================================================
def get_universe():
    UNIVERSE = list(set([
        "SMCI", "TTD", "RKLB", "CRDO", "ALAB", "FTAI", "SATS", "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSLA", "AMD", "QCOM",
        "INTC", "CSCO", "CRM", "ADBE", "ORCL", "TXN", "SAP", "IBM", "UBER", "PYPL", "SQ", "MU", "ADI", "ASML", "LRCX", "AMAT", "KLAC", "SNPS",
        "CDNS", "PANW", "NOW", "SNOW", "ROP", "KEYS", "MSI", "HPQ", "DELL", "HPE", "NTAP", "STX", "WDC", "GLW", "TER", "FFIV", "CIEN", "EXTR",
        "AKAM", "SWKS", "QRVO", "ON", "MCHP", "NXPI", "MRVL", "INFY", "WIT", "CTSH", "ACN", "GIB", "EPAM", "CDW", "TDY", "TRMB", "PTC", "TYL",
        "ADSK", "ANSS", "V", "MA", "AXP", "FISV", "FIS", "GPN", "FLT", "SHOP", "ETSY", "CPNG", "CHWY", "DASH", "LYFT", "ZM", "DOCU", "TEAM",
        "WDAY", "HUBS", "RNG", "SMAR", "BILL", "DOCN", "MDB", "PLTR", "U", "RBLX", "APP", "DDOG", "NET", "OKTA", "ZS", "CRWD", "FTNT", "CHKP",
        "QLYS", "TENB", "VRNS", "CYBR", "S", "SYM", "GEN", "TSM", "ARM", "SOXL", "GFS", "UMC", "ASX", "ENTG", "MKSI", "FSLR", "BE", "PLUG",
        "ENPH", "SEDG", "RUN", "NOVA", "ARRY", "MAXN", "AI", "SOUN", "UPST", "PATH", "IOT", "GTLB", "STEM", "BLDE", "IONQ", "RGTI", "QUBT", "DM",
        "NNDM", "SMRT", "EVGO", "CHPT", "BLNK", "RIVN", "LCID", "PSNY", "ABNB", "COIN", "HOOD", "MQ", "AFRM", "SOFI", "NU", "OPRT", "LMND",
        "ROOT", "BMBL", "MTCH", "DUOL", "CHGG", "COUR", "SKLZ", "PLTK", "TTWO", "EA", "ROKU", "FUBO", "SPOT", "PINS", "SNAP", "WIX", "UPWK",
        "FVRR", "ASAN", "MNDY", "DOMO", "DT", "VRM", "CVNA", "MRNA", "BNTX", "NVAX", "REGN", "VRTX", "ALNY", "BIIB", "GILD", "AMGN", "EXEL",
        "IONS", "CRSP", "EDIT", "NTLA", "BEAM", "FATE", "IOVA", "ARVN", "INCY", "JAZZ", "HALO", "MYGN", "PACB", "TWST", "ILMN", "ABBV", "LLY",
        "MRK", "PFE", "BMY", "AZN", "NVS", "SNY", "GSK", "TAK", "ABT", "ISRG", "SYK", "MDT", "BDX", "ZBH", "BSX", "EW", "DHR", "TMO", "PODD",
        "DXCM", "MASI", "RMD", "TFX", "HOLX", "XRAY", "COO", "ALGN", "AXNX", "GMED", "ICUI", "PEN", "NVCR", "TNDM", "TECH", "CTLT", "VEEV",
        "IQV", "LH", "DGX", "IDXX", "WAT", "A", "CRL", "ZTS", "ELAN", "NTR", "MOS", "CF", "FMC", "CTVA", "XOM", "CVX", "COP", "EOG", "PSX",
        "MPC", "HAL", "SLB", "VLO", "OXY", "DVN", "KMI", "WMB", "OKE", "TRP", "ENB", "ET", "FANG", "MRO", "APA", "BKR", "FTI", "HP", "NBR",
        "RIG", "WHD", "PBF", "DK", "SU", "IMO", "CNQ", "CVE", "TRMLF", "LNG", "CTRA", "AR", "EQT", "RRC", "BTU", "METC", "CRK", "SM", "MTDR",
        "VNOM", "CIVI", "TPL", "PTEN", "RES", "OIS", "WTTR", "TEN", "DRVN", "OII", "JNJ", "UNH", "CVS", "CI", "ELV", "HUM", "CNC", "MOH",
        "HCA", "DVA", "UHS", "THC", "PG", "KO", "PEP", "WMT", "COST", "NKE", "HD", "LOW", "SBUX", "MCD", "CMG", "YUM", "QSR", "DRI", "TXRH",
        "EAT", "CAKE", "PZZA", "DPZ", "WING", "TGT", "DG", "DLTR", "BBY", "TJX", "ROST", "ANF", "AEO", "URBN", "LULU", "ULTA", "EL", "CL",
        "KMB", "CHD", "CLX", "HSY", "K", "GIS", "CPB", "MDLZ", "TSN", "HRL", "CAG", "MKC", "SJM", "BF-B", "STZ", "TAP", "SAM", "DEO", "BUD",
        "CCEP", "MNST", "CELH", "KDP", "F", "GM", "RACE", "TM", "HMC", "STLA", "MAR", "HLT", "BKNG", "EXPE", "CCL", "RCL", "NCLH", "WYNN",
        "LVS", "MGM", "CZR", "PENN", "DKNG", "DIS", "NFLX", "JPM", "BAC", "GS", "MS", "WFC", "BLK", "SCHW", "SPGI", "CME", "ICE", "NDAQ",
        "MCO", "AJG", "AON", "MMC", "BRO", "WTW", "TRV", "PGR", "ALL", "CB", "HIG", "AIG", "MET", "PRU", "AFL", "AMP", "BEN", "TROW", "IVZ",
        "NTRS", "STT", "BK", "USB", "PNC", "TFC", "FITB", "KEY", "HBAN", "RF", "CFG", "SYF", "COF", "ALLY", "SLM", "NAVI", "CACC", "OMF",
        "CAT", "DE", "GE", "LMT", "HON", "UNP", "BA", "NOC", "EMR", "ETN", "RTX", "GD", "TDG", "WM", "RSG", "URI", "ITW", "PH", "CMI",
        "PCAR", "FAST", "GWW", "MMM", "JCI", "XYL", "DOV", "AME", "SWK", "SNA", "IR", "TT", "IEX", "AOS", "FBIN", "MAS", "BLD", "DAL",
        "UAL", "AAL", "LUV", "ALK", "FDX", "UPS", "CHRW", "EXPD", "ODFL", "XPO", "JBHT", "KNX", "SNDR", "LIN", "APD", "NEM", "DOW", "ECL",
        "SHW", "PPG", "FCX", "SCCO", "GOLD", "FNV", "WPM", "AEM", "IFF", "DD", "LYB", "ALB", "SQM", "CE", "EMN", "OLN", "WLK", "AXTA",
        "CC", "AVTR", "STLD", "NUE", "CLF", "QQQ"
    ]))
    return UNIVERSE

# ==============================================================================
# 3. SIGNAL ENGINE MODULE
# ==============================================================================
def tema(series, period):
    ema1 = series.ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    ema3 = ema2.ewm(span=period, adjust=False).mean()
    return 3 * ema1 - 3 * ema2 + ema3

def calculate_adx(high, low, close, length):
    plus_dm = high.diff()
    minus_dm = low.diff()
    plus_dm[plus_dm < 0] = 0
    minus_dm[minus_dm > 0] = 0
    minus_dm = abs(minus_dm)
    tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/length, adjust=False).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/length, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/length, adjust=False).mean() / atr)
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-6))
    return dx.ewm(alpha=1/length, adjust=False).mean()

def generate_signals(tickers):
    print("Generating signals...")
    signals = []
    qqq_hist = yfinance.Ticker("QQQ").history(period="1y")
    qqq_sma200 = qqq_hist['Close'].rolling(window=200).mean().iloc[-1]
    qqq_close = qqq_hist['Close'].iloc[-1]
    is_bull_regime = qqq_close > qqq_sma200
    is_bear_regime = qqq_close < qqq_sma200
    print(f"Market Regime: {'BULL' if is_bull_regime else 'BEAR' if is_bear_regime else 'NEUTRAL'}")

    for ticker in tickers:
        if ticker == "QQQ": continue
        try:
            stock = yfinance.Ticker(ticker)
            daily_data = stock.history(period="1y")
            weekly_data = stock.history(period="2y", interval="1wk")
            if daily_data.empty or weekly_data.empty: continue

            daily_data['TEMA_fast'] = tema(daily_data['Close'], 5)
            daily_data['TEMA_slow'] = tema(daily_data['Close'], 50)
            daily_data['ADX'] = calculate_adx(daily_data['High'], daily_data['Low'], daily_data['Close'], 14)
            weekly_data['TEMA_fast'] = tema(weekly_data['Close'], 5)

            last_daily, prev_daily = daily_data.iloc[-1], daily_data.iloc[-2]
            last_weekly_tema = weekly_data['TEMA_fast'].iloc[-1]
            
            is_crossover_long = prev_daily['TEMA_fast'] < prev_daily['TEMA_slow'] and last_daily['TEMA_fast'] > last_daily['TEMA_slow']
            is_crossover_short = prev_daily['TEMA_fast'] > prev_daily['TEMA_slow'] and last_daily['TEMA_fast'] < last_daily['TEMA_slow']

            if is_bull_regime and is_crossover_long and last_daily['Close'] > last_weekly_tema and last_daily['ADX'] > 18.0:
                signals.append({'ticker': ticker, 'signal': 'LONG', 'price': last_daily['Close'], 'adx': last_daily['ADX']})
                print(f"  -> LONG Signal found for {ticker}")
            if is_bear_regime and is_crossover_short and last_daily['Close'] < last_weekly_tema and last_daily['ADX'] > 18.0:
                signals.append({'ticker': ticker, 'signal': 'SHORT', 'price': last_daily['Close'], 'adx': last_daily['ADX']})
                print(f"  -> SHORT Signal found for {ticker}")
        except Exception:
            pass
    return signals

# ==============================================================================
# 4. UTILITIES (OPTIONS & NOTIFY)
# ==============================================================================
def find_best_contract(ticker, direction, tda_api_key):
    try:
        contract_type = 'CALL' if direction == 'LONG' else 'PUT'
        from_date = date.today() + timedelta(days=45)
        to_date = date.today() + timedelta(days=120)
        
        endpoint = "https://api.tdameritrade.com/v1/marketdata/chains"
        params = {'apikey': tda_api_key, 'symbol': ticker, 'contractType': contract_type, 'strikeCount': 20, 'includeQuotes': 'FALSE', 'strategy': 'SINGLE', 'fromDate': from_date.strftime('%Y-%m-%d'), 'toDate': to_date.strftime('%Y-%m-%d')}
        url = endpoint + '?' + urllib.parse.urlencode(params)

        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.FOLLOWLOCATION, True)
        # --- FINAL FIX: Explicitly set the SSL/TLS version ---
        c.setopt(pycurl.SSLVERSION, pycurl.SSLVERSION_TLSv1_2)
        c.perform()
        
        http_code = c.getinfo(pycurl.HTTP_CODE)
        c.close()
        
        if http_code != 200:
            print(f"    - API call failed for {ticker} with HTTP status code: {http_code}")
            return None
        
        chain = json.loads(buffer.getvalue().decode('iso-8859-1'))
        if chain.get('status') != 'SUCCESS' or not chain.get('callExpDateMap'):
            print(f"    - No options chain returned for {ticker}.")
            return None

        all_contracts = []
        date_map = chain.get('callExpDateMap') if direction == 'LONG' else chain.get('putExpDateMap')
        for exp_date_str, strikes in date_map.items():
            for strike, contract_data in strikes.items():
                all_contracts.extend(contract_data)
        if not all_contracts: return None

        df = pd.DataFrame(all_contracts)
        df = df[df['inTheMoney'] == True]
        df = df[(df['openInterest'] >= 100) & (df['totalVolume'] >= 20)]
        df = df[(df['delta'] >= 0.60) & (df['delta'] <= 0.70)] if direction == 'LONG' else df[(df['delta'] <= -0.60) & (df['delta'] >= -0.70)]
        if df.empty: return None

        df['dte_target_diff'] = abs(df['daysToExpiration'] - 75)
        best_option = df.sort_values(by=['dte_target_diff', 'totalVolume'], ascending=[True, False]).iloc[0]
        print(f"    -> Suitable Option Found for {ticker}.")
        return {"strike": best_option['strikePrice'], "expiration": best_option['expirationDate'], "dte": int(best_option['daysToExpiration']), "delta": best_option['delta'], "iv": best_option['volatility'], "oi": best_option['openInterest'], "volume": best_option['totalVolume']}
    except Exception as e:
        print(f"    - An error occurred while fetching options for {ticker}: {e}")
        return None

def send_pushbullet(api_key, title, body):
    if api_key and body:
        try:
            pb = Pushbullet(api_key)
            pb.push_note(title, body)
            print("\n--> Notification sent successfully!")
        except Exception as e:
            print(f"\n--> ERROR sending notification: {e}")

# ==============================================================================
# 5. MAIN ORCHESTRATION FUNCTION
# ==============================================================================
def run_screener_main(request):
    """This function was originally for Google Cloud, we adapt it for script execution."""
    print(f"Scan started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    tickers = get_universe()
    print(f"Loaded {len(tickers)} tickers for scanning.")
    initial_signals = generate_signals(tickers)

    if not initial_signals:
        print("\nNo initial stock signals found today.")
        send_pushbullet(PUSHBULLET_API_KEY, "Mike's Algo Report", "No signals found today.")
        return

    print(f"\nFound {len(initial_signals)} initial stock signals. Now filtering for suitable options...")
    notification_body = ""
    final_signal_count = 0
    for sig in initial_signals:
        print(f"\n--- Analyzing Options for {sig['ticker']} ({sig['signal']}) ---")
        contract = find_best_contract(sig['ticker'], sig['signal'], TDA_API_KEY)
        if contract:
            final_signal_count += 1
            msg = (
                f"✅ [{sig['signal']}] {sig['ticker']} @ {sig['price']:.2f} (ADX: {sig['adx']:.1f})\n"
                f"-> Option: {datetime.fromtimestamp(contract['expiration']/1000).strftime('%d%b%y')} {contract['strike']:.1f}{'C' if sig['signal']=='LONG' else 'P'}\n"
                f"   (DTE: {contract['dte']}, Δ: {contract['delta']:.2f}, IV: {contract['iv']:.1%}, Vol: {contract['volume']:,}, OI: {contract['oi']:,})\n\n"
            )
            notification_body += msg

    if final_signal_count > 0:
        notification_title = f"Mike's Algo: {final_signal_count} Final Signal(s)"
        print(f"\n✅ Scan Complete. {final_signal_count} final signals with suitable options found.")
        send_pushbullet(PUSHBULLET_API_KEY, notification_title, notification_body.strip())
    else:
        print("\nNo signals met the options filtering criteria.")
        initial_body = f"{len(initial_signals)} initial signals found, but none had suitable options."
        send_pushbullet(PUSHBULLET_API_KEY, "Mike's Algo Report (No Options)", initial_body)

# ==============================================================================
# 6. SCRIPT EXECUTION BLOCK
# ==============================================================================
if __name__ == '__main__':
    run_screener_main(None)
