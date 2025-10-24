import pandas as pd
import yfinance
from pushbullet import Pushbullet
from datetime import datetime, date, timedelta
import os
# --- FINAL FIX: The import section is now cleaned up ---
# Only the necessary components from the current Alpaca library are imported.
from alpaca_trade_api.data.historical.option import OptionHistoricalDataClient
from alpaca_trade_api.data.requests import OptionChainRequest

# ==============================================================================
# 1. API KEY CONFIGURATION
# ==============================================================================
ALPACA_KEY = os.environ.get('ALPACA_KEY')
ALPACA_SECRET = os.environ.get('ALPACA_SECRET')
PUSHBULLET_API_KEY = os.environ.get('PUSHBULLET_API_KEY')

# ==============================================================================
# 2. SECTOR & UNIVERSE DATA
# ==============================================================================
SECTOR_MAP = {
    "CORE TECH": ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "AVGO", "TSLA", "AMD", "QCOM", "INTC", "CSCO", "CRM", "ADBE", "ORCL", "TXN", "SAP", "IBM", "UBER", "PYPL", "SQ", "MU", "ADI", "ASML", "LRCX", "AMAT", "KLAC", "SNPS", "CDNS", "PANW", "NOW", "SNOW", "ROP", "KEYS", "MSI", "HPQ", "DELL", "HPE", "NTAP", "STX", "WDC", "GLW", "TER", "FFIV", "CIEN", "EXTR", "AKAM", "SWKS", "QRVO", "ON", "MCHP", "NXPI", "MRVL", "INFY", "WIT", "CTSH", "ACN", "GIB", "EPAM", "CDW", "TDY", "TRMB", "PTC", "TYL", "ADSK", "ANSS", "V", "MA", "AXP", "FISV", "FIS", "GPN", "FLT", "SHOP", "ETSY", "CPNG", "CHWY", "DASH", "LYFT", "ZM", "DOCU", "TEAM", "WDAY", "HUBS", "RNG", "SMAR", "BILL", "DOCN", "MDB", "PLTR", "U", "RBLX", "APP", "DDOG", "NET", "OKTA", "ZS", "CRWD", "FTNT", "CHKP", "QLYS", "TENB", "VRNS", "CYBR", "S", "SYM", "GEN"],
    "HIGH-POTENTIAL TECH": ["SMCI", "TTD", "RKLB", "CRDO", "ALAB", "FTAI", "SATS"],
    "SEMICONDUCTORS": ["TSM", "ARM", "SOXL", "GFS", "UMC", "ASX", "ENTG", "MKSI"],
    "RENEWABLE ENERGY": ["FSLR", "BE", "PLUG", "ENPH", "SEDG", "RUN", "NOVA", "ARRY", "MAXN"],
    "EMERGING TECH": ["AI", "SOUN", "UPST", "PATH", "IOT", "GTLB", "STEM", "BLDE", "IONQ", "RGTI", "QUBT", "DM", "NNDM", "SMRT", "EVGO", "CHPT", "BLNK", "RIVN", "LCID", "PSNY", "ABNB", "COIN", "HOOD", "MQ", "AFRM", "SOFI", "NU", "OPRT", "LMND", "ROOT", "BMBL", "MTCH", "DUOL", "CHGG", "COUR", "SKLZ", "PLTK", "TTWO", "EA", "ROKU", "FUBO", "SPOT", "PINS", "SNAP", "WIX", "UPWK", "FVRR", "ASAN", "MNDY", "DOMO", "DT", "VRM", "CVNA"],
    "BIOTECH & HEALTHCARE": ["MRNA", "BNTX", "NVAX", "REGN", "VRTX", "ALNY", "BIIB", "GILD", "AMGN", "EXEL", "IONS", "CRSP", "EDIT", "NTLA", "BEAM", "FATE", "IOVA", "ARVN", "INCY", "JAZZ", "HALO", "MYGN", "PACB", "TWST", "ILMN", "ABBV", "LLY", "MRK", "PFE", "BMY", "AZN", "NVS", "SNY", "GSK", "TAK", "ABT", "ISRG", "SYK", "MDT", "BDX", "ZBH", "BSX", "EW", "DHR", "TMO", "PODD", "DXCM", "MASI", "RMD", "TFX", "HOLX", "XRAY", "COO", "ALGN", "AXNX", "GMED", "ICUI", "PEN", "NVCR", "TNDM", "TECH", "CTLT", "VEEV", "IQV", "LH", "DGX", "IDXX", "WAT", "A", "CRL", "ZTS", "ELAN", "JNJ", "UNH", "CVS", "CI", "ELV", "HUM", "CNC", "MOH", "HCA", "DVA", "UHS", "THC"],
    "AGRICULTURE": ["NTR", "MOS", "CF", "FMC", "CTVA"],
    "ENERGY": ["XOM", "CVX", "COP", "EOG", "PSX", "MPC", "HAL", "SLB", "VLO", "OXY", "DVN", "KMI", "WMB", "OKE", "TRP", "ENB", "ET", "FANG", "MRO", "APA", "BKR", "FTI", "HP", "NBR", "RIG", "WHD", "PBF", "DK", "SU", "IMO", "CNQ", "CVE", "TRMLF", "LNG", "CTRA", "AR", "EQT", "RRC", "BTU", "METC", "CRK", "SM", "MTDR", "VNOM", "CIVI", "TPL", "PTEN", "RES", "OIS", "WTTR", "TEN", "DRVN", "OII"],
    "CONSUMER": ["PG", "KO", "PEP", "WMT", "COST", "NKE", "HD", "LOW", "SBUX", "MCD", "CMG", "YUM", "QSR", "DRI", "TXRH", "EAT", "CAKE", "PZZA", "DPZ", "WING", "TGT", "DG", "DLTR", "BBY", "TJX", "ROST", "ANF", "AEO", "URBN", "LULU", "ULTA", "EL", "CL", "KMB", "CHD", "CLX", "HSY", "K", "GIS", "CPB", "MDLZ", "TSN", "HRL", "CAG", "MKC", "SJM", "BF-B", "STZ", "TAP", "SAM", "DEO", "BUD", "CCEP", "MNST", "CELH", "KDP", "F", "GM", "RACE", "TM", "HMC", "STLA", "MAR", "HLT", "BKNG", "EXPE", "CCL", "RCL", "NCLH", "WYNN", "LVS", "MGM", "CZR", "PENN", "DKNG", "DIS", "NFLX"],
    "FINANCIALS": ["JPM", "BAC", "GS", "MS", "WFC", "BLK", "SCHW", "SPGI", "CME", "ICE", "NDAQ", "MCO", "AJG", "AON", "MMC", "BRO", "WTW", "TRV", "PGR", "ALL", "CB", "HIG", "AIG", "MET", "PRU", "AFL", "AMP", "BEN", "TROW", "IVZ", "NTRS", "STT", "BK", "USB", "PNC", "TFC", "FITB", "KEY", "HBAN", "RF", "CFG", "SYF", "COF", "ALLY", "SLM", "NAVI", "CACC", "OMF"],
    "INDUSTRIALS": ["CAT", "DE", "GE", "LMT", "HON", "UNP", "BA", "NOC", "EMR", "ETN", "RTX", "GD", "TDG", "WM", "RSG", "URI", "ITW", "PH", "CMI", "PCAR", "FAST", "GWW", "MMM", "JCI", "XYL", "DOV", "AME", "SWK", "SNA", "IR", "TT", "IEX", "AOS", "FBIN", "MAS", "BLD", "DAL", "UAL", "AAL", "LUV", "ALK", "FDX", "UPS", "CHRW", "EXPD", "ODFL", "XPO", "JBHT", "KNX", "SNDR"],
    "MATERIALS": ["LIN", "APD", "NEM", "DOW", "ECL", "SHW", "PPG", "FCX", "SCCO", "GOLD", "FNV", "WPM", "AEM", "IFF", "DD", "LYB", "ALB", "SQM", "CE", "EMN", "OLN", "WLK", "AXTA", "CC", "AVTR", "STLD", "NUE", "CLF"]
}

def get_universe_and_mapping():
    universe = [ticker for sector_tickers in SECTOR_MAP.values() for ticker in sector_tickers]
    universe.append("QQQ")
    ticker_to_sector = {ticker: sector for sector, tickers in SECTOR_MAP.items() for ticker in tickers}
    return list(set(universe)), ticker_to_sector

# ==============================================================================
# 3. SIGNAL ENGINE MODULE
# ==============================================================================
def tema(series, period):
    ema1 = series.ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    ema3 = ema2.ewm(span=period, adjust=False).mean()
    return 3 * ema1 - 3 * ema2 + ema3

def calculate_adx(high, low, close, length):
    plus_dm = high.diff(); minus_dm = low.diff()
    plus_dm[plus_dm < 0] = 0; minus_dm[minus_dm > 0] = 0
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
            is_crossover_short = prev_daily['
