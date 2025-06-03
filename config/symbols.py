"""
Stock symbols configuration
"""

# NSE Top 50 stocks (Nifty 50)
NIFTY_50 = [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
    'HDFC.NS', 'KOTAKBANK.NS', 'HINDUNILVR.NS', 'SBIN.NS', 'BHARTIARTL.NS',
    'ASIANPAINT.NS', 'ITC.NS', 'AXISBANK.NS', 'LT.NS', 'DMART.NS',
    'MARUTI.NS', 'TITAN.NS', 'BAJFINANCE.NS', 'NESTLEIND.NS', 'ULTRACEMCO.NS',
    'WIPRO.NS', 'ONGC.NS', 'NTPC.NS', 'TECHM.NS', 'HCLTECH.NS',
    'POWERGRID.NS', 'TATAMOTORS.NS', 'COALINDIA.NS', 'BAJAJFINSV.NS', 'M&M.NS',
    'SUNPHARMA.NS', 'TATASTEEL.NS', 'GRASIM.NS', 'ADANIPORTS.NS', 'BRITANNIA.NS',
    'DRREDDY.NS', 'EICHERMOT.NS', 'CIPLA.NS', 'BPCL.NS', 'HEROMOTOCO.NS',
    'JSWSTEEL.NS', 'INDUSINDBK.NS', 'DIVISLAB.NS', 'TATACONSUM.NS', 'APOLLOHOSP.NS',
    'BAJAJ-AUTO.NS', 'HINDALCO.NS', 'SHREECEM.NS', 'UPL.NS', 'SBILIFE.NS'
]

# Test symbols for development
TEST_SYMBOLS = [
    'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS'
]

# Banking sector
BANKING_STOCKS = [
    'HDFCBANK.NS', 'ICICIBANK.NS', 'SBIN.NS', 'KOTAKBANK.NS', 'AXISBANK.NS',
    'INDUSINDBK.NS', 'FEDERALBNK.NS', 'BANKBARODA.NS', 'PNB.NS', 'IDFCFIRSTB.NS'
]

# IT sector  
IT_STOCKS = [
    'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'TECHM.NS', 'HCLTECH.NS',
    'LTI.NS', 'MINDTREE.NS', 'MPHASIS.NS', 'LTTS.NS', 'COFORGE.NS'
]

# Auto sector
AUTO_STOCKS = [
    'MARUTI.NS', 'TATAMOTORS.NS', 'M&M.NS', 'EICHERMOT.NS', 'BAJAJ-AUTO.NS',
    'ASHOKLEY.NS', 'HEROMOTOCO.NS', 'TVSMOTOR.NS', 'BHARATFORG.NS', 'MOTHERSUMI.NS'
]

# Default symbols for different use cases
DEFAULT_SYMBOLS = {
    'development': TEST_SYMBOLS,
    'production': NIFTY_50,
    'sector_banking': BANKING_STOCKS,
    'sector_it': IT_STOCKS,
    'sector_auto': AUTO_STOCKS
}

def get_symbols(symbol_set='development'):
    """
    Get symbols for a specific set
    
    Args:
        symbol_set: One of 'development', 'production', 'sector_banking', etc.
    
    Returns:
        List of stock symbols
    """
    return DEFAULT_SYMBOLS.get(symbol_set, TEST_SYMBOLS) 