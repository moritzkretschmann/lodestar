#!/usr/bin/env python3
"""Lodestar: a local market terminal.

Serves index.html on http://127.0.0.1:8765 and proxies Finnhub (key read from
~/.config/lodestar/finnhub.key), public RSS feeds, OpenFIGI, Deutsche Börse delayed
data, Binance klines, Yahoo chart events and Business Insider ISIN lookups, so the
browser never sees the API key.
Standard library only.

    python3 ~/lodestar/server.py            # starts and opens the browser
    python3 ~/lodestar/server.py --no-open
"""
import base64
import concurrent.futures
import email.utils
import hashlib
import html
import json
import os
import pathlib
import queue
import re
import socket
import ssl
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = pathlib.Path(__file__).resolve().parent
CONF = pathlib.Path('~/.config/lodestar').expanduser()
KEY_FILE = CONF / 'finnhub.key'
CACHE_FILE = CONF / 'cache.json'
PORT = int(os.environ.get('LODESTAR_PORT', '8765'))
FH = 'https://finnhub.io/api/v1'
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Lodestar/1.0'
FEED_UA = 'Mozilla/5.0 (compatible; Lodestar/1.0 personal RSS reader)'

try:
    KEY = KEY_FILE.read_text().strip()
except FileNotFoundError:
    sys.exit(f'Missing {KEY_FILE}. Put your Finnhub API key in it.')

SYM_RE = re.compile(r'^[A-Za-z0-9.:_\-^=]{1,32}$')
ISIN_RE = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')
SUFFIX_CC = {'DE': 'DE', 'F': 'DE', 'BE': 'DE', 'MU': 'DE', 'SG': 'DE', 'HM': 'DE', 'DU': 'DE', 'HA': 'DE', 'L': 'GB', 'PA': 'FR',
             'AS': 'NL', 'MI': 'IT', 'MC': 'ES', 'SW': 'CH', 'VI': 'AT', 'BR': 'BE', 'ST': 'SE', 'CO': 'DK',
             'HE': 'FI', 'OL': 'NO', 'LS': 'PT', 'IR': 'IE', 'TO': 'CA', 'V': 'CA', 'T': 'JP', 'HK': 'HK',
             'AX': 'AU', 'NS': 'IN', 'BO': 'IN', 'KS': 'KR', 'SS': 'CN', 'SZ': 'CN'}

CATALOG = [
    ('BINANCE:BTCUSDT', 'Bitcoin / Tether', 'Crypto'), ('BINANCE:ETHUSDT', 'Ether / Tether', 'Crypto'),
    ('BINANCE:SOLUSDT', 'Solana / Tether', 'Crypto'), ('BINANCE:XRPUSDT', 'XRP / Tether', 'Crypto'),
    ('BINANCE:BNBUSDT', 'BNB / Tether', 'Crypto'), ('BINANCE:DOGEUSDT', 'Dogecoin / Tether', 'Crypto'),
    ('BINANCE:ADAUSDT', 'Cardano / Tether', 'Crypto'), ('BINANCE:LINKUSDT', 'Chainlink / Tether', 'Crypto'),
    ('BINANCE:AVAXUSDT', 'Avalanche / Tether', 'Crypto'), ('BINANCE:LTCUSDT', 'Litecoin / Tether', 'Crypto'),
    ('OANDA:EUR_USD', 'Euro / US Dollar', 'FX'), ('OANDA:GBP_USD', 'British Pound / US Dollar', 'FX'),
    ('OANDA:USD_JPY', 'US Dollar / Japanese Yen', 'FX'), ('OANDA:USD_CHF', 'US Dollar / Swiss Franc', 'FX'),
    ('OANDA:EUR_GBP', 'Euro / British Pound', 'FX'), ('OANDA:EUR_CHF', 'Euro / Swiss Franc', 'FX'),
    ('OANDA:AUD_USD', 'Australian Dollar / US Dollar', 'FX'), ('OANDA:USD_CAD', 'US Dollar / Canadian Dollar', 'FX'),
    ('OANDA:EUR_JPY', 'Euro / Japanese Yen', 'FX'), ('OANDA:USD_CNH', 'US Dollar / Offshore Yuan', 'FX'),
    ('OANDA:XAU_USD', 'Gold spot / US Dollar', 'Commodities'), ('OANDA:XAG_USD', 'Silver spot / US Dollar', 'Commodities'),
    ('OANDA:BCO_USD', 'Brent crude oil', 'Commodities'), ('OANDA:WTICO_USD', 'WTI crude oil', 'Commodities'),
    ('OANDA:NATGAS_USD', 'Natural gas', 'Commodities'), ('OANDA:XCU_USD', 'Copper', 'Commodities'),
    ('OANDA:SPX500_USD', 'S&P 500 index future (OANDA CFD)', 'Index futures'),
    ('OANDA:NAS100_USD', 'Nasdaq 100 index future (OANDA CFD)', 'Index futures'),
    ('OANDA:US30_USD', 'Dow Jones index future (OANDA CFD)', 'Index futures'),
    ('OANDA:DE30_EUR', 'DAX index future (OANDA CFD)', 'Index futures'),
    ('OANDA:EU50_EUR', 'Euro Stoxx 50 index future (OANDA CFD)', 'Index futures'),
    ('OANDA:JP225_USD', 'Nikkei 225 index future (OANDA CFD)', 'Index futures'),
    ('OANDA:HK33_HKD', 'Hang Seng index future (OANDA CFD)', 'Index futures'),
    ('OANDA:UK100_GBP', 'FTSE 100 index future (OANDA CFD)', 'Index futures'),
    ('OANDA:FR40_EUR', 'CAC 40 index future (OANDA CFD)', 'Index futures'),
    ('OANDA:US2000_USD', 'Russell 2000 index future (OANDA CFD)', 'Index futures'),
    ('YH:^KS11', 'KOSPI Composite index (Yahoo, may be delayed)', 'Index futures'),
    ('BINANCE:BTCEUR', 'Bitcoin / Euro', 'Crypto'),
]
CATALOG_MAP = {s: (n, g) for s, n, g in CATALOG}

# Exchange futures that OANDA has no CFD for, via Yahoo (continuous front month, delayed ~10–30 min).
# (Yahoo symbol, short label, full name, group, session key, extra search words incl. German)
YH_FUTURES = [
    ('ES=F', 'E-mini S&P', 'E-mini S&P 500 future (CME)', 'Index futures', 'globex', 'sp500 spx'),
    ('NQ=F', 'E-mini Nasdaq', 'E-mini Nasdaq 100 future (CME)', 'Index futures', 'globex', 'nasdaq100'),
    ('YM=F', 'E-mini Dow', 'E-mini Dow future (CBOT)', 'Index futures', 'globex', 'dow jones'),
    ('RTY=F', 'E-mini Russell', 'E-mini Russell 2000 future (CME)', 'Index futures', 'globex', 'russell 2000'),
    ('NKD=F', 'Nikkei (CME)', 'Nikkei 225 dollar future (CME)', 'Index futures', 'globex', 'nikkei'),
    ('KC=F', 'Coffee', 'Coffee "C" future (ICE US)', 'Commodity futures', 'coffee', 'kaffee arabica'),
    ('CC=F', 'Cocoa', 'Cocoa future (ICE US)', 'Commodity futures', 'cocoa', 'kakao'),
    ('SB=F', 'Sugar No. 11', 'Sugar No. 11 future (ICE US)', 'Commodity futures', 'sugar', 'zucker'),
    ('CT=F', 'Cotton', 'Cotton No. 2 future (ICE US)', 'Commodity futures', 'cotton', 'baumwolle'),
    ('OJ=F', 'Orange juice', 'Orange juice future (ICE US)', 'Commodity futures', 'oj', 'orangensaft fcoj'),
    ('GC=F', 'Gold (COMEX)', 'Gold future (COMEX)', 'Commodity futures', 'globex', 'gold'),
    ('SI=F', 'Silver (COMEX)', 'Silver future (COMEX)', 'Commodity futures', 'globex', 'silber'),
    ('HG=F', 'Copper', 'Copper future (COMEX)', 'Commodity futures', 'globex', 'kupfer'),
    ('PL=F', 'Platinum', 'Platinum future (NYMEX)', 'Commodity futures', 'globex', 'platin'),
    ('PA=F', 'Palladium', 'Palladium future (NYMEX)', 'Commodity futures', 'globex', 'palladium'),
    ('CL=F', 'WTI (NYMEX)', 'WTI crude oil future (NYMEX)', 'Commodity futures', 'globex', 'rohöl öl oil crude'),
    ('BZ=F', 'Brent (NYMEX)', 'Brent crude oil future (NYMEX)', 'Commodity futures', 'globex', 'rohöl öl oil crude'),
    ('NG=F', 'Natural gas', 'Henry Hub natural gas future (NYMEX)', 'Commodity futures', 'globex', 'erdgas gas'),
    ('HO=F', 'Heating oil', 'NY Harbor ULSD / heating oil future (NYMEX)', 'Commodity futures', 'globex', 'heizöl diesel'),
    ('RB=F', 'Gasoline', 'RBOB gasoline future (NYMEX)', 'Commodity futures', 'globex', 'benzin'),
    ('ZC=F', 'Corn', 'Corn future (CBOT)', 'Commodity futures', 'grain', 'mais'),
    ('ZW=F', 'Wheat', 'Chicago SRW wheat future (CBOT)', 'Commodity futures', 'grain', 'weizen'),
    ('KE=F', 'KC wheat', 'KC HRW wheat future (CBOT)', 'Commodity futures', 'grain', 'weizen'),
    ('ZS=F', 'Soybeans', 'Soybean future (CBOT)', 'Commodity futures', 'grain', 'sojabohnen soja'),
    ('ZM=F', 'Soybean meal', 'Soybean meal future (CBOT)', 'Commodity futures', 'grain', 'sojaschrot'),
    ('ZL=F', 'Soybean oil', 'Soybean oil future (CBOT)', 'Commodity futures', 'grain', 'sojaöl'),
    ('ZO=F', 'Oats', 'Oats future (CBOT)', 'Commodity futures', 'grain', 'hafer'),
    ('ZR=F', 'Rough rice', 'Rough rice future (CBOT)', 'Commodity futures', 'grain', 'reis rice'),
    ('LE=F', 'Live cattle', 'Live cattle future (CME)', 'Commodity futures', 'livestock', 'rinder lebendrind'),
    ('GF=F', 'Feeder cattle', 'Feeder cattle future (CME)', 'Commodity futures', 'livestock', 'mastrinder'),
    ('HE=F', 'Lean hogs', 'Lean hog future (CME)', 'Commodity futures', 'livestock', 'schweine mageres'),
    ('LBR=F', 'Lumber', 'Lumber future (CME)', 'Commodity futures', 'lumber', 'holz bauholz'),
    ('ZT=F', 'US 2Y note', '2-year US Treasury note future (CBOT)', 'Rate futures', 'globex', 'treasury bond anleihe'),
    ('ZF=F', 'US 5Y note', '5-year US Treasury note future (CBOT)', 'Rate futures', 'globex', 'treasury bond anleihe'),
    ('ZN=F', 'US 10Y note', '10-year US Treasury note future (CBOT)', 'Rate futures', 'globex', 'treasury bond anleihe'),
    ('ZB=F', 'US T-bond', '30-year US Treasury bond future (CBOT)', 'Rate futures', 'globex', 'treasury bond anleihe'),
    ('6E=F', 'Euro FX', 'Euro FX future (CME)', 'FX futures', 'globex', 'eur euro'),
    ('6B=F', 'British pound', 'British pound future (CME)', 'FX futures', 'globex', 'gbp pfund'),
    ('6J=F', 'Japanese yen', 'Japanese yen future (CME)', 'FX futures', 'globex', 'jpy yen'),
    ('6S=F', 'Swiss franc', 'Swiss franc future (CME)', 'FX futures', 'globex', 'chf franken'),
    ('BTC=F', 'Bitcoin (CME)', 'Bitcoin future (CME)', 'Crypto', 'globex', 'btc'),
    ('ETH=F', 'Ether (CME)', 'Ether future (CME)', 'Crypto', 'globex', 'eth ethereum'),
]
# Trading-hours keys for OANDA CFDs, by instrument prefix (the CFD follows its underlying future).
OANDA_SESS = {'SPX500': 'globex', 'NAS100': 'globex', 'US30': 'globex', 'US2000': 'globex', 'XAU': 'globex', 'XAG': 'globex',
              'XPT': 'globex', 'XPD': 'globex', 'XCU': 'globex', 'WTICO': 'globex', 'NATGAS': 'globex', 'USB': 'globex',
              'BCO': 'ice', 'CORN': 'grain', 'WHEAT': 'grain', 'SOYBN': 'grain', 'SUGAR': 'sugar', 'DE30': 'eurex',
              'EU50': 'eurex', 'DE10YB': 'eurex', 'UK100': 'iceuk', 'UK10YB': 'iceuk', 'FR40': 'euronext',
              'NL25': 'euronext', 'JP225': 'ose', 'HK33': 'hkfe'}
OANDA_COMMOD = ('XAU', 'XAG', 'XPT', 'XPD', 'XCU', 'BCO', 'WTICO', 'NATGAS', 'CORN', 'WHEAT', 'SOYBN', 'SUGAR')


def oanda_catalog():
    """Every OANDA instrument on Finnhub's free stream (FX pairs, index, commodity and bond CFDs), cached a day."""
    try:
        rows = fh('forex/symbol', {'exchange': 'oanda'}, ttl=86400, persist=True)
    except Upstream:
        return []
    out = []
    for r in rows:
        sym = r.get('symbol') or ''
        if not sym.startswith('OANDA:') or not SYM_RE.match(sym):
            continue
        code = sym[6:]
        desc = (r.get('description') or code).replace('Oanda ', '')
        is_fx = bool(re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', code)) and not code.startswith(OANDA_COMMOD)
        if is_fx:
            group, name = 'FX', desc
        elif code.startswith(OANDA_COMMOD):
            group, name = 'Commodity futures', f'{desc} (OANDA CFD)'
        elif code.startswith(('USB', 'DE10YB', 'UK10YB')):
            group, name = 'Rate futures', f'{desc} (OANDA CFD)'
        else:
            group, name = 'Index futures', f'{desc} index (OANDA CFD)'
        sess = 'fx' if is_fx else next((v for k, v in OANDA_SESS.items() if code.startswith(k)), None)
        out.append({'symbol': sym, 'name': name, 'group': group, 'sess': sess, 'label': desc if not is_fx else code.replace('_', '/')})
    return out

# (source, url, category, region, poll seconds). Categories: markets, companies, economy, cb (central banks and
# regulators), crypto, fxc (FX and commodities), wires (company press releases). finnhub:* are Finnhub news categories.
FEEDS = [
    ('Finnhub (Reuters, CNBC…)', 'finnhub:general', 'markets', 'US', 180),
    ('Finnhub forex', 'finnhub:forex', 'fxc', 'Global', 300),
    ('Finnhub crypto', 'finnhub:crypto', 'crypto', 'Global', 300),
    ('Finnhub M&A', 'finnhub:merger', 'wires', 'US', 300),
    ('CNBC', 'https://www.cnbc.com/id/100003114/device/rss/rss.html', 'markets', 'US', 300),
    ('CNBC Economy', 'https://www.cnbc.com/id/20910258/device/rss/rss.html', 'economy', 'US', 300),
    ('CNBC Finance', 'https://www.cnbc.com/id/10000664/device/rss/rss.html', 'markets', 'US', 300),
    ('CNBC Earnings', 'https://www.cnbc.com/id/15839135/device/rss/rss.html', 'companies', 'US', 300),
    ('CNBC Europe', 'https://www.cnbc.com/id/19794221/device/rss/rss.html', 'markets', 'EU', 300),
    ('CNBC Asia', 'https://www.cnbc.com/id/19832390/device/rss/rss.html', 'markets', 'Asia', 300),
    ('MarketWatch', 'https://feeds.content.dowjones.io/public/rss/mw_topstories', 'markets', 'US', 300),
    ('WSJ Markets', 'https://feeds.content.dowjones.io/public/rss/RSSMarketsMain', 'markets', 'US', 300),
    ('WSJ World', 'https://feeds.content.dowjones.io/public/rss/RSSWorldNews', 'economy', 'Global', 600),
    ('Financial Times Markets', 'https://www.ft.com/markets?format=rss', 'markets', 'UK', 300),
    ('Financial Times Companies', 'https://www.ft.com/companies?format=rss', 'companies', 'UK', 300),
    ('Bloomberg Markets', 'https://www.bloomberg.com/feeds/markets/news.rss', 'markets', 'US', 300),
    ('Bloomberg Economics', 'https://www.bloomberg.com/feeds/economics/news.rss', 'economy', 'US', 600),
    ('Seeking Alpha', 'https://seekingalpha.com/market_currents.xml', 'markets', 'US', 300),
    ('Investing.com', 'https://www.investing.com/rss/news.rss', 'markets', 'Global', 600),
    ('Fortune', 'https://fortune.com/feed/', 'companies', 'US', 900),
    ('BBC Business', 'https://feeds.bbci.co.uk/news/business/rss.xml', 'economy', 'UK', 600),
    ('The Guardian Business', 'https://www.theguardian.com/uk/business/rss', 'economy', 'UK', 600),
    ('New York Times Business', 'https://rss.nytimes.com/services/xml/rss/nyt/Business.xml', 'economy', 'US', 600),
    ('The Economist', 'https://www.economist.com/finance-and-economics/rss.xml', 'economy', 'Global', 1800),
    ('Al Jazeera', 'https://www.aljazeera.com/xml/rss/all.xml', 'economy', 'MidEast', 900),
    ('Federal Reserve', 'https://www.federalreserve.gov/feeds/press_all.xml', 'cb', 'US', 900),
    ('ECB', 'https://www.ecb.europa.eu/rss/press.html', 'cb', 'EU', 900),
    ('Bank of England', 'https://www.bankofengland.co.uk/rss/news', 'cb', 'UK', 900),
    ('Deutsche Bundesbank', 'https://www.bundesbank.de/service/rss/de/633286/feed.rss', 'cb', 'DE', 900),
    ('Swiss National Bank', 'https://www.snb.ch/public/rss/en/news', 'cb', 'CH', 900),
    ('SEC', 'https://www.sec.gov/news/pressreleases.rss', 'cb', 'US', 1800),
    ('ESMA', 'https://www.esma.europa.eu/rss.xml', 'cb', 'EU', 1800),
    ('Handelsblatt Finanzen', 'https://feeds.cms.handelsblatt.com/finanzen', 'markets', 'DE', 300),
    ('Handelsblatt', 'https://www.handelsblatt.com/contentexport/feed/top-themen', 'economy', 'DE', 600),
    ('tagesschau Wirtschaft', 'https://www.tagesschau.de/wirtschaft/index~rss2.xml', 'economy', 'DE', 600),
    ('FAZ Finanzen', 'https://www.faz.net/rss/aktuell/finanzen/', 'markets', 'DE', 300),
    ('Spiegel Wirtschaft', 'https://www.spiegel.de/wirtschaft/index.rss', 'economy', 'DE', 600),
    ('Zeit Wirtschaft', 'https://newsfeed.zeit.de/wirtschaft/index', 'economy', 'DE', 900),
    ('n-tv Wirtschaft', 'https://www.n-tv.de/wirtschaft/rss', 'markets', 'DE', 600),
    ('NZZ Wirtschaft', 'https://www.nzz.ch/wirtschaft.rss', 'economy', 'CH', 900),
    ('Nikkei Asia', 'https://asia.nikkei.com/rss/feed/nar', 'markets', 'JP', 600),
    ('South China Morning Post', 'https://www.scmp.com/rss/92/feed/', 'economy', 'HK', 600),
    ('Economic Times Markets', 'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms', 'markets', 'IN', 600),
    ('Straits Times Business', 'https://www.straitstimes.com/news/business/rss.xml', 'economy', 'SG', 900),
    ('ABC Australia Business', 'https://www.abc.net.au/news/feed/51892/rss.xml', 'economy', 'AU', 900),
    ('Globe and Mail Business', 'https://www.theglobeandmail.com/arc/outboundfeeds/rss/category/business/', 'markets', 'CA', 600),
    ('CoinDesk', 'https://www.coindesk.com/arc/outboundfeeds/rss', 'crypto', 'Global', 300),
    ('Cointelegraph', 'https://cointelegraph.com/rss', 'crypto', 'Global', 300),
    ('The Block', 'https://www.theblock.co/rss.xml', 'crypto', 'Global', 300),
    ('Decrypt', 'https://decrypt.co/feed', 'crypto', 'Global', 600),
    ('FXStreet', 'https://www.fxstreet.com/rss/news', 'fxc', 'Global', 300),
    ('investingLive (Forexlive)', 'https://investinglive.com/feed/news', 'fxc', 'Global', 300),
    ('OilPrice.com', 'https://oilprice.com/rss/main', 'fxc', 'Global', 600),
    ('Mining.com', 'https://www.mining.com/feed/', 'fxc', 'Global', 900),
    ('Business Wire', 'https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFtRWA==', 'wires', 'US', 60),
    ('PR Newswire', 'https://www.prnewswire.com/rss/news-releases-list.rss', 'wires', 'US', 60),
    ('PR Newswire Earnings', 'https://www.prnewswire.com/rss/financial-services-latest-news/earnings-list.rss', 'wires', 'US', 60),
]
REGION_GROUPS = {'europe': {'EU', 'UK', 'DE', 'CH', 'IT', 'ES', 'FR'}, 'asia': {'Asia', 'JP', 'HK', 'IN', 'SG', 'AU', 'CN'}}

# Corporate-action headline classifier (English and German wording)
CA_RULES = [
    ('Split', r'(reverse\s+)?(stock|share)\s+split|\d+[- ]for[- ]\d+\s+(reverse\s+)?(stock\s+)?split|aktiensplit|reverse\s+split'),
    ('Dividend', r'(declar\w*|announc\w*|rais\w*|increas\w*|hikes?|cuts?|suspend\w*|reinstat\w*|special|quarterly|monthly|interim|final|initiat\w*)\s+(\w+\s+){0,3}(cash\s+)?dividends?\b'
                 r'|dividends?\s+(increase|hike|cut|declaration|of\s+\$)|sonderdividende|dividendenerh\w+|dividendenk\w+|dividende\s+(von|in\s+h\w+he|erh\w+|senk\w*|anheb\w*|zahl\w*)'),
    ('Tender offer', r'tender\s+offer|exchange\s+offer|übernahmeangebot|pflichtangebot|squeeze-?out|rückkaufangebot|abfindungsangebot'),
    ('M&A', r'\bto\s+acquire\b|\bacquires\b|\bacquisition\s+of\b|agree[sd]?\s+to\s+(buy|acquire|merge|sell|be\s+acquired)|merger\s+agreement|definitive\s+agreement|\bto\s+merge\b|takeover\s+(bid|offer|approach)|\bbuyout\b|übernimmt|übernahme\b|\bfusion\b|kauft\s+\w+\s+für'),
    ('Buyback', r'(share|stock)\s+(re)?purchase|buy-?back|aktienrückkauf|rückkaufprogramm'),
    ('Capital raise', r'(public|secondary|registered\s+direct|follow-on|underwritten)\s+offering|pricing\s+of\s+.{0,60}offering|rights\s+(issue|offering)|kapitalerhöhung|bezugsrecht|private\s+placement|convertible\s+(senior\s+)?notes\s+offering|at-the-market\s+offering'),
    ('Spin-off', r'spin-?\s?offs?|spins?\s+off|spinoff|demerger|carve-?\s?out|abspaltung|abspalten|abgespalten|aufspaltung|ausgliederung|separation\s+into\s+two'),
    ('IPO / listing', r'\bIPOs?\b|initial\s+public\s+offering|börsengang|börsendebüt|erstnotiz|(market|trading|stock\s+market)\s+debut|\bdebut\b|debüt|begins\s+trading|commence\s+trading|uplist\w*|direct\s+listing'),
    ('Delisting', r'delist\w*|notierungseinstellung|going\s+private'),
    ('Name change', r'name\s+change|ticker\s+(symbol\s+)?change|changes?\s+(its\s+)?(corporate\s+)?name|umfirmierung'),
    ('AGM', r'hauptversammlung|annual\s+general\s+meeting|\bAGM\b|annual\s+(shareholder|stockholder)s?\s+meeting|shareholders?\s+(approve|vote)|aktionäre\s+(stimmen|billigen|genehmigen)'),
    ('Dividend', r'\bdividende\b|dividendenzahlung|dividendenvorschlag|dividendenpolitik|ausschüttung|\bpays?\s+(a\s+)?dividend|dividend\s+(payment|proposal|policy)'),
]
CA_RES = [(k, re.compile(p, re.I)) for k, p in CA_RULES]
CA_NOISE = re.compile(r'\b(stocks?\s+to\s+(buy|watch|own)|best\s+dividend|top\s+\d+|\d+\s+(high-yield|dividend|growth)\s+stocks?|dividend\s+stocks?|etf|passive\s+income)\b', re.I)


# Only English and German headlines are kept. Stopword counts are enough for headlines; words shared between
# languages (de, en, la, des, die/de…) are left out so they don't decide anything.
LANG_WORDS = {
    'en': set('the and of to for with on is are as by its from at this that will has have new announces after over into'.split()),
    'de': set('der die das und mit für von den dem im auf ist sind zu eine einen nach bei nicht auch wird über aus'.split()),
    'fr': set('le les et pour avec une du sur dans est au aux leur sont pas plus nouveau nouvelle annonce'.split()),
    'es': set('el los las del para con una por que se su sus más nuevo nueva anuncia'.split()),
    'it': set('il della di per con gli che delle nel una sono dei alla al più nuova'.split()),
    'nl': set('het van voor met een op zijn niet ook naar wordt'.split()),
    'pt': set('os da do das dos para com uma em não ao mais nova'.split()),
}
NON_LATIN = re.compile(r'[\u0400-\u04ff\u0590-\u06ff\u0900-\u0dff\u3040-\u30ff\u3400-\u9fff\uac00-\ud7af]')


def lang_ok(text):
    """False only when the text is clearly in a language other than English or German."""
    letters = [c for c in text if c.isalpha()]
    if letters and len(NON_LATIN.findall(text)) > len(letters) * 0.3:
        return False
    words = re.findall(r"[a-zà-öø-ÿ']+", text.lower())
    score = {k: sum(w in ws for w in words) for k, ws in LANG_WORDS.items()}
    other = max(v for k, v in score.items() if k not in ('en', 'de'))
    return not (other >= 2 and other > max(score['en'], score['de']))


# ---------- "Hot": market-moving headlines (macro data, central banks, big-company earnings, big deals, shocks) ----------
BIG_COMPANIES = re.compile(r"\b(Apple|Microsoft|Nvidia|NVIDIA|Amazon|Alphabet|Google|Meta|Tesla|Berkshire|JPMorgan|Exxon|Walmart|"
    r"Eli Lilly|Broadcom|Netflix|Palantir|AMD|Intel|Oracle|Salesforce|Boeing|Goldman Sachs|Morgan Stanley|Bank of America|"
    r"Advanced Micro Devices|UnitedHealth|Costco|Coca-Cola|Pfizer|Disney|IBM|Qualcomm|Micron|Chevron|"
    r"SAP|Siemens|Allianz|Telekom|Mercedes|BMW|Volkswagen|VW|Porsche|BASF|Bayer|Deutsche Bank|Commerzbank|Infineon|"
    r"Rheinmetall|Airbus|Adidas|Munich Re|Münchener Rück|E\.ON|Eon|RWE|DHL|Merck|Henkel|Continental|Zalando|Lufthansa|"
    r"Daimler Truck|Hannover Rück|Vonovia|Beiersdorf|Heidelberg Materials|MTU|Qiagen|Brenntag|Sartorius|Symrise|Fresenius|"
    r"Deutsche Börse|Thyssenkrupp|thyssenkrupp|Schaeffler|"
    r"ASML|LVMH|Nestlé|Nestle|Novo Nordisk|Shell|TotalEnergies|Roche|Novartis|Unilever|HSBC|AstraZeneca|UBS|BP|Stellantis|"
    r"TSMC|Samsung|Toyota|Alibaba|Tencent|BYD|Saudi Aramco|Aramco)\b")
BIG_MARKETS = re.compile(r"\b(DAX|Dow|Dow Jones|S&P 500|S&P|Nasdaq|Wall Street|Euro Stoxx|Stoxx 600|Nikkei|Hang Seng|"
                         r"Bitcoin|Ölpreis|oil prices?|Brent|Goldpreis|gold price|Euro|Dollar|Treasur(y|ies)|Bund(esanleihen)?)\b")
HOT_RULES = [  # (label, weight, pattern); each label counts once, first match wins the label
    ('Central bank', 4, r"\b(rate decision|zinsentscheid\w*|leitzins\w*|cuts? (interest )?rates|raises? (interest )?rates|holds? rates|"
                        r"rate (cut|hike)s?|zinssenkung\w*|zinserhöhung\w*|senkt (die )?zinsen|erhöht (die )?zinsen|fomc|"
                        r"zinspause)\b"),
    ('Central bank', 2, r"\b(fed|federal reserve|ecb|ezb|bank of england|bank of japan|boj|snb|powell|lagarde|bundesbank)\b"),
    # Named indicators are hot on their own; plain "inflation" only with data context (a number, "data", "rises"...).
    ('Macro data', 4, r"\b(cpi|nonfarm|payrolls|jobs report|job openings|jolts|arbeitsmarktbericht|unemployment rate|"
                      r"arbeitslosenquote|arbeitslosenzahl|gdp|bip|bruttoinlandsprodukt|pce|pmi|einkaufsmanager\w*|ifo|zew|"
                      r"retail sales|einzelhandelsums[aä]tze?|industrieproduktion|auftragseingang|verbraucherpreise|inflationsrate)\b|"
                      r"\b(inflation|teuerung)\b.{0,50}(\d|data|daten|figures|rises?|rose|falls?|fell|jumps?|slows?|eases?|"
                      r"accelerat\w*|steigt|sinkt|fällt|klettert)|(\d[\d,.]*\s?(%|prozent)).{0,40}\b(inflation|teuerung)\b"),
    ('Earnings', 2, r"\b(earnings|quarterly results|q[1-4] results|results beat|beats estimates|misses estimates|guidance|outlook|"
                    r"profit warning|quartalszahlen|quartalsergebnis\w*|geschäftszahlen|zahlen vorgelegt|gewinnwarnung|prognose\w*|"
                    r"gewinnziel\w*|übertrifft erwartungen|verfehlt erwartungen|umsatz\w*|gewinn\w*|revenue|net income|eps|profit)\b|"
                    r"\btops? .{0,25}\b(eps|estimates|expectations|forecasts?)|\bbeats? (on )?(revenue|wall street|expectations|forecasts?|estimates)|"
                    r"\bmiss(es)? (on )?(revenue|expectations|estimates)|\bq[1-4] eps|financial results|"
                    r"\breports? (q[1-4]|first|second|third|fourth)[- ]quarter"),
    ('Big deal', 4, r"(billion|milliard\w*|mrd\.?|\$\d+\s?bn|\d+\s?bn\b).{0,60}\b(acqui\w*|merger|takeover|übernahme\w*|übernimmt|"
                    r"fusion\w*|to buy|buys|kauft)\b|\b(acqui\w*|merger|takeover|übernahme\w*|übernimmt|fusion\w*|to buy|buys|kauft)\b"
                    r".{0,60}(billion|milliard\w*|mrd\.?|\$\d+\s?bn)"),
    ('OPEC', 4, r"\bopec\+?.{0,50}\b(cuts?|raises?|boosts?|output|production|förder\w*|kürz\w*)"),
    ('Market move', 2, r"\b(crash\w*|plunge\w*|tumble\w*|soar\w*|surge\w*|sell-?off|absturz|stürzt\w*|bricht ein|einbruch|rally|"
                       r"record high|all-time high|rekordhoch|allzeithoch|rekordjagd|bankrupt\w*|insolven\w*|chapter 11|default)\b"),
    ('Geopolitics', 1, r"\b(opec\+?|sanctions?|sanktion\w*|tariffs?|zölle?n?|embargo)\b"),
]
HOT_RES = [(lbl, w, re.compile(pat, re.I)) for lbl, w, pat in HOT_RULES]
# Actual results, as opposed to previews and trading ideas around them.
RESULT_RX = re.compile(r"\b(beats?|tops?|miss(es)?|reports?|posts?|delivers?|announces?|unveils?)\b.{0,70}\b(eps|earnings|results|revenue|"
                       r"sales|profit|quarter|q[1-4]|guidance|outlook|expectations|estimates|forecasts?|consensus)\b|financial results|\bq[1-4] (eps|results)\b|"
                       r"\b(raises?|cuts?|lowers?|boosts?|lifts?|hikes?|trims?|reaffirms?)\b.{0,25}\b(guidance|outlook|forecast)|"
                       r"quartalszahlen|quartalsergebnis\w*|geschäftszahlen|zahlen vorgelegt|(hebt|senkt|bestätigt).{0,25}prognose|gewinnwarnung", re.I)
PREVIEW_RX = re.compile(r"preview|ahead of|into (its )?earnings|before (its |the )?(earnings|results|report)|what to (watch|expect)|"
                        r"expected to|week ahead|options?|volatility|to report|will report|set to report|could|vor den zahlen|"
                        r"vorschau|erwartet|to participate|conference call|webcast|announces date|schedule", re.I)
HOT_MIN = 4


def hot_score(title):
    """(score, reasons) for one headline. Earnings/market moves only count with a big company or a big market."""
    score, why = 0, []
    big = bool(BIG_COMPANIES.search(title))
    mkt = bool(BIG_MARKETS.search(title))
    for lbl, w, rx in HOT_RES:
        if lbl in why or not rx.search(title):
            continue
        if lbl == 'Earnings' and (not big or PREVIEW_RX.search(title)):
            continue
        if lbl == 'Market move' and not (big or mkt):
            continue
        score += w
        why.append(lbl)
    if why and (big or (mkt and 'Market move' in why)):
        score += 2
    return score, why


def classify(title, summary=''):
    text = title + ' ' + summary
    if CA_NOISE.search(title):
        return None
    for k, rx in CA_RES:
        if rx.search(text):
            return k
    return None


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


class Upstream(Exception):
    def __init__(self, status, msg):
        super().__init__(msg)
        self.status = status


def http_get(url, headers=None, timeout=10, limit=6_000_000):
    req = urllib.request.Request(url, headers={'User-Agent': UA, **(headers or {})})
    host = urllib.parse.urlsplit(url).netloc
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read(limit)
    except urllib.error.HTTPError as e:
        raise Upstream(e.code, f'{host} answered HTTP {e.code}') from None
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise Upstream(502, f'{host} unreachable ({e})') from None


class Cache:
    """In-memory cache plus a small on-disk cache for slow-changing lookups (names, ISINs)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.mem, self.dirty = {}, False
        try:
            self.disk = json.loads(CACHE_FILE.read_text())
        except (OSError, ValueError):
            self.disk = {}

    def get(self, key):
        now = time.time()
        with self.lock:
            for store in (self.mem, self.disk):
                hit = store.get(key)
                if hit and hit[0] > now:
                    return True, hit[1]
        return False, None

    def put(self, key, value, ttl, persist=False):
        with self.lock:
            (self.disk if persist else self.mem)[key] = [time.time() + ttl, value]
            self.dirty |= persist

    def flush(self):
        with self.lock:
            if not self.dirty:
                return
            now = time.time()
            data = {k: v for k, v in self.disk.items() if v[0] > now}
            self.dirty = False
        tmp = CACHE_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(data))
        os.chmod(tmp, 0o600)
        tmp.replace(CACHE_FILE)


class Limiter:
    """Sliding-window limiter; Finnhub's free plan allows 60 calls a minute."""

    def __init__(self, n, per, msg='Finnhub free plan limit (60 calls a minute) reached; retrying shortly'):
        self.n, self.per, self.ts, self.lock, self.msg = n, per, [], threading.Lock(), msg

    def used(self):
        with self.lock:
            now = time.time()
            return sum(1 for t in self.ts if now - t < self.per)

    def try_acquire(self):
        with self.lock:
            now = time.time()
            self.ts = [t for t in self.ts if now - t < self.per]
            if len(self.ts) < self.n:
                self.ts.append(now)
                return True
            return False

    def acquire(self, wait=8):
        deadline = time.time() + wait
        while True:
            with self.lock:
                now = time.time()
                self.ts = [t for t in self.ts if now - t < self.per]
                if len(self.ts) < self.n:
                    self.ts.append(now)
                    return
                delay = self.per - (now - self.ts[0]) + 0.05
            if time.time() + delay > deadline:
                raise Upstream(429, self.msg)
            time.sleep(min(delay, 1))


cache = Cache()
limiter = Limiter(55, 60)


def fh(path, params=None, ttl=10, persist=False):
    params = params or {}
    key = 'fh:' + path + '?' + urllib.parse.urlencode(sorted(params.items()))
    ok, v = cache.get(key)
    if ok:
        if isinstance(v, dict) and v.get('__err'):
            raise Upstream(v['__err'], v['msg'])
        return v
    limiter.acquire()
    try:
        v = json.loads(http_get(f'{FH}/{path}?{urllib.parse.urlencode(params)}', {'X-Finnhub-Token': KEY}))
    except Upstream as e:
        if e.status == 403:
            msg = 'Not included in the Finnhub free plan'
            cache.put(key, {'__err': 403, 'msg': msg}, 3600)
            raise Upstream(403, msg) from None
        raise
    except ValueError:
        raise Upstream(502, 'Finnhub returned an unreadable response') from None
    cache.put(key, v, ttl, persist)
    return v


# ---------- ISINs via Business Insider's public search ----------
BI_ROW = re.compile(r'new Array\("((?:[^"\\]|\\.)*)", "((?:[^"\\]|\\.)*)", "((?:[^"\\]|\\.)*)"')


def bi_suggest(q):
    """Returns [{name, ticker, isin}] or None when the lookup failed (not cached)."""
    key = 'bi:' + q.lower()
    ok, v = cache.get(key)
    if ok:
        return v
    try:
        raw = http_get('https://markets.businessinsider.com/ajax/SearchController_Suggest?'
                       + urllib.parse.urlencode({'max_results': 20, 'query': q})).decode('utf-8', 'replace')
    except Upstream:
        return None
    out = []
    for name, cat, kw in BI_ROW.findall(raw):
        parts = kw.split('|')
        if len(parts) >= 2 and ISIN_RE.match(parts[1]):
            out.append({'name': name.replace('\\"', '"'), 'cat': cat, 'ticker': parts[0].upper(), 'isin': parts[1]})
    cache.put(key, out, 7 * 86400, persist=True)
    return out


def split_sym(sym):
    base, _, suffix = sym.upper().partition('.')
    if len(suffix) == 1 and suffix not in SUFFIX_CC:  # share classes like BRK.B
        return sym.upper(), ''
    return base, suffix


def is_live(sym):
    if ':' in sym:
        return True
    _, suffix = split_sym(sym)
    return suffix == ''


NAME_NOISE = {'reg', 'registered', 'shs', 'shares', 'share', 'ord', 'ordinary', 'inc', 'corp', 'corporation', 'co',
              'company', 'plc', 'ag', 'se', 'sa', 'nv', 'n', 'v', 'ltd', 'limited', 'the', 'class', 'cl', 'a', 'holding',
              'holdings', 'group', 'kgaa', 'spa', 'asa', 'ab', 'oyj', 'as', 'bhd', 'na', 'vz', 'st', 'akt', 'adr'}


def norm_name(name):
    words = re.sub(r'[^a-z0-9]+', ' ', (name or '').lower()).split()
    return ' '.join(w for w in words if w not in NAME_NOISE)


def pick_isin(sym, hits, name=None):
    base, suffix = split_sym(sym)
    want = SUFFIX_CC.get(suffix) if suffix else 'US'
    tick = base.replace('.', '')
    cand = [h for h in hits if h['ticker'].replace('.', '') == tick]
    pref = [h for h in cand if h['isin'][:2] == want]
    if pref or (cand and not suffix):
        return (pref or cand)[0]
    # Non-US listings: BI files them under US OTC tickers, so match on the company name instead.
    nn = norm_name(name)
    if not nn or 'adr' in (name or '').lower():
        return None
    same = [h for h in hits if norm_name(h['name']) == nn and 'adr' not in h['name'].lower()]
    if want:
        same = [h for h in same if h['isin'][:2] == want]
    return same[0] if same else None


def isin_for(sym, name=''):
    if ':' in sym:
        return None
    key = 'isin:' + sym
    ok, v = cache.get(key)
    if ok:
        return v
    hits = bi_suggest(split_sym(sym)[0])
    if hits is None:
        return None
    h = pick_isin(sym, hits, name)
    if not h and name:
        more = bi_suggest(name[:40])
        h = pick_isin(sym, more or [], name)
    v = h['isin'] if h else None
    cache.put(key, v, 7 * 86400 if v else 86400, persist=True)
    return v


# ---------- Bloomberg-style tickers via OpenFIGI (Bloomberg's free identifier service) ----------
EXCH_NAMES = {
    'US': 'US composite', 'UN': 'NYSE', 'UW': 'Nasdaq GS', 'UQ': 'Nasdaq GM', 'UR': 'Nasdaq CM', 'UA': 'NYSE American',
    'UP': 'NYSE Arca', 'UV': 'OTC US', 'PQ': 'OTC Markets',
    'GR': 'Germany composite', 'GY': 'Xetra', 'GF': 'Frankfurt', 'TH': 'Tradegate', 'LA': 'LS Exchange',
    'LU': 'Lang & Schwarz (LSSI)', 'GZ': 'gettex', 'QT': 'Quotrix', 'GS': 'Stuttgart', 'GM': 'Munich',
    'GD': 'Düsseldorf', 'GH': 'Hamburg', 'GI': 'Hannover', 'GB': 'Berlin',
    'LN': 'London', 'FP': 'Euronext Paris', 'NA': 'Euronext Amsterdam', 'BB': 'Euronext Brussels',
    'ID': 'Euronext Dublin', 'PL': 'Euronext Lisbon', 'IM': 'Borsa Italiana', 'SM': 'Madrid', 'SE': 'SIX Swiss',
    'AV': 'Vienna', 'SS': 'Stockholm', 'DC': 'Copenhagen', 'FH': 'Helsinki', 'NO': 'Oslo',
    'IX': 'Cboe CXE', 'EB': 'Cboe BXE', 'EU': 'European composite',
    'CT': 'Toronto', 'JT': 'Tokyo', 'HK': 'Hong Kong', 'AT': 'ASX',
}
US_CODES = {'US', 'UN', 'UW', 'UQ', 'UR', 'UA', 'UP', 'UV', 'PQ'}
LISTING_ORDER = ['US', 'UW', 'UN', 'UQ', 'UR', 'UA', 'UP', 'PQ', 'UV', 'GR', 'GY', 'GF', 'TH', 'LA', 'LU', 'GZ', 'QT',
                 'GS', 'GM', 'GD', 'GH', 'GB', 'GI', 'LN', 'FP', 'NA', 'SE', 'IM', 'SM', 'AV', 'BB', 'ID', 'PL', 'SS',
                 'DC', 'FH', 'NO', 'IX', 'EB', 'EU', 'CT', 'JT', 'HK', 'AT']
FINNHUB_SUFFIX = {'GY': 'DE', 'GF': 'F', 'GS': 'SG', 'GM': 'MU', 'GD': 'DU', 'GH': 'HM', 'GB': 'BE', 'GI': 'HA',
                  'LN': 'L', 'FP': 'PA', 'NA': 'AS', 'IM': 'MI', 'SM': 'MC', 'SE': 'SW', 'AV': 'VI', 'BB': 'BR',
                  'ID': 'IR', 'PL': 'LS', 'SS': 'ST', 'DC': 'CO', 'FH': 'HE', 'NO': 'OL', 'CT': 'TO', 'JT': 'T',
                  'HK': 'HK', 'AT': 'AX'}
BBG_RE = re.compile(r'^([A-Z0-9][A-Z0-9./\-]{0,14})\s+([A-Z][A-Z0-9])(?:\s+EQUITY)?$')
figi_limiter = Limiter(20, 60, 'OpenFIGI allows 25 lookups a minute without a key; try again shortly')


def figi_map(job):
    key = 'figi:' + json.dumps(job, sort_keys=True)
    ok, v = cache.get(key)
    if ok:
        return v
    figi_limiter.acquire(wait=6)
    req = urllib.request.Request('https://api.openfigi.com/v3/mapping', data=json.dumps([job]).encode(), method='POST',
                                 headers={'Content-Type': 'application/json', 'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            res = json.loads(r.read())[0]
    except urllib.error.HTTPError as e:
        raise Upstream(e.code, f'OpenFIGI answered HTTP {e.code}') from None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, IndexError) as e:
        raise Upstream(502, f'OpenFIGI unreachable ({e})') from None
    data = res.get('data') or []
    cache.put(key, data, 7 * 86400 if data else 86400, persist=True)
    return data


def bbg_symbol(ticker, code):
    if code in US_CODES:
        return ticker.replace('/', '.')
    t = ticker.replace('/', '-').replace(' ', '-')
    return f'{t}.{FINNHUB_SUFFIX.get(code, code)}'


def listing_entries(data):
    eq = [d for d in data if d.get('marketSector') == 'Equity' and d.get('exchCode') in EXCH_NAMES]
    has_us = any(d['exchCode'] == 'US' for d in eq)
    out, seen = [], set()
    for d in eq:
        code = d['exchCode']
        if code in US_CODES and code != 'US' and has_us:
            continue
        sym = bbg_symbol(d.get('ticker') or '', code)
        if not SYM_RE.match(sym) or sym in seen:
            continue
        seen.add(sym)
        out.append({'symbol': sym, 'bbg': f"{d.get('ticker')} {code}", 'exch': code, 'venue': EXCH_NAMES[code],
                    'figiName': d.get('name') or '', 'shareClass': d.get('shareClassFIGI'),
                    'secType': d.get('securityType2') or d.get('securityType') or '', 'live': code in US_CODES,
                    'data': data_kind(code)})
    out.sort(key=lambda e: LISTING_ORDER.index(e['exch']))
    return out


TITLE_WORDS = {'INC', 'CORP', 'LTD', 'CO', 'THE', 'AND', 'OF', 'REG', 'HLDG', 'GRP', 'INTL'}


def nice_name(n):
    if not n or not n.isupper():
        return n or ''
    # Keep short all-caps words (SAP, SE, AG, NV) in capitals; title-case the rest.
    return re.sub(r"[A-Z][A-Z']*", lambda m: m.group(0) if len(m.group(0)) <= 3 and m.group(0) not in TITLE_WORDS
                  else m.group(0).title().replace("'S", "'s"), n)


def isin_from_listings(ents):
    us = next((e for e in ents if e['live']), None)
    if us:
        return isin_for(us['symbol'], nice_name(us['figiName']))
    de = next((e for e in ents if e['exch'] == 'GY'), None)
    return isin_for(de['symbol'], nice_name(de['figiName'])) if de else None


def as_results(ents, isin):
    return [{'symbol': e['symbol'], 'bbg': e['bbg'], 'name': nice_name(e['figiName']), 'isin': isin,
             'group': 'ETFs' if 'ETP' in e['secType'] or 'ETF' in e['secType'] else 'Equities',
             'type': e['venue'], 'venue': e['venue'], 'live': e['live'], 'data': e['data']} for e in ents]


def bbg_search(ticker, code):
    data = figi_map({'idType': 'TICKER', 'idValue': ticker, 'exchCode': code})
    if not data:
        return []
    sc = data[0].get('shareClassFIGI')
    ents = listing_entries(figi_map({'idType': 'ID_BB_GLOBAL_SHARE_CLASS_LEVEL', 'idValue': sc}) if sc else data)
    isin = isin_from_listings(ents)
    target = bbg_symbol(ticker, code)
    ents = order_listings(ents, isin)
    hit = next((e for e in ents[1:] if e['symbol'] == target), None)
    if hit:  # the listing that was typed comes right after the main listing
        ents.remove(hit)
        ents.insert(1, hit)
    return as_results(ents, isin)


def listings(sym, isin):
    if isin and ISIN_RE.match(isin):
        ents = order_listings(listing_entries(figi_map({'idType': 'ID_ISIN', 'idValue': isin})), isin)
    elif ':' not in sym and is_live(sym):
        data = figi_map({'idType': 'TICKER', 'idValue': sym.replace('.', '/'), 'exchCode': 'US'})
        sc = data[0].get('shareClassFIGI') if data else None
        ents = order_listings(listing_entries(figi_map({'idType': 'ID_BB_GLOBAL_SHARE_CLASS_LEVEL', 'idValue': sc})), 'US') if sc else []
    else:
        ents = []
    return {'items': ents[:40]}


# ---------- Deutsche Börse delayed data (MiFIR Art. 13, 15 minutes, free for non-commercial use) ----------
DB_VENUES = {'GY': ('DETR', 'Xetra'), 'GF': ('DFRA', 'Börse Frankfurt'), 'TH': ('DGAT', 'Tradegate')}
DB_PRETRADE = {'TH', 'GF'}  # Xetra's quote file is ~28 MB a minute, so Xetra gets trades only
SUFFIX_VENUE = {'DE': 'GY', 'F': 'GF', 'TH': 'TH'}
DB_DIR = CONF / 'delayed'
DB_UA = 'Lodestar/1.0 (personal, non-commercial use of MiFIR delayed data)'
DB_API = 'https://mfs.deutsche-boerse.com/api/'


class Budget(Exception):
    pass


# Deutsche Börse answers HTTP 429 after roughly 30 quick downloads, so downloads share one modest budget.
db_limiter = Limiter(25, 600, 'Deutsche Börse download budget used up')


def db_venue(sym):
    return SUFFIX_VENUE.get(split_sym(sym)[1]) if ':' not in sym else None


def db_ts(s):
    """'2026-11-03T16:19:59.995426512Z' -> epoch seconds."""
    base, _, frac = (s or '').rstrip('Z').partition('.')
    try:
        t = datetime.strptime(base, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None
    return t + (float('0.' + frac[:6]) if frac else 0)


class Delayed:
    def __init__(self):
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.interest = {}   # (venue, isin) -> last request time
        self.state = {}      # (venue, isin) -> {'trades': [...], 'done': set()}
        self.files = {}      # venue -> today's minute files on disk, oldest first
        self.prev = {}       # venue -> {'file': daily name, 'px': {isin: last price}}
        self.quotes = {}     # venue -> {'t': fetched, 'q': {isin: pretrade record}}
        self.synced = {}     # venue -> last sync time
        self.error = {}      # venue -> last error text
        self.backfilling = set()
        self.cooldown = 0.0  # until when Deutsche Börse asked us to back off (HTTP 429)
        self.priority = None  # venue of the instrument currently open
        self.expected = {}    # venue -> number of minute files published today
        threading.Thread(target=self.loop, daemon=True, name='delayed').start()

    def active(self):
        now = time.time()
        with self.lock:
            for k in [k for k, t in self.interest.items() if now - t > 900]:
                del self.interest[k]
            return dict(self.interest)

    def loop(self):
        while True:
            self.wake.wait(10)
            self.wake.clear()
            act = self.active()
            if time.time() < self.cooldown:
                continue
            for code in sorted(DB_VENUES, key=lambda c: c != self.priority):
                isins = {i for (c, i) in act if c == code}
                if not isins:
                    continue
                try:
                    if time.time() - self.synced.get(code, 0) >= 60:
                        self.sync(code)
                    self.index(code, isins)
                    if code in DB_PRETRADE and time.time() - self.quotes.get(code, {}).get('t', 0) >= 300:
                        self.pretrade(code, isins)
                    self.error.pop(code, None)
                except Budget:
                    pass
                except Upstream as e:
                    if e.status == 429:
                        self.cooldown = time.time() + 600
                        db_limiter.n = max(6, db_limiter.n // 2)
                        for c in DB_VENUES:
                            self.error[c] = 'Deutsche Börse asked us to slow down; continuing in 10 minutes'
                        log(f'delayed data: HTTP 429, download budget now {db_limiter.n} per 10 min')
                        break
                    else:
                        self.error[code] = str(e)[:200]
                    log(f'delayed data {code}: {e}')
                except Exception as e:  # noqa: BLE001 - keep the loop alive
                    self.error[code] = str(e)[:200]
                    log(f'delayed data {code}: {e}')

    def listing(self, kind, prefix):
        return json.loads(http_get(f'{DB_API}{prefix}-{kind}', {'User-Agent': DB_UA}, timeout=20)).get('CurrentFiles') or []

    def download(self, name, dest):
        if not db_limiter.try_acquire():
            raise Budget()
        raw = http_get(f'{DB_API}download/{name}', {'User-Agent': DB_UA}, timeout=60, limit=60_000_000)
        if raw[:2] != b'\x1f\x8b':
            raise Upstream(502, f'{name} is not a gzip file')
        if dest is None:
            return raw
        tmp = dest.with_suffix('.part')
        tmp.write_bytes(raw)
        tmp.replace(dest)

    def sync(self, code):
        self.synced[code] = time.time()
        prefix = DB_VENUES[code][0]
        names = self.listing('posttrade', prefix)
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        # German pre-market opens 07:00 CET (05:00 UTC in summer); earlier minute files are empty.
        want = sorted(n for n in names if n.startswith(f'{prefix}-posttrade-{today}T') and n.endswith('.json.gz')
                      and n[len(prefix) + 22:len(prefix) + 24] >= '05')
        d = DB_DIR / prefix
        d.mkdir(parents=True, exist_ok=True)
        missing = [n for n in want if not (d / n).exists()]
        self.expected[code] = len(want)
        daily = next((n for n in names if 'daily' in n), None)
        if daily and self.prev.get(code, {}).get('file') != daily:
            self.load_prev(code, d, daily)  # previous close first: one file, needed for the day change
        # New minutes first so every venue keeps up; then at most 8 older minutes per round, so venues share the budget.
        have = max((n for n in want if (d / n).exists()), default='')
        newer = [n for n in missing if n > have]
        older = [n for n in reversed(missing) if n <= have][:8]
        try:
            for n in newer + older:
                self.download(n, d / n)
                time.sleep(1)
        except Budget:
            pass
        finally:
            self.files[code] = [n for n in want if (d / n).exists()]
            if len(self.files[code]) < len(want):
                self.backfilling.add(code)
            else:
                self.backfilling.discard(code)
        keep = set(self.files[code]) | {f'prev-{daily}.json'}
        for p in d.iterdir():
            if p.name not in keep:
                p.unlink(missing_ok=True)
        self.synced[code] = time.time()

    def load_prev(self, code, d, daily):
        cache_file = d / f'prev-{daily}.json'
        try:
            px = json.loads(cache_file.read_text())
        except (OSError, ValueError):
            raw = self.download(daily, None)
            last = {}
            for line in gzip_lines(raw):
                r = parse_trade(line)
                if r and (r[0] not in last or r[1] >= last[r[0]][0]):
                    last[r[0]] = (r[1], r[2])
            px = {k: v[1] for k, v in last.items()}
            cache_file.write_text(json.dumps(px))
        self.prev[code] = {'file': daily, 'px': px}

    def index(self, code, isins):
        files = self.files.get(code) or []
        d = DB_DIR / DB_VENUES[code][0]
        with self.lock:
            states = {i: self.state.setdefault((code, i), {'trades': [], 'done': set()}) for i in isins}
        for name in files:
            need = [i for i, st in states.items() if name not in st['done']]
            if not need:
                continue
            try:
                raw = gzip_decompress((d / name).read_bytes())
            except OSError:
                continue
            hits = {i: [] for i in need if i.encode() in raw}
            if hits:
                for line in raw.splitlines():
                    for i in hits:
                        if i.encode() in line:
                            r = parse_trade(line)
                            if r and r[0] == i:
                                hits[i].append((r[1], r[2], r[3]))
            with self.lock:
                for i in need:
                    st = states[i]
                    st['trades'].extend(hits.get(i, []))
                    st['done'].add(name)
        with self.lock:
            for st in states.values():
                st['trades'].sort()

    def pretrade(self, code, isins):
        prefix = DB_VENUES[code][0]
        names = [n for n in self.listing('pretrade', prefix) if 'daily' not in n]
        if not names:
            return
        raw = self.download(max(names), None)
        found = {}
        keys = [(i, i.encode()) for i in isins]
        for line in gzip_lines(raw):
            for i, b in keys:
                if b in line:
                    try:
                        r = json.loads(line)
                    except ValueError:
                        continue
                    if r.get('instrumentIdentificationCode') == i:
                        t = db_ts(r.get('updateDateAndTime'))
                        if i not in found or (t or 0) >= found[i]['t']:
                            found[i] = {'t': t, 'bid': r.get('bid'), 'ask': r.get('ask'), 'bidQty': r.get('bidQty'),
                                        'askQty': r.get('askQty'), 'phase': r.get('tradingSystemPhase')}
        self.quotes[code] = {'t': time.time(), 'q': {**self.quotes.get(code, {}).get('q', {}), **found}}

    def get(self, sym, isin, selected=False):
        code = db_venue(sym)
        if not code:
            raise Upstream(400, 'No delayed data for this venue')
        if not ISIN_RE.match(isin or ''):
            raise Upstream(400, 'An ISIN is needed for delayed data')
        key = (code, isin)
        with self.lock:
            new = key not in self.interest
            self.interest[key] = time.time()
            st = self.state.get(key)
            trades = list(st['trades']) if st else []
            indexed = bool(st) and len(st['done']) >= len(self.files.get(code) or [1])
        if selected:
            self.priority = code
        if new:
            self.wake.set()
        prev = self.prev.get(code, {}).get('px', {}).get(isin)
        q = self.quotes.get(code, {}).get('q', {}).get(isin)
        bars = []
        for t, p, v in trades:
            bt = int(t // 60 * 60)
            if bars and bars[-1][0] == bt:
                b = bars[-1]
                b[2], b[3], b[4], b[5] = max(b[2], p), min(b[3], p), p, b[5] + v
            else:
                bars.append([bt, p, p, p, p, v])
        files = self.files.get(code) or []
        as_of = None
        if files:
            m = re.search(r'T(\d\d)_(\d\d)', files[-1])
            if m:
                as_of = datetime.now(timezone.utc).replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0,
                                                           microsecond=0).timestamp() + 60
        loading = not indexed or not files
        partial = code in self.backfilling
        since = None
        if files:
            m0 = re.search(r'T(\d\d)_(\d\d)', files[0])
            since = f'{m0.group(1)}:{m0.group(2)} UTC' if m0 else None
        note = None
        if self.error.get(code):
            note = f'Deutsche Börse delayed data: {self.error[code]}'
        elif loading:
            note = f'Loading {DB_VENUES[code][1]} trades from Deutsche Börse…'
        elif partial:
            note = (f'{len(files)} of {self.expected.get(code, len(files))} minutes of today loaded so far; the rest fills in '
                    'over the next hour or so. Open, high, low and volume cover the loaded minutes only.')
        elif not trades:
            note = f'No trades on {DB_VENUES[code][1]} today yet.'
        return {'venue': code, 'venueName': DB_VENUES[code][1], 'delayed': True, 'delayMin': 15, 'asOf': as_of,
                'last': trades[-1][1] if trades else prev, 'lastT': trades[-1][0] if trades else None,
                'open': trades[0][1] if trades else None, 'high': max(t[1] for t in trades) if trades else None,
                'low': min(t[1] for t in trades) if trades else None, 'volume': sum(t[2] for t in trades),
                'trades': len(trades), 'prevClose': prev, 'quote': q, 'bars': bars,
                'prints': [[t, p, v] for t, p, v in trades[-150:]][::-1], 'loading': loading, 'partial': partial,
                'since': since, 'note': note,
                'quotesIncluded': code in DB_PRETRADE}


def gzip_decompress(raw):
    import gzip
    return gzip.decompress(raw)


def gzip_lines(raw):
    import gzip
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as g:
        for line in g:
            yield line


def parse_trade(line):
    """One post-trade NDJSON line -> (isin, time, price, qty), skipping cancellations and amendments."""
    try:
        r = json.loads(line)
    except ValueError:
        return None
    if r.get('mmtModificationInd') not in (None, '-') or not r.get('price') or not r.get('quantity'):
        return None
    t = db_ts(r.get('tradingDateAndTime'))
    return (r.get('instrumentIdentificationCode'), t, float(r['price']), float(r['quantity'])) if t else None


delayed = None


def data_kind(code):
    return 'live' if code in US_CODES else 'delayed' if code in DB_VENUES else 'none'


MAIN_EXCH = {'US': 'US', 'DE': 'GY', 'GB': 'LN', 'FR': 'FP', 'NL': 'NA', 'CH': 'SE', 'IT': 'IM', 'ES': 'SM', 'AT': 'AV',
             'BE': 'BB', 'PT': 'PL', 'SE': 'SS', 'DK': 'DC', 'FI': 'FH', 'NO': 'NO', 'CA': 'CT', 'JP': 'JT', 'HK': 'HK',
             'AU': 'AT', 'IE': 'ID'}


def order_listings(ents, isin):
    """Main listing first, then venues with real-time data, then 15-minute delayed ones, then the rest."""
    if not ents:
        return ents
    main = MAIN_EXCH.get((isin or 'US')[:2])
    if not any(e['exch'] == main for e in ents):
        main = next((e['exch'] for e in ents if e['exch'] in US_CODES), None) or next((e['exch'] for e in ents if e['exch'] in DB_VENUES), ents[0]['exch'])
    rank = {'live': 1, 'delayed': 2, 'none': 3}
    pos = {c: i for i, c in enumerate(LISTING_ORDER)}
    return sorted(ents, key=lambda e: (e['exch'] != main, rank[data_kind(e['exch'])], pos.get(e['exch'], 99)))


def group_for(typ):
    t = (typ or '').lower()
    if t in ('etp', 'etf', 'etn') or 'etf' in t:
        return 'ETFs'
    if any(k in t for k in ('stock', 'adr', 'reit', 'gdr', 'ndr', 'preferred', 'equity')):
        return 'Equities'
    return 'Other'


def search(q):
    q = re.sub(r'\s+', ' ', q.strip()[:60])
    if not q:
        return {'results': []}
    ql = q.lower()
    out, seen, err = [], set(), None
    m = BBG_RE.match(q.upper())
    if m and m.group(2) in EXCH_NAMES:  # Bloomberg style: "NVD GY", "SAP GR Equity"
        try:
            res = bbg_search(m.group(1), m.group(2))
            if res:
                return {'results': res[:30], 'error': None}
        except Upstream as e:
            err = str(e)
    flat = ql.replace('/', '').replace(' ', '')
    for sym, name, group in CATALOG:
        if ql in name.lower() or flat in sym.lower().replace('_', '') or ql == sym.lower():
            out.append({'symbol': sym, 'name': name, 'group': group, 'type': group, 'isin': None, 'live': True, 'data': 'live',
                        'sess': next((v for k, v in OANDA_SESS.items() if sym.startswith('OANDA:' + k)), None)})
            seen.add(sym)
    if len(ql) >= 2:
        for o in oanda_catalog():
            if o['symbol'] not in seen and (ql in o['name'].lower() or flat in o['symbol'].lower().replace('_', '')):
                out.append({**o, 'type': 'OANDA CFD', 'venue': 'OANDA', 'isin': None, 'live': True, 'data': 'live'})
                seen.add(o['symbol'])
        for ysym, label, name, group, sess, words in YH_FUTURES:
            sym = 'YH:' + ysym
            hay = f'{label} {name} {words} {ysym}'.lower()
            if sym not in seen and all(w in hay for w in ql.split()):
                out.append({'symbol': sym, 'label': label, 'name': name, 'group': group, 'sess': sess, 'type': 'Future',
                            'venue': 'Yahoo futures', 'isin': None, 'live': True, 'data': 'yahoo'})
                seen.add(sym)
    try:
        res = fh('search', {'q': q}, ttl=3600, persist=True).get('result', [])
    except Upstream as e:
        res, err = [], str(e)
    q_isin = q.upper() if ISIN_RE.match(q.upper()) else None
    bi = bi_suggest(q) or []
    if not q_isin and res:
        tickers = {split_sym(r['symbol'])[0] for r in res[:6]}
        if len(tickers) == 1:  # a ticker query: also look up the ticker itself
            bi = bi + (bi_suggest(next(iter(tickers))) or [])
    for r in res[:30]:
        sym = r.get('symbol', '')
        if not sym or sym in seen or not SYM_RE.match(sym):
            continue
        seen.add(sym)
        h = pick_isin(sym, bi, r.get('description'))
        isin = q_isin or (h['isin'] if h else None)
        name = (h['name'] if h and not split_sym(sym)[1] else '') or r.get('description') or sym
        out.append({'symbol': sym, 'name': name, 'group': group_for(r.get('type')), 'type': r.get('type') or '',
                    'isin': isin, 'live': is_live(sym),
                    'data': 'live' if is_live(sym) else 'delayed' if db_venue(sym) else 'none'})
    if q_isin:  # every listing of the ISIN with Bloomberg codes: main listing, real-time, delayed, rest
        try:
            ents = order_listings(listing_entries(figi_map({'idType': 'ID_ISIN', 'idValue': q_isin})), q_isin)
        except Upstream as e:
            ents, err = [], err or str(e)
        if ents:
            res = as_results(ents, q_isin)
            names = {o['symbol']: o['name'] for o in out if o.get('name')}
            for r in res:
                r['name'] = names.get(r['symbol']) or r['name']
            syms = {r['symbol'] for r in res}
            return {'results': (res + [o for o in out if o['symbol'] not in syms])[:40], 'error': err}
    qu = q.upper()
    out.sort(key=lambda x: (x['symbol'].split(':')[-1].upper() != qu, not x['live']))
    exact = next((o for o in out if o['symbol'] == qu and o['live'] and ':' not in o['symbol']), None)
    if exact and re.fullmatch(r'[A-Z0-9.]{1,8}', qu):  # a ticker: main listing first, then its other venues
        try:
            ents = listings(qu, exact.get('isin'))['items']
            exact.update(bbg=f'{qu} US', venue='US composite')
            more = [r for r in as_results(ents, exact.get('isin')) if r['symbol'] != qu]
            for r in more:
                r['name'] = exact['name']
            more_syms = {r['symbol'] for r in more}
            futs = [o for o in out if o['symbol'].startswith(('OANDA:', 'YH:'))]  # "corn": futures before the ETF's other venues
            rest = [o for o in out if o is not exact and o not in futs and o['symbol'] not in more_syms]
            out = [exact] + futs + more + rest
        except Upstream:
            pass
    return {'results': out[:40], 'error': err}


def profile(sym, name=''):
    if sym in CATALOG_MAP:
        n, g = CATALOG_MAP[sym]
        return {'symbol': sym, 'name': n, 'group': g, 'isin': None, 'live': True}
    try:
        p = fh('stock/profile2', {'symbol': sym}, ttl=7 * 86400, persist=True)
    except Upstream:
        p = {}
    return {'symbol': sym, 'name': p.get('name') or '', 'isin': isin_for(sym, p.get('name') or name[:80]),
            'exchange': p.get('exchange'), 'industry': p.get('finnhubIndustry'), 'country': p.get('country'),
            'currency': p.get('currency'), 'marketCap': p.get('marketCapitalization'), 'ipo': p.get('ipo'),
            'web': p.get('weburl'), 'live': is_live(sym)}


def quotes(syms):
    out = {}
    for s in syms[:60]:
        try:
            q = fh('quote', {'symbol': s}, ttl=10)
            out[s] = q if q.get('c') else {'error': 'No quote for this symbol on the Finnhub free plan'}
        except Upstream as e:
            out[s] = {'error': str(e)}
    return out


# ---------- intraday bars built from the trade stream ----------
class Bars:
    def __init__(self):
        self.lock = threading.Lock()
        self.s, self.m = {}, {}

    def add(self, trades):
        with self.lock:
            for t in trades:
                sym, p, v, ts = t['s'], t['p'], t.get('v') or 0, t['t'] / 1000
                for store, res, cap in ((self.s, 1, 1800), (self.m, 60, 2880)):
                    bt = int(ts // res * res)
                    arr = store.setdefault(sym, [])
                    if arr and arr[-1][0] == bt:
                        b = arr[-1]
                        b[2], b[3], b[4], b[5] = max(b[2], p), min(b[3], p), p, b[5] + v
                    elif not arr or bt > arr[-1][0]:
                        arr.append([bt, p, p, p, p, v])
                        if len(arr) > cap:
                            del arr[0]

    def get(self, sym, res):
        with self.lock:
            return [list(b) for b in (self.s if res == 1 else self.m).get(sym, [])]


    def save(self, syms):
        with self.lock:
            data = {s: self.m.get(s, []) for s in syms if self.m.get(s)}
        tmp = BARS_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(data))
        tmp.replace(BARS_FILE)

    def load(self):
        try:
            data = json.loads(BARS_FILE.read_text())
        except (OSError, ValueError):
            return
        cutoff = time.time() - 2 * 86400
        with self.lock:
            for sym, arr in data.items():
                self.m[sym] = [b for b in arr if b[0] >= cutoff]


BARS_FILE = CONF / 'bars.json'
bars = Bars()

# ---------- futures panel: OANDA CFDs that track the index/commodity futures, streamed around the clock ----------
FUTURES = {  # symbol: (settlement timezone, hour, minute, trades at weekends, Yahoo symbol for the first-day fallback)
    'OANDA:SPX500_USD': ('America/New_York', 17, 0, False, 'ES=F'),
    'OANDA:NAS100_USD': ('America/New_York', 17, 0, False, 'NQ=F'),
    'OANDA:US30_USD': ('America/New_York', 17, 0, False, 'YM=F'),
    'OANDA:DE30_EUR': ('Europe/Berlin', 17, 30, False, '^GDAXI'),
    'OANDA:EU50_EUR': ('Europe/Berlin', 17, 30, False, '^STOXX50E'),
    'OANDA:JP225_USD': ('Asia/Tokyo', 15, 45, False, '^N225'),
    'OANDA:HK33_HKD': ('Asia/Hong_Kong', 16, 0, False, '^HSI'),
    'OANDA:XAU_USD': ('America/New_York', 17, 0, False, 'GC=F'),
    'OANDA:XAG_USD': ('America/New_York', 17, 0, False, 'SI=F'),
    'OANDA:WTICO_USD': ('America/New_York', 17, 0, False, 'CL=F'),
    'OANDA:BCO_USD': ('America/New_York', 17, 0, False, 'BZ=F'),
    'OANDA:EUR_USD': ('America/New_York', 17, 0, False, 'EURUSD=X'),
    'BINANCE:BTCEUR': ('UTC', 0, 0, True, 'BTC-EUR'),
}
PINNED = set(FUTURES)


def last_settle(tzname, hh, mm, weekends):
    try:
        tz = ZoneInfo(tzname) if tzname != 'UTC' else timezone.utc
    except Exception:  # noqa: BLE001
        tz = timezone.utc
    now = datetime.now(tz)
    cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if cand > now:
        cand -= timedelta(days=1)
    while not weekends and cand.weekday() >= 5:
        cand -= timedelta(days=1)
    return cand.timestamp()


# Settlement times per session, and the Yahoo counterpart of each OANDA CFD, for CFDs added from search.
SETTLE = {'globex': ('America/New_York', 17, 0, False), 'fx': ('America/New_York', 17, 0, False),
          'ice': ('America/New_York', 17, 0, False), 'grain': ('America/Chicago', 13, 20, False),
          'sugar': ('America/New_York', 13, 0, False), 'eurex': ('Europe/Berlin', 17, 30, False),
          'iceuk': ('Europe/London', 16, 30, False), 'euronext': ('Europe/Paris', 17, 30, False),
          'ose': ('Asia/Tokyo', 15, 45, False), 'hkfe': ('Asia/Hong_Kong', 16, 0, False)}
OANDA_YAHOO = {'US2000': 'RTY=F', 'XPT': 'PL=F', 'XPD': 'PA=F', 'XCU': 'HG=F', 'NATGAS': 'NG=F', 'CORN': 'ZC=F',
               'WHEAT': 'ZW=F', 'SOYBN': 'ZS=F', 'SUGAR': 'SB=F', 'UK100': '^FTSE', 'FR40': '^FCHI', 'NL25': '^AEX',
               'AU200': '^AXJO', 'JP225Y': '^N225', 'USB02Y': 'ZT=F', 'USB05Y': 'ZF=F', 'USB10Y': 'ZN=F', 'USB30Y': 'ZB=F'}


def oanda_spec(sym):
    """(tz, hh, mm, weekends, Yahoo symbol) for any OANDA instrument, pinned or not."""
    if sym in FUTURES:
        return FUTURES[sym]
    if not sym.startswith('OANDA:'):
        return None
    code = sym[6:]
    is_fx = bool(re.fullmatch(r'[A-Z]{3}_[A-Z]{3}', code)) and not code.startswith(OANDA_COMMOD)
    sess = 'fx' if is_fx else next((v for k, v in OANDA_SESS.items() if code.startswith(k)), 'globex')
    ysym = (code.replace('_', '') + '=X') if is_fx else next((v for k, v in OANDA_YAHOO.items() if code.startswith(k)), None)
    return (*SETTLE.get(sess, SETTLE['globex']), ysym)


def futures_refs(extra=()):
    """Day-change reference per futures proxy: its own price at the last settlement, else Yahoo's % move."""
    out = {}
    specs = dict(FUTURES)
    for s in extra:
        sp = oanda_spec(s)
        if sp and s not in specs:
            specs[s] = sp
    for sym, (tzname, hh, mm, wk, ysym) in specs.items():
        settle = last_settle(tzname, hh, mm, wk)
        arr = bars.get(sym, 60)
        before = [b for b in arr if b[0] < settle]
        if before and settle - before[-1][0] < 6 * 3600:
            out[sym] = {'ref': before[-1][4], 'refT': settle, 'source': 'stream'}
            continue
        if sym.startswith('BINANCE:'):  # Binance's daily candle opens exactly at 00:00 UTC
            key = 'bnday:' + sym
            ok, o = cache.get(key)
            if not ok:
                try:
                    k = json.loads(http_get('https://api.binance.com/api/v3/klines?' + urllib.parse.urlencode(
                        {'symbol': sym[8:], 'interval': '1d', 'limit': 1})))
                    o = float(k[-1][1]) if k else None
                except (Upstream, ValueError, IndexError, TypeError):
                    o = None
                cache.put(key, o, 600 if o else 60)
            if o:
                out[sym] = {'ref': o, 'refT': settle, 'source': 'binance'}
                continue
        r = yahoo_chart(ysym, {'interval': '1m', 'range': '1d'}, 600, 'yhref:' + ysym) if ysym else None
        meta = (r or {}).get('meta') or {}
        prev, last = meta.get('chartPreviousClose') or meta.get('previousClose'), meta.get('regularMarketPrice')
        if arr and prev and last:
            out[sym] = {'ref': arr[-1][4] * prev / last, 'refT': settle, 'source': 'yahoo'}
        elif prev:  # no CFD ticks yet (thin after hours): Yahoo's previous close as is
            out[sym] = {'ref': prev, 'refT': settle, 'source': 'yahoo'}
        else:
            out[sym] = {'ref': None, 'refT': settle, 'source': None,
                        'note': 'Yahoo is rate limiting right now' if (r or {}).get('busy') else None}
    return {'refs': out}


def yahoo_quote(ysym):
    """KOSPI and other instruments without a stream: Yahoo 1-minute chart, cached for two minutes."""
    r = yahoo_chart(ysym, {'interval': '1m', 'range': '1d'}, 120, 'yh1m:' + ysym)
    if not r or r.get('busy'):
        return {'error': 'Yahoo is rate limiting right now; retrying shortly' if r else 'No data from Yahoo', 'bars': []}
    meta = r.get('meta') or {}
    q = (r.get('indicators') or {}).get('quote', [{}])[0]
    bars_ = [[t, o, h, l, c, v or 0] for t, o, h, l, c, v in
             zip(r.get('timestamp') or [], q.get('open', []), q.get('high', []), q.get('low', []), q.get('close', []), q.get('volume', []))
             if None not in (o, h, l, c)]
    return {'last': meta.get('regularMarketPrice'), 'lastT': meta.get('regularMarketTime'),
            'prevClose': meta.get('chartPreviousClose') or meta.get('previousClose'),
            'high': meta.get('regularMarketDayHigh'), 'low': meta.get('regularMarketDayLow'),
            'open': bars_[0][1] if bars_ else None, 'bars': bars_, 'currency': meta.get('currency')}
yahoo_cooldown = [0.0]
YAHOO_BUSY = 'Yahoo is rate limiting right now; this retries automatically in a few minutes.'


def yahoo_symbol(sym):
    if sym.startswith('OANDA:'):
        a, _, b = sym[6:].partition('_')
        return f'{a}{b}=X' if len(a) == 3 and len(b) == 3 and a.isalpha() else None
    if ':' in sym:
        return None
    base, suffix = split_sym(sym)
    return base.replace('.', '-') if not suffix else sym.upper()


def yahoo_chart(ys, params, ttl, key):
    """One plain Yahoo chart request with caching and a shared 5-minute back-off after HTTP 429."""
    ok, v = cache.get(key)
    if ok:
        return v
    if time.time() < yahoo_cooldown[0]:
        return {'busy': True}
    try:
        j = json.loads(http_get(f'https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(ys)}?'
                                + urllib.parse.urlencode(params)))
        r = j['chart']['result'][0]
    except Upstream as e:
        if e.status == 429:
            yahoo_cooldown[0] = time.time() + 300
            return {'busy': True}
        r = None
    except (KeyError, IndexError, TypeError, ValueError):
        r = None
    cache.put(key, r, ttl if r else 600)
    return r


def yahoo_1m(sym):
    ys = yahoo_symbol(sym)
    if not ys:
        return [], 'No intraday history source for this instrument; the chart builds from live trades.'
    r = yahoo_chart(ys, {'interval': '1m', 'range': '1d', 'includePrePost': 'true'}, 120, 'yh1mx:' + ys)  # with pre-market and after hours
    if not r:
        return [], 'No intraday history for this symbol; the chart builds from live trades.'
    if r.get('busy'):
        return [], 'Intraday history (Yahoo) is rate limited right now; the chart builds from live trades.'
    try:
        q = r['indicators']['quote'][0]
        out = [[t, o, h, l, c, v or 0] for t, o, h, l, c, v in
               zip(r.get('timestamp') or [], q['open'], q['high'], q['low'], q['close'], q['volume'])
               if None not in (o, h, l, c)]
    except (KeyError, IndexError, TypeError):
        out = []
    return out, None if out else 'No intraday history for today yet; the chart builds from live trades.'


def yahoo_events(sym):
    ys = yahoo_symbol(sym)
    if not ys or '=' in ys:
        return [], [], None, None
    r = yahoo_chart(ys, {'interval': '1mo', 'range': '10y', 'events': 'div|split'}, 12 * 3600, 'yhev:' + ys)
    if not r:
        return [], [], None, 'No dividend or split history found.'
    if r.get('busy'):
        return [], [], None, YAHOO_BUSY
    ev = r.get('events') or {}
    divs = sorted(({'t': int(d['date']), 'amount': d['amount']} for d in (ev.get('dividends') or {}).values()),
                  key=lambda d: d['t'])
    splits = sorted(({'t': int(s['date']), 'ratio': s.get('splitRatio') or f"{s.get('numerator')}:{s.get('denominator')}",
                      'num': s.get('numerator'), 'den': s.get('denominator')}
                     for s in (ev.get('splits') or {}).values()), key=lambda s: s['t'])
    return divs, splits, (r.get('meta') or {}).get('currency'), None


def read_key(name):
    """Optional extra keys in ~/.config/lodestar/: oanda.key (token), alpaca.key ("KEY_ID:SECRET")."""
    try:
        return (CONF / name).read_text().strip() or None
    except OSError:
        return None


def iso_ts(s):
    return parse_date((s or '').replace('Z', '+00:00')[:19] + '+00:00') if s else None


def oanda_1m(sym):
    """1-minute mid candles for an OANDA instrument from a free OANDA demo (practice) account."""
    token = read_key('oanda.key')
    if not token:
        return None, None
    key = 'oa1m:' + sym
    ok, v = cache.get(key)
    if ok:
        return v, None
    url = (f'https://api-fxpractice.oanda.com/v3/instruments/{urllib.parse.quote(sym[6:])}/candles?'
           + urllib.parse.urlencode({'granularity': 'M1', 'count': 1440, 'price': 'M'}))
    try:
        j = json.loads(http_get(url, {'Authorization': f'Bearer {token}', 'Accept-Datetime-Format': 'UNIX'}, timeout=15))
    except Upstream as e:
        return [], f'OANDA history unavailable: {e}'
    except ValueError:
        return [], 'OANDA returned an unreadable response'
    out = []
    for c in j.get('candles') or []:
        m = c.get('mid') or {}
        try:
            out.append([int(float(c['time'])), float(m['o']), float(m['h']), float(m['l']), float(m['c']), c.get('volume') or 0])
        except (KeyError, TypeError, ValueError):
            continue
    cache.put(key, out, 60)
    return out, None


def alpaca_1m(sym):
    """Today's 1-minute bars for a US stock from Alpaca's free plan: SIP up to 16 minutes ago, IEX after that."""
    cred = read_key('alpaca.key')
    if not cred or ':' not in cred:
        return None, None
    key = 'al1m:' + sym
    ok, v = cache.get(key)
    if ok:
        return v, None
    kid, secret = cred.split(':', 1)
    hdr = {'APCA-API-KEY-ID': kid.strip(), 'APCA-API-SECRET-KEY': secret.strip()}
    now = datetime.now(timezone.utc)
    start = (now - timedelta(hours=20)).strftime('%Y-%m-%dT%H:%M:%SZ')
    cut = (now - timedelta(minutes=16)).strftime('%Y-%m-%dT%H:%M:%SZ')
    out = []
    try:
        for feed, a, b in (('sip', start, cut), ('iex', cut, None)):
            params = {'timeframe': '1Min', 'start': a, 'limit': 10000, 'feed': feed, 'adjustment': 'raw'}
            if b:
                params['end'] = b
            j = json.loads(http_get(f'https://data.alpaca.markets/v2/stocks/{urllib.parse.quote(sym)}/bars?'
                                    + urllib.parse.urlencode(params), hdr, timeout=15))
            for r in j.get('bars') or []:
                t = iso_ts(r.get('t'))
                if t:
                    out.append([int(t), r['o'], r['h'], r['l'], r['c'], r.get('v') or 0])
    except Upstream as e:
        return [], f'Alpaca history unavailable: {e}'
    except (ValueError, KeyError, TypeError):
        return [], 'Alpaca returned an unreadable response'
    out.sort()
    cache.put(key, out, 60)
    return out, None


def yahoo_scaled(sym):
    """No OANDA key: the future's Yahoo minute bars, scaled to the CFD's price level."""
    ysym = (oanda_spec(sym) or (None,) * 5)[4]
    if not ysym:
        return [], 'No intraday history source for this CFD; the chart builds from live prices.'
    r = yahoo_chart(ysym, {'interval': '1m', 'range': '2d'}, 120, 'yh1m2:' + ysym)  # two days: covers the overnight session
    if not r or r.get('busy'):
        return [], 'Intraday history (Yahoo) is rate limited right now; the chart builds from live prices.'
    try:
        q = r['indicators']['quote'][0]
        yb = [[t, o, h, l, c, v or 0] for t, o, h, l, c, v in
              zip(r.get('timestamp') or [], q['open'], q['high'], q['low'], q['close'], q['volume'])
              if None not in (o, h, l, c)]
    except (KeyError, IndexError, TypeError):
        yb = []
    live = bars.get(sym, 60)
    if not yb:
        return [], 'No intraday history from Yahoo for this future.'
    if not live:
        return [], 'Waiting for the first CFD price to line up the history.'
    by_t = {b[0]: b[4] for b in yb}
    pair = next(((by_t[b[0]], b[4]) for b in reversed(live) if b[0] in by_t), None) or (yb[-1][4], live[-1][4])
    k = pair[1] / pair[0]
    return [[b[0], b[1] * k, b[2] * k, b[3] * k, b[4] * k, b[5]] for b in yb], None


def history(sym):
    live_m, live_s = bars.get(sym, 60), bars.get(sym, 1)
    back, note, source = [], None, None
    if sym.startswith('BINANCE:'):
        pair = sym[8:]
        key = 'bn:' + pair
        ok, v = cache.get(key)
        if ok:
            back = v
        else:
            try:
                kl = json.loads(http_get('https://api.binance.com/api/v3/klines?'
                                         + urllib.parse.urlencode({'symbol': pair, 'interval': '1m', 'limit': 720})))
                back = [[k[0] // 1000, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])] for k in kl]
                cache.put(key, back, 30)
            except (Upstream, ValueError, TypeError) as e:
                note = f'Binance history unavailable: {e}'
        source = 'Binance'
    elif sym.startswith('OANDA:'):
        back, note = oanda_1m(sym)
        if back is not None and not note:
            source = 'OANDA'
        elif oanda_spec(sym):
            back, note2 = yahoo_scaled(sym)
            note = note2 if not note else note
            source = 'Yahoo futures, scaled to the CFD' if back else None
        else:
            back, note = yahoo_1m(sym)
            source = 'Yahoo Finance' if back else None
    else:
        back, note = (alpaca_1m(sym) if is_live(sym) else (None, None))
        if back:
            source = 'Alpaca'
        else:
            back, note2 = yahoo_1m(sym)
            note = note or note2
            source = 'Yahoo Finance' if back else None
    back = back or []
    if live_m:
        # Recorded bars win; backfill fills every gap, also holes left while the Mac was asleep.
        covered = {b[0] for b in live_m}
        merged = sorted(live_m + [b for b in back if b[0] not in covered and b[0] <= live_m[-1][0]], key=lambda b: b[0])
    else:
        merged = back
    return {'m': merged, 's': live_s, 'note': note, 'source': source}


# ---------- news: RSS/Atom feeds + Finnhub categories in one store ----------
def tag(el):
    return el.tag.rsplit('}', 1)[-1]


def clean(s, limit=None):
    s = html.unescape(s or '')
    s = re.sub(r'<[^>]+>', ' ', s)
    s = re.sub(r'\s+', ' ', html.unescape(s)).strip()
    if limit and len(s) > limit:
        s = s[:limit].rsplit(' ', 1)[0] + '…'
    return s


def parse_date(s):
    s = (s or '').strip()
    if not s:
        return None
    try:
        d = email.utils.parsedate_to_datetime(s)
    except (TypeError, ValueError, IndexError):
        try:
            d = datetime.fromisoformat(s.replace('Z', '+00:00'))
        except ValueError:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.timestamp()


def parse_feed(raw):
    root = ET.fromstring(raw)
    out = []
    for el in root.iter():
        if tag(el) not in ('item', 'entry'):
            continue
        kids = {}
        link = ''
        for c in el:
            t = tag(c)
            kids.setdefault(t, c)
            if t == 'link' and not link:
                href = c.get('href')
                if href and c.get('rel', 'alternate') == 'alternate':
                    link = href
                elif (c.text or '').strip().startswith('http'):
                    link = c.text.strip()
        if not link and 'guid' in kids and (kids['guid'].text or '').strip().startswith('http'):
            link = kids['guid'].text.strip()
        title = clean(''.join(kids['title'].itertext()) if 'title' in kids else '')
        if not title or not link.startswith('http'):
            continue
        # Elements without children are falsy, so pick by presence rather than with `or`.
        summ_el = next((kids[k] for k in ('description', 'summary', 'encoded', 'content') if k in kids), None)
        summary = clean(''.join(summ_el.itertext()) if summ_el is not None else '', 300)
        date_el = next((kids[k] for k in ('pubDate', 'published', 'updated', 'date') if k in kids), None)
        out.append({'title': title, 'url': link, 'summary': summary if summary != title else '',
                    'ts': parse_date(date_el.text if date_el is not None else '')})
    return out


class Feed:
    def __init__(self, name, url, cat, region, every):
        self.name, self.url, self.cat, self.region, self.every = name, url, cat, region, every
        self.etag = self.lastmod = None
        self.next_due, self.busy = 0.0, False
        self.status, self.error, self.items, self.last_ok, self.newest = 'pending', None, 0, None, None


class NewsStore:
    MAX_AGE = 4 * 86400

    def __init__(self):
        self.lock = threading.Lock()
        self.items, self.by_title = {}, {}

    def add(self, feed, items):
        now = time.time()
        added = 0
        first_poll = feed.last_ok is None
        with self.lock:
            for n, it in enumerate(items):
                # Undated items from a feed's first fetch are backlog, not breaking news.
                ts = it.get('ts') or (now - 6 * 3600 - n * 60 if first_poll else now)
                if ts > now + 3600:
                    ts = now
                if now - ts > self.MAX_AGE:
                    continue
                if not lang_ok(it['title'] + ' ' + (it.get('summary') or '')):
                    continue
                key = re.sub(r'[^a-z0-9]+', '', it['title'].lower())[:120]
                iid = hashlib.sha1(it['url'].encode()).hexdigest()[:14]
                old = self.items.get(self.by_title.get(key) or iid)
                if old:
                    old['cats'].add(feed.cat)
                    old['regions'].add(feed.region)
                    continue
                source = it.get('source') or feed.name
                self.items[iid] = {'id': iid, 't': ts, 'title': it['title'], 'url': it['url'],
                                   'summary': it.get('summary') or '', 'source': source, 'feed': feed.name,
                                   'cats': {feed.cat}, 'regions': {feed.region}, 'related': it.get('related') or '',
                                   'ca': classify(it['title'], it.get('summary', '') if feed.cat == 'wires' else ''),
                                   'hot': it.get('hot') or (feed.cat == 'wires' and reporter_result(it['title'])) or hot_score(it['title'])}
                self.by_title[key] = iid
                added += 1
            if len(self.items) > 6000 or added:
                cutoff = now - self.MAX_AGE
                for k in [k for k, v in self.items.items() if v['t'] < cutoff]:
                    del self.items[k]
                self.by_title = {k: v for k, v in self.by_title.items() if v in self.items}
        return added

    def query(self, tab='all', q='', matcher=None, limit=200):
        words = [w for w in q.lower().split() if w]
        with self.lock:
            items = list(self.items.values())
        out = []
        for it in sorted(items, key=lambda x: -x['t']):
            if tab == 'ca' and not it['ca']:
                continue
            if tab in REGION_GROUPS and not (it['regions'] & REGION_GROUPS[tab]):
                continue
            if tab not in ('all', 'ca', 'selected') and tab not in REGION_GROUPS:
                cats = {'markets': {'markets', 'companies'}}.get(tab, {tab})
                if not (it['cats'] & cats):
                    continue
            hay = (it['title'] + ' ' + it['summary']).lower()
            if words and not all(w in hay for w in words):
                continue
            if matcher and not matcher(it['title'] + ' ' + it['summary'] + ' ' + it['related']):
                continue
            out.append(public_item(it))
            if len(out) >= limit:
                break
        return out


def public_item(it):
    score, why = it.get('hot') or (0, [])
    key = title_key(it['title'])
    return {**it, 'cats': sorted(it['cats']), 'regions': sorted(it['regions']),
            'hot': score >= HOT_MIN or key in HOT_KEYS, 'hotWhy': (why or HOT_KEYS.get(key) or [None])[0]}


def title_key(t):
    return re.sub(r'[^a-z0-9]+', '', (t or '').lower())[:80]


HOT_KEYS = {}  # title key -> reasons, for headlines that only became hot through wide coverage


news_store = NewsStore()
feeds = [Feed(*f) for f in FEEDS]


def poll_feed(f):
    try:
        if f.url.startswith('finnhub:'):
            raw = fh('news', {'category': f.url.split(':', 1)[1]}, ttl=60)
            items = [{'title': clean(n.get('headline')), 'url': n.get('url') or '', 'summary': clean(n.get('summary'), 300),
                      'ts': n.get('datetime'), 'source': n.get('source'), 'related': n.get('related')}
                     for n in raw if n.get('headline') and (n.get('url') or '').startswith('http')]
        else:
            hdr = {'User-Agent': FEED_UA, 'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml'}
            if f.etag:
                hdr['If-None-Match'] = f.etag
            if f.lastmod:
                hdr['If-Modified-Since'] = f.lastmod
            req = urllib.request.Request(f.url, headers=hdr)
            try:
                with urllib.request.urlopen(req, timeout=15) as r:
                    raw = r.read(5_000_000)
                    f.etag, f.lastmod = r.headers.get('ETag'), r.headers.get('Last-Modified')
            except urllib.error.HTTPError as e:
                if e.code == 304:
                    f.status, f.error, f.last_ok = 'ok', None, time.time()
                    f.next_due = time.time() + f.every
                    return
                raise Upstream(e.code, f'HTTP {e.code}') from None
            items = parse_feed(raw)
        news_store.add(f, items)
        stamps = [i['ts'] for i in items if i.get('ts')]
        f.items, f.newest = len(items), max(stamps) if stamps else f.newest
        f.status, f.error, f.last_ok = 'ok', None, time.time()
        f.next_due = time.time() + f.every
    except Exception as e:  # noqa: BLE001 - one bad feed must not stop the others
        f.status, f.error = 'error', str(e)[:160]
        f.next_due = time.time() + min(f.every * 3, 3600)
    finally:
        f.busy = False


def feed_loop():
    pool = concurrent.futures.ThreadPoolExecutor(6, thread_name_prefix='feed')
    for i, f in enumerate(feeds):
        f.next_due = time.time() + i * 0.4
    while True:
        now = time.time()
        for f in feeds:
            if not f.busy and f.next_due <= now:
                f.busy = True
                pool.submit(poll_feed, f)
        time.sleep(1)


def feed_status():
    return {'feeds': [{'name': f.name, 'cat': f.cat, 'region': f.region, 'status': f.status, 'error': f.error,
                       'items': f.items, 'lastOk': f.last_ok, 'newest': f.newest,
                       'url': f.url if f.url.startswith('http') else 'https://finnhub.io' if f.url.startswith('finnhub') else 'https://news.google.com'}
                      for f in feeds + [watch_feed, sweep_feed]]}


GENERIC_FIRST = {'advanced', 'american', 'applied', 'general', 'united', 'first', 'national', 'international', 'global',
                 'digital', 'new', 'royal', 'standard', 'southern', 'northern', 'western', 'eastern', 'pacific', 'atlantic',
                 'central', 'consolidated', 'universal', 'public', 'capital', 'energy', 'power', 'bank', 'deutsche', 'british',
                 'canadian', 'japan', 'china', 'great', 'golden', 'smart', 'super', 'micro', 'meta', 'data', 'west', 'east'}


def core_name(name):
    core = [w for w in re.sub(r'[^\w&.\-]+', ' ', name or '').split() if norm_name(w)]
    if not core:
        return ''
    if core[0].lower() in GENERIC_FIRST and len(core) > 1:  # "Advanced Micro Devices", not "Advanced"
        return ' '.join(core[:3])
    key = core[0] if len(core[0]) >= 4 or len(core) == 1 else ' '.join(core[:2])
    return key if len(key) >= 3 else ''


def name_matcher(sym, name):
    pats = []
    key = core_name(name)
    if key:
        pats.append(re.compile(r'(?<!\w)' + re.escape(key) + r'(?!\w)', re.I))
    base = '' if ':' in sym else split_sym(sym)[0]
    if len(base) >= 3 and base.isalpha():
        pats.append(re.compile(r'(?<![A-Za-z$])\$?' + re.escape(base) + r'(?![A-Za-z])'))
    if not pats:
        return None
    return lambda text: any(p.search(text) for p in pats)


def news(tab, q, sym, name):
    if tab == 'hot':
        items = hot_items()
        if q:
            ws = q.lower().split()
            items = [i for i in items if all(w in (i['title'] + ' ' + i['source']).lower() for w in ws)]
        return {'items': items}
    hot_items()  # keeps the coverage-based hot marks fresh for the other tabs
    if tab == 'selected':
        if not sym:
            return {'items': []}
        items = news_store.query('selected', q, name_matcher(sym, name), limit=120)
        if is_live(sym) and ':' not in sym:
            today = datetime.now(timezone.utc).date()
            try:
                raw = fh('company-news', {'symbol': sym, 'from': str(today - timedelta(days=4)), 'to': str(today)}, ttl=180)
            except Upstream:
                raw = []
            seen = {i['url'] for i in items}
            for n in raw[:80]:
                if not n.get('headline') or n.get('url') in seen:
                    continue
                title = clean(n['headline'])
                if not lang_ok(title + ' ' + clean(n.get('summary'), 300)):
                    continue
                items.append({'id': f"f{n.get('id')}", 't': n.get('datetime') or 0, 'title': title, 'url': n.get('url'),
                              'summary': clean(n.get('summary'), 300), 'source': n.get('source') or 'Finnhub',
                              'feed': 'Finnhub company news', 'cats': ['companies'], 'regions': ['US'],
                              'related': n.get('related') or '', 'ca': classify(title),
                              'hot': hot_score(title)[0] >= HOT_MIN, 'hotWhy': (hot_score(title)[1] or [None])[0]})
            items.sort(key=lambda i: -i['t'])
        return {'items': items[:150]}
    return {'items': news_store.query(tab, q)}


# ---------- corporate actions ----------
FORM_LABELS = {
    '8-K': ('Event', 'Current report: material event'), '8-K/A': ('Event', 'Amended current report'),
    '6-K': ('Event', 'Foreign issuer report'), '10-K': ('Report', 'Annual report'), '10-Q': ('Report', 'Quarterly report'),
    '20-F': ('Report', 'Annual report (foreign issuer)'), '40-F': ('Report', 'Annual report (Canadian issuer)'),
    'DEF 14A': ('Meeting', 'Proxy statement: shareholder meeting'), 'DEFA14A': ('Meeting', 'Additional proxy material'),
    'PRE 14A': ('Meeting', 'Preliminary proxy statement'), 'DEFM14A': ('M&A', 'Merger proxy statement'),
    'PREM14A': ('M&A', 'Preliminary merger proxy'), 'S-4': ('M&A', 'Registration for merger or exchange'),
    '425': ('M&A', 'Merger communication'), 'SC TO-T': ('Tender offer', 'Third-party tender offer'),
    'SC TO-I': ('Tender offer', 'Issuer tender offer (buyback)'), 'SC TO-C': ('Tender offer', 'Tender offer communication'),
    'SC 14D9': ('Tender offer', 'Board response to tender offer'), 'SC 13E3': ('Delisting', 'Going-private transaction'),
    'SC 13D': ('Stake', 'Activist stake above 5%'), 'SC 13D/A': ('Stake', 'Activist stake amended'),
    'SC 13G': ('Stake', 'Passive stake above 5%'), 'SC 13G/A': ('Stake', 'Passive stake amended'),
    'S-1': ('IPO / listing', 'IPO registration'), 'F-1': ('IPO / listing', 'IPO registration (foreign issuer)'),
    'S-3': ('Capital raise', 'Shelf registration'), 'S-3ASR': ('Capital raise', 'Automatic shelf registration'),
    'F-3': ('Capital raise', 'Shelf registration (foreign issuer)'), 'S-8': ('Capital raise', 'Employee share plan registration'),
    '424B2': ('Capital raise', 'Prospectus: securities offering'), '424B3': ('Capital raise', 'Prospectus'),
    '424B4': ('Capital raise', 'Final prospectus'), '424B5': ('Capital raise', 'Prospectus supplement: offering'),
    '424B7': ('Capital raise', 'Prospectus supplement'), 'FWP': ('Capital raise', 'Free writing prospectus (offering terms)'),
    '25-NSE': ('Delisting', 'Exchange delisting notice'), '25': ('Delisting', 'Delisting'),
    '15-12B': ('Delisting', 'Deregistration'), '15-12G': ('Delisting', 'Deregistration'), '15-15D': ('Delisting', 'Suspension of reporting'),
    '8-A12B': ('IPO / listing', 'Registration for exchange listing'), 'SD': ('Report', 'Conflict minerals report'),
    'ARS': ('Report', 'Annual report to shareholders'), '11-K': ('Report', 'Employee plan annual report'),
    'NPORT-P': ('Report', 'Fund portfolio holdings'), 'N-CSR': ('Report', 'Fund shareholder report'),
    'N-CSRS': ('Report', 'Fund semi-annual report'), 'N-30D': ('Report', 'Fund report to shareholders'),
    'N-CEN': ('Report', 'Fund annual census'), '485BPOS': ('Report', 'Fund prospectus update'),
    '497': ('Report', 'Fund prospectus supplement'), '497K': ('Report', 'Fund summary prospectus'),
    '24F-2NT': ('Report', 'Fund share registration notice'), 'N-PX': ('Meeting', 'Fund proxy voting record'),
}
SKIP_FORMS = {'3', '4', '5', '3/A', '4/A', '5/A', '144', '144/A', '13F-HR', '13F-NT', 'CORRESP', 'UPLOAD', 'CERT', 'IRANNOTICE'}
INSIDER_CODES = {'P': 'Buy', 'S': 'Sell', 'M': 'Option exercise', 'A': 'Award', 'F': 'Tax withholding', 'G': 'Gift',
                 'D': 'Disposition to issuer', 'C': 'Conversion', 'X': 'Option exercise', 'J': 'Other'}


def actions(sym, name):
    today = datetime.now(timezone.utc).date()
    out = {'symbol': sym, 'events': [], 'notes': [], 'insiders': [], 'eps': [], 'rec': None, 'next': None,
           'divs': [], 'splits': [], 'currency': None}
    us = ':' not in sym and is_live(sym)

    def safe(fn, label):
        try:
            return fn()
        except Upstream as e:
            out['notes'].append(f'{label}: {e}')
            return None

    if us:
        filings = safe(lambda: fh('stock/filings', {'symbol': sym, 'from': str(today - timedelta(days=500)), 'to': str(today)},
                                  ttl=3600), 'SEC filings') or []
        for f in filings:
            form = (f.get('form') or '').strip()
            if form in SKIP_FORMS:
                continue
            kind, label = FORM_LABELS.get(form) or (('Capital raise', 'Prospectus') if form.startswith('424B') else ('Filing', ''))
            ts = parse_date((f.get('acceptedDate') or f.get('filedDate') or '').replace(' ', 'T'))
            out['events'].append({'t': ts or 0, 'kind': kind, 'title': f'{form}' + (f' · {label}' if label else ''),
                                  'url': f.get('reportUrl') or f.get('filingUrl'), 'source': 'SEC EDGAR via Finnhub'})
        ins = safe(lambda: fh('stock/insider-transactions', {'symbol': sym, 'from': str(today - timedelta(days=120)),
                                                             'to': str(today)}, ttl=3600), 'Insider transactions') or {}
        for d in (ins.get('data') or [])[:200]:
            out['insiders'].append({'name': d.get('name'), 'date': d.get('transactionDate'), 'filed': d.get('filingDate'),
                                    'code': d.get('transactionCode'), 'what': INSIDER_CODES.get(d.get('transactionCode'), d.get('transactionCode')),
                                    'change': d.get('change'), 'price': d.get('transactionPrice'), 'held': d.get('share'),
                                    'derivative': d.get('isDerivative')})
        out['eps'] = (safe(lambda: fh('stock/earnings', {'symbol': sym}, ttl=6 * 3600), 'Earnings history') or [])[:8]
        rec = safe(lambda: fh('stock/recommendation', {'symbol': sym}, ttl=6 * 3600), 'Analyst recommendations') or []
        out['rec'] = rec[0] if rec else None
        cal = safe(lambda: fh('calendar/earnings', {'symbol': sym, 'from': str(today), 'to': str(today + timedelta(days=150))},
                              ttl=6 * 3600), 'Earnings calendar') or {}
        upcoming = sorted(cal.get('earningsCalendar') or [], key=lambda e: e.get('date') or '')
        out['next'] = upcoming[0] if upcoming else None
    divs, splits, ccy, note = yahoo_events(sym)
    out['divs'], out['splits'], out['currency'] = divs, splits, ccy
    if note:
        out['notes'].append('Dividends and splits: ' + note)
    for d in divs:
        out['events'].append({'t': d['t'], 'kind': 'Dividend', 'title': f"Dividend {d['amount']:g} {ccy or ''} per share (ex-date)".replace('  ', ' '),
                              'url': None, 'source': 'Yahoo Finance'})
    for s in splits:
        out['events'].append({'t': s['t'], 'kind': 'Split', 'title': f"Stock split {s['ratio']}", 'url': None, 'source': 'Yahoo Finance'})
    m = name_matcher(sym, name)
    if m:
        heads, seen_titles = [], set()
        live = [{'t': it['t'], 'kind': it['ca'], 'title': it['title'], 'url': it['url'], 'source': it['source']}
                for it in news_store.query('ca', '', m, limit=60)]
        found = []
        core = core_name(name)
        if core:
            for it in gnews_ca(core):
                k = classify(it['title'])
                if k and m(it['title']) and lang_ok(it['title']):
                    found.append({**it, 'kind': k})
        for h in live + found:
            if core and not about_company(h['title'], core):
                continue
            if h['title'].count('|') >= 2:  # stock-portal quote pages ("Aktie | Kurs | Dividende | ..."), not events
                continue
            key = re.sub(r'[^a-z0-9]+', '', h['title'].lower())[:80]
            if key not in seen_titles:
                seen_titles.add(key)
                heads.append(h)
        events = cluster_events(heads, core)
        out['events'].extend(events)
        recent = [e for e in events if e['kind'] in MAJOR_KINDS and time.time() - e['t'] < 120 * 86400]
        out['highlight'] = max(recent, key=lambda e: (e['reports'] >= 2, e['t'])) if recent else None
        out['coverage'] = f'{len(heads)} corporate-action headlines from Google News and the live feeds, last 12 months'
    out['events'].sort(key=lambda e: -(e['t'] or 0))
    return out


GN_TERMS = {
    'de': ('Abspaltung OR Ausgliederung OR Dividende OR Aktiensplit OR Kapitalerhöhung OR Übernahme OR Übernahmeangebot '
           'OR Aktienrückkauf OR Hauptversammlung OR Delisting OR Squeeze-out OR Börsengang OR Erstnotiz OR Umfirmierung',
           {'hl': 'de', 'gl': 'DE', 'ceid': 'DE:de'}),
    'en': ('spin-off OR spinoff OR demerger OR dividend OR "stock split" OR "rights issue" OR acquisition OR merger '
           'OR "tender offer" OR buyback OR "capital increase" OR delisting OR IPO OR "annual general meeting"',
           {'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'}),
}


GN_SECTIONS = {'top': '', 'business': '/headlines/section/topic/BUSINESS', 'world': '/headlines/section/topic/WORLD',
               'nation': '/headlines/section/topic/NATION', 'technology': '/headlines/section/topic/TECHNOLOGY'}


def mark_hot(items):
    for it in items:
        sc, why = it.get('_hot') or hot_score(it['title'])
        key = title_key(it['title'])
        it['hot'] = sc >= HOT_MIN or key in HOT_KEYS
        it['hotWhy'] = (why or HOT_KEYS.get(key) or [None])[0]
    return [{k: v for k, v in it.items() if k != '_hot'} for it in items]


HOT_CACHE = {'t': 0, 'items': []}
HOT_STOP = set('the and for with from that this after over into will says said amid what about their have more than '
               'der die das und mit für von den dem nach nicht eine einen einer dass sich sind wird über auch beim'.split())


def earnings_company(title):
    head = re.sub(r'-(Aktie|Aktien|Konzern|Chef)\b', r' \1', title[:70])  # "AMD-Aktie" -> "AMD Aktie"
    for rx, sym in REPORTERS:
        if rx.search(head):
            return sym.lower()
    m = BIG_COMPANIES.search(head)
    if not m:
        return None
    co = m.group(1).lower()
    return {'saudi aramco': 'aramco', 'nvidia': 'nvidia', 'vw': 'volkswagen', 'eon': 'e.on', 'münchener rück': 'munich re',
            'google': 'alphabet', 'nestle': 'nestlé', 'advanced micro devices': 'amd', 'thyssenkrupp': 'thyssenkrupp'}.get(co, co)


def merge_earnings(items):
    """One Hot entry per company's results: its first report (often the press release), plus the others as 'more'."""
    groups, out = {}, []
    for it in sorted(items, key=lambda i: i['t']):
        co = earnings_company(it['title']) if it.get('hotWhy') == 'Earnings' else None
        g = groups.get(co) if co else None
        if g and it['t'] - g['t'] < 16 * 3600:  # one reporting day, one entry
            g['more'].append({'title': it['title'], 'url': it['url'], 'source': it['source'], 't': it['t']})
            continue
        it = {**it, 'more': []}
        if co:
            groups[co] = it
        out.append(it)
    return out


def hot_items():
    """Market-moving headlines of the last 36 hours from all feeds plus Google News, cached for a minute.
    A headline scoring 2–3 also becomes hot when at least three other outlets carry the same story."""
    if time.time() - HOT_CACHE['t'] < 20:
        return HOT_CACHE['items']
    now = time.time()
    pool = [i for i in news_store.query('all', limit=3000) if now - i['t'] < 36 * 3600]
    for sec in ('top', 'business', 'world', 'nation'):
        try:
            for it in gnews_section(sec)['items']:
                if now - it['t'] < 36 * 3600:
                    pool.append({**it, 'hot': it.get('_hot')})
        except (Upstream, ET.ParseError):
            pass
    toks = []
    index = {}
    for n, it in enumerate(pool):
        ws = {w for w in re.findall(r'[a-zäöüß0-9]{4,}', it['title'].lower()) if w not in HOT_STOP}
        toks.append(ws)
        for w in ws:
            index.setdefault(w, set()).add(n)
    seen, out, keys = set(), [], {}
    for n, it in enumerate(pool):
        sc, why = it['hot'] if isinstance(it['hot'], tuple) else hot_score(it['title'])
        key = title_key(it['title'])
        if key[:45] in seen:  # same story with a ticker suffix or from a syndication partner
            continue
        if 2 <= sc < HOT_MIN and why:
            shared = {}
            for w in toks[n]:
                for m in index[w]:
                    if m != n:
                        shared[m] = shared.get(m, 0) + 1
            outlets = {pool[m]['source'] for m, c in shared.items() if c >= 3}
            if len(outlets) >= 3:
                sc += 2
                keys[key] = why
        if sc >= HOT_MIN:
            seen.add(key[:45])
            out.append({**{k: v for k, v in it.items() if k not in ('hot', '_hot', 'cats', 'regions')},
                        'cats': sorted(it['cats']) if isinstance(it.get('cats'), set) else it.get('cats', []),
                        'regions': sorted(it['regions']) if isinstance(it.get('regions'), set) else it.get('regions', []),
                        'hot': True, 'hotWhy': why[0] if why else None})
    out = merge_earnings(out)
    out.sort(key=lambda i: -i['t'])
    HOT_KEYS.clear()
    HOT_KEYS.update(keys)
    HOT_CACHE.update(t=now, items=out[:150])
    return HOT_CACHE['items']


def gnews_section(section):
    """Google News, German edition (the non-personalised home page and its sections), cached 2 minutes."""
    path = GN_SECTIONS.get(section, '')
    key = 'gnsec:' + section
    ok, v = cache.get(key)
    if ok:
        return v
    url = f'https://news.google.com/rss{path}?hl=de&gl=DE&ceid=DE:de'
    root = ET.fromstring(http_get(url, {'User-Agent': FEED_UA}, timeout=15))
    items = []
    for it in root.iter('item'):
        title, src = it.findtext('title') or '', it.findtext('source') or ''
        if src and title.endswith(' - ' + src):
            title = title[:-len(src) - 3]
        link = it.findtext('link') or ''
        if title and link.startswith('http'):
            ts = parse_date(it.findtext('pubDate'))
            items.append({'id': hashlib.sha1(link.encode()).hexdigest()[:14], 't': ts or time.time(), 'title': clean(title),
                          'url': link, 'source': src or 'Google News', 'summary': '', 'regions': ['DE'], 'related': ''})
    v = {'items': items}
    for it in items:
        sc, why = hot_score(it['title'])
        it['_hot'] = (sc, why)
    cache.put(key, v, 120)
    return v


def gnews_public(section):
    hot_items()  # refresh coverage-based hot marks
    return {'items': mark_hot([dict(i) for i in gnews_section(section)['items']])}


LOW_QUALITY = re.compile(r'ad-hoc-news', re.I)  # automated daily digests that blur separate events


def gnews_ca(core):
    """Twelve months of corporate-action coverage for one company from Google News search (RSS), cached 6 h."""
    out = []
    for lang, (terms, loc) in GN_TERMS.items():
        key = f'gn:{lang}:{core.lower()}'
        ok, v = cache.get(key)
        if not ok:
            url = 'https://news.google.com/rss/search?' + urllib.parse.urlencode({'q': f'"{core}" ({terms}) when:365d', **loc})
            try:
                root = ET.fromstring(http_get(url, {'User-Agent': FEED_UA}, timeout=15))
                v = []
                for it in root.iter('item'):
                    title, src = it.findtext('title') or '', it.findtext('source') or ''
                    if src and title.endswith(' - ' + src):
                        title = title[:-len(src) - 3]
                    link = it.findtext('link') or ''
                    if LOW_QUALITY.search(src):
                        continue
                    if title and link.startswith('http'):
                        v.append({'title': clean(title), 'url': link, 'source': src, 't': parse_date(it.findtext('pubDate'))})
                cache.put(key, v, 6 * 3600)
            except (Upstream, ET.ParseError):
                v = []
                cache.put(key, v, 600)
        out.extend(v)
    return out


MAJOR_OUTLETS = re.compile(r'reuters|bloomberg|dpa|handelsblatt|börsen-zeitung|faz|frankfurter|wirtschaftswoche|zeit|manager magazin|'
                           r'spiegel|tagesschau|süddeutsche|financial times|wall street journal|wsj|cnbc|associated press|afp|nzz', re.I)
MAJOR_KINDS = ('Spin-off', 'M&A', 'Tender offer', 'Delisting', 'Capital raise', 'Split', 'IPO / listing', 'Name change')


EVENT_STOP = set("""spin spinoff spinoffs abspaltung abspaltungen aktie aktien shares stock stocks with from that this after over
into will says said nach nicht eine einen einer dass sich sind wird mehr unit units division sparte börse market markets
trading listing debut börsendebüt company konzern group plans plan prepares begins dividend dividende merger übernahme
acquisition billion milliarden million euro dollar price kurs value""".split())


ALLOW_AFTER_NAME = {'ag', 'se', 'inc', 'inc.', 'corp', 'corp.', 'co', 'plc', 'nv', 'sa', 'group', 'aktie', 'aktien', 'konzern',
                    'ceo', 'chef', 'vorstand', 'shares', 'share', 'stock', 'stocks', 'aktionäre', 'shareholders', 'says',
                    'sagt', 'will', 'to', 'and', 'und', 'plans', 'plant', 'is', 'ist', 'has', 'hat', 'reports', 'meldet'}


SENTENCE_START = {'why', 'how', 'what', 'is', 'are', 'can', 'should', 'will', 'the', 'a', 'an', 'watch', 'buy', 'sell',
                  'hold', 'breaking', 'update', 'exclusive', 'analysis', 'opinion', 'stock', 'stocks', 'shares', 'warum',
                  'wie', 'was', 'die', 'der', 'das', 'aktie', 'nach', 'bei', 'für'}


def about_company(title, core):
    """The headline is about this company: its name comes early, and is not the start of another name (Siemens Energy)."""
    m = re.search(r'(?<!\w)' + re.escape(core) + r'(?!\w)', title, re.I)
    if not m or m.start() > 50:
        return False
    if core.split()[0].lower() in GENERIC_FIRST:  # "Portland General Electric" is not General Electric
        prev = re.search(r"([A-ZÄÖÜ][\w.&'-]*)\s+$", title[:m.start()])
        if prev and prev.group(1).lower() not in SENTENCE_START:
            return False
    rest = title[m.end():]
    if re.match(r'-[A-ZÄÖÜ][\w.]*-\w', rest):  # Siemens-Energy-Aktie: another company; Siemens-Aktie is fine
        return False
    nxt = re.match(r"(?:'s)?\s+([A-ZÄÖÜ][\w.]*)", rest)
    return not nxt or nxt.group(1).lower() in ALLOW_AFTER_NAME


def cluster_events(heads, core=''):
    """Many outlets reporting the same action become one event: same kind, reports no more than 2 days apart."""
    core_lc = core.lower()
    clusters = []
    fam = lambda k: 'Spin-off' if k in ('Spin-off', 'IPO / listing') else k  # a spin-off's first listing is part of it
    for h in sorted((h for h in heads if h.get('t')), key=lambda h: h['t']):
        c = next((c for c in reversed(clusters) if fam(c['kind']) == fam(h['kind']) and h['t'] - c['last'] <= 2 * 86400), None)
        if c:
            c['reports'].append(h)
            c['last'] = h['t']
        else:
            clusters.append({'kind': h['kind'], 'first': h['t'], 'last': h['t'], 'reports': [h]})
    out = []
    for c in clusters:
        reps = c['reports']
        if any(r['kind'] == 'Spin-off' for r in reps):
            c['kind'] = 'Spin-off'
        # Date: the day most reports came out. Headline: from that day, a major outlet first, shorter titles first.
        days = {}
        for r in reps:
            day = int(r['t'] // 86400)
            days[day] = days.get(day, 0) + 1
        top_day = max(days, key=lambda dd: (days[dd], -dd))
        on_day = [r for r in reps if int(r['t'] // 86400) == top_day]
        event_t = min(r['t'] for r in on_day)
        # Favour titles naming what the event is about: the word most titles in the group share (e.g. "accelis").
        freq = {}
        for r in reps:
            for w in set(re.findall(r'[a-zäöüß]{4,}', r['title'].lower())) - EVENT_STOP - {core_lc}:
                freq[w] = freq.get(w, 0) + 1
        top = {w: n for w, n in sorted(freq.items(), key=lambda kv: -kv[1])[:3] if n >= 2}
        first = min(on_day, key=lambda r: (-sum(top.get(w, 0) for w in set(re.findall(r'[a-zäöüß]{4,}', r['title'].lower()))),
                                           not MAJOR_OUTLETS.search(r['source'] or ''), len(r['title'])))
        srcs = list(dict.fromkeys(r['source'] for r in reps if r['source']))
        out.append({'t': event_t, 'kind': c['kind'], 'title': first['title'], 'url': first['url'],
                    'source': ', '.join(srcs[:3]) + (f' +{len(srcs) - 3}' if len(srcs) > 3 else ''),
                    'headline': True, 'reports': len(c['reports']),
                    'more': [{'title': r['title'], 'url': r['url'], 'source': r['source'], 't': r['t']}
                             for r in sorted(c['reports'], key=lambda r: r['t'])[:12]]})
    return out


try:
    from zoneinfo import ZoneInfo
    NY = ZoneInfo('America/New_York')
except Exception:  # noqa: BLE001 - no tz database: fall back to UTC-5
    NY = timezone(timedelta(hours=-5))

cap_queue, cap_pending = queue.Queue(), set()


def cached_profile(sym):
    ok, v = cache.get('fh:stock/profile2?' + urllib.parse.urlencode([('symbol', sym)]))
    return v if ok and isinstance(v, dict) and not v.get('__err') else None


def cap_worker():
    """Fills market caps for the earnings list slowly, so live quotes keep most of the 60 calls a minute."""
    while True:
        sym = cap_queue.get()
        while limiter.used() > 35:  # leave at least 20 calls a minute for quotes, search and the panels
            time.sleep(5)
        try:
            if cached_profile(sym) is None:
                fh('stock/profile2', {'symbol': sym}, ttl=7 * 86400, persist=True)
                time.sleep(2.5)
        except Upstream:
            time.sleep(10)
        finally:
            cap_pending.discard(sym)


def earnings():
    today = datetime.now(NY).date()
    try:  # once a day is enough; kept on disk across restarts
        j = fh('calendar/earnings', {'from': str(today), 'to': str(today)}, ttl=86400, persist=True)
    except Upstream as e:
        return {'date': str(today), 'items': [], 'pending': 0, 'retry': True,
                'note': 'Finnhub is busy right now; loading today\'s earnings again in a minute.' if e.status == 429 else str(e)}
    items = []
    for e in j.get('earningsCalendar') or []:
        sym = e.get('symbol') or ''
        if not SYM_RE.match(sym):
            continue
        p = cached_profile(sym)
        items.append({**e, 'name': (p or {}).get('name'), 'marketCap': (p or {}).get('marketCapitalization'),
                      'currency': (p or {}).get('currency'), '_known': p is not None})
    # Largest revenue estimates first, so the big names get their market cap soonest.
    for e in sorted(items, key=lambda e: -(e.get('revenueEstimate') or 0)):
        if not e['_known'] and e['symbol'] not in cap_pending:
            cap_pending.add(e['symbol'])
            cap_queue.put(e['symbol'])
    items.sort(key=lambda e: (e['marketCap'] is None, -(e['marketCap'] or 0), -(e.get('revenueEstimate') or 0)))
    for e in items:
        del e['_known']
    return {'date': str(today), 'items': items, 'pending': sum(1 for e in items if e['symbol'] in cap_pending)}


# ---------- DES: description, listing, company facts, trading statistics, valuation ----------
WIKI_UA = 'Lodestar/1.0 (personal market terminal; non-commercial)'
COMPANY_WORDS = re.compile(r'\b(company|corporation|conglomerate|manufacturer|maker|producer|bank|insurer|insurance|group|firm|'
                           r'retailer|airline|holding|multinational|enterprise|provider|developer|operator|chain|brand|fund|trust)\b', re.I)


def clean_company(name):
    n = re.sub(r'\(.*?\)', '', name or '')
    n = re.sub(r'[,.]?\s+(inc|corp|corporation|co|ltd|plc|company|holdings?|group|ag|se|sa|nv|n\.v\.|kgaa|spa|asa|ab|oyj|'
               r'reg|-reg|class [a-z]|cl [a-z])\.?\b', '', n, flags=re.I)
    return re.sub(r'\s+', ' ', n).strip(' -,.')


def wiki_profile(name):
    """English Wikipedia summary plus a few Wikidata facts (CEO, HQ, founded, employees, website), cached a week."""
    base = clean_company(name)
    if len(base) < 2:
        return None
    key = 'wiki:' + base.lower()
    ok, v = cache.get(key)
    if ok:
        return v
    try:
        hits = json.loads(http_get('https://en.wikipedia.org/w/api.php?' + urllib.parse.urlencode(
            {'action': 'query', 'list': 'search', 'srsearch': f'{base} company', 'srlimit': 4, 'format': 'json'}),
            {'User-Agent': WIKI_UA}, timeout=12))['query']['search']
    except (Upstream, ValueError, KeyError):
        return None
    first = re.sub(r'\W', '', base.split()[0].lower())
    summary = None
    # The exact name first ("SAP SE"; plain "SAP" is a disambiguation page), then the search hits.
    exact = re.sub(r'\s*\(.*?\)', '', name or '').replace('-Reg', '').strip(' ,.')
    tries = [{'title': t} for t in dict.fromkeys([exact, base]) if t] + hits
    for h in tries:
        if first not in re.sub(r'\W', '', h['title'].lower()):
            continue
        try:
            sm = json.loads(http_get('https://en.wikipedia.org/api/rest_v1/page/summary/' + urllib.parse.quote(h['title'].replace(' ', '_')),
                                     {'User-Agent': WIKI_UA}, timeout=12))
        except (Upstream, ValueError):
            continue
        if sm.get('type') == 'standard' and COMPANY_WORDS.search(sm.get('extract', '')[:400]):
            summary = sm
            break
    if not summary:
        cache.put(key, None, 86400, persist=True)
        return None
    out = {'title': summary.get('title'), 'extract': summary.get('extract'),
           'url': ((summary.get('content_urls') or {}).get('desktop') or {}).get('page'), 'facts': {}}
    qid = summary.get('wikibase_item')
    if qid:
        try:
            ent = json.loads(http_get(f'https://www.wikidata.org/wiki/Special:EntityData/{qid}.json', {'User-Agent': WIKI_UA}, timeout=12))
            claims = ent['entities'][qid]['claims']

            def items(pid):
                # Current values only: skip claims with an end date (P582), preferred rank first.
                cs = [c for c in claims.get(pid, []) if 'P582' not in (c.get('qualifiers') or {}) and c.get('rank') != 'deprecated']
                cs.sort(key=lambda c: c.get('rank') != 'preferred')
                return [c['mainsnak']['datavalue']['value']['id'] for c in cs
                        if c['mainsnak'].get('datavalue') and isinstance(c['mainsnak']['datavalue']['value'], dict)
                        and 'id' in c['mainsnak']['datavalue']['value']]

            def latest_quantity(pid):
                best = None
                for c in claims.get(pid, []):
                    dv = c['mainsnak'].get('datavalue')
                    if not dv:
                        continue
                    t = ((c.get('qualifiers') or {}).get('P585') or [{}])[0].get('datavalue', {}).get('value', {}).get('time', '')
                    amt = dv['value'].get('amount')
                    if amt and (best is None or t > best[0]):
                        best = (t, int(float(amt)), t[1:5])
                return best
            ids = {'ceo': items('P169')[:2], 'hq': items('P159')[:1], 'founders': items('P112')[:4], 'industry': items('P452')[:2]}
            allids = sorted({i for v in ids.values() for i in v})
            labels = {}
            if allids:
                lj = json.loads(http_get('https://www.wikidata.org/w/api.php?' + urllib.parse.urlencode(
                    {'action': 'wbgetentities', 'ids': '|'.join(allids), 'props': 'labels', 'languages': 'en', 'format': 'json'}),
                    {'User-Agent': WIKI_UA}, timeout=12))
                labels = {k: (e.get('labels', {}).get('en') or {}).get('value') for k, e in lj.get('entities', {}).items()}
            f = {k: [labels.get(i) for i in v if labels.get(i)] for k, v in ids.items()}
            emp = latest_quantity('P1128')
            if emp:
                f['employees'] = {'n': emp[1], 'year': emp[2]}
            inc = [c['mainsnak']['datavalue']['value']['time'][1:5] for c in claims.get('P571', []) if c['mainsnak'].get('datavalue')]
            if inc:
                f['founded'] = min(inc)
            web = [c['mainsnak']['datavalue']['value'] for c in claims.get('P856', []) if c['mainsnak'].get('datavalue')]
            if web:
                f['website'] = web[0]
            out['facts'] = f
        except (Upstream, ValueError, KeyError, TypeError):
            pass
    cache.put(key, out, 7 * 86400, persist=True)
    return out


def daily_series(ys):
    r = yahoo_chart(ys, {'interval': '1d', 'range': '1y'}, 12 * 3600, 'yhd1y:' + ys)
    if not r or r.get('busy'):
        return None, bool(r and r.get('busy'))
    try:
        q = r['indicators']['quote'][0]
        rows = [(t, o, h, l, c, v or 0) for t, o, h, l, c, v in
                zip(r.get('timestamp') or [], q['open'], q['high'], q['low'], q['close'], q['volume']) if c is not None]
    except (KeyError, IndexError, TypeError):
        return None, False
    return {'rows': rows, 'meta': r.get('meta') or {}}, False


def trading_stats(sym, ys, bench):
    import math
    d, busy = daily_series(ys)
    if not d or len(d['rows']) < 25:
        return {'note': YAHOO_BUSY if busy else 'No daily price history for this instrument.'}
    rows = d['rows']
    closes = [r[4] for r in rows]
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] and closes[i]]

    def vol(n):
        xs = rets[-n:]
        if len(xs) < n * .8:
            return None
        m = sum(xs) / len(xs)
        return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) * math.sqrt(252) * 100

    def perf(days):
        return (closes[-1] / closes[-1 - days] - 1) * 100 if len(closes) > days else None
    year = datetime.now(timezone.utc).year
    ytd_base = next((r[4] for r in reversed(rows) if datetime.fromtimestamp(r[0], timezone.utc).year < year), None)
    last30 = rows[-30:]
    out = {'vol30': vol(30), 'vol90': vol(90), 'avgVol30': sum(r[5] for r in last30) / len(last30),
           'avgTurnover30': sum(r[5] * r[4] for r in last30) / len(last30),
           'hi52': max(r[2] for r in rows if r[2] is not None), 'lo52': min(r[3] for r in rows if r[3] is not None),
           'perf1m': perf(21), 'perf3m': perf(63), 'perfYtd': (closes[-1] / ytd_base - 1) * 100 if ytd_base else None,
           'perf1y': (closes[-1] / closes[0] - 1) * 100, 'last': closes[-1], 'currency': d['meta'].get('currency'),
           'spark': [[r[0], r[4]] for r in rows[::max(1, len(rows) // 120)]] + [[rows[-1][0], rows[-1][4]]],
           'exchange': d['meta'].get('fullExchangeName') or d['meta'].get('exchangeName')}
    # Today: volume and turnover from the 1-minute bars of the regular session
    r1 = yahoo_chart(ys, {'interval': '1m', 'range': '1d', 'includePrePost': 'true'}, 120, 'yh1mx:' + ys)
    if r1 and not r1.get('busy'):
        m1 = r1.get('meta') or {}
        out['volToday'] = m1.get('regularMarketVolume')
        try:
            q1 = r1['indicators']['quote'][0]
            per = (m1.get('currentTradingPeriod') or {}).get('regular') or {}
            lo_t, hi_t = per.get('start', 0), per.get('end', 1e12)
            out['turnoverToday'] = sum((c or 0) * (v or 0) for t, c, v in zip(r1.get('timestamp') or [], q1['close'], q1['volume'])
                                       if lo_t <= t < hi_t) or None
        except (KeyError, IndexError, TypeError):
            pass
    if bench:
        b, _ = daily_series(bench)
        if b:
            bmap = {datetime.fromtimestamp(r[0], timezone.utc).date(): r[4] for r in b['rows']}
            pairs = [(datetime.fromtimestamp(r[0], timezone.utc).date(), r[4]) for r in rows]
            xs, ys2 = [], []
            for (d0, c0), (d1, c1) in zip(pairs, pairs[1:]):
                if d0 in bmap and d1 in bmap and c0 and c1 and bmap[d0] and bmap[d1]:
                    xs.append(math.log(bmap[d1] / bmap[d0]))
                    ys2.append(math.log(c1 / c0))
            if len(xs) > 60:
                mx, my = sum(xs) / len(xs), sum(ys2) / len(ys2)
                cov = sum((a - mx) * (c - my) for a, c in zip(xs, ys2))
                var = sum((a - mx) ** 2 for a in xs)
                out['beta'] = cov / var if var else None
                out['benchmark'] = {'^GSPC': 'S&P 500', '^GDAXI': 'DAX', '^STOXX50E': 'Euro Stoxx 50'}.get(bench, bench)
    return out


def des(sym, name, isin):
    out = {'symbol': sym, 'name': name, 'isin': isin or None}
    base, suffix = split_sym(sym)
    us = ':' not in sym and is_live(sym)
    if sym in CATALOG_MAP or sym.startswith(('OANDA:', 'BINANCE:', 'YH:')):
        out['kind'] = 'other'
    else:
        out['kind'] = 'equity'
        try:
            p = fh('stock/profile2', {'symbol': sym if us else base}, ttl=7 * 86400, persist=True) if us else {}
        except Upstream:
            p = {}
        out['profile'] = {k: p.get(k) for k in ('exchange', 'finnhubIndustry', 'country', 'currency', 'ipo', 'weburl',
                                                  'marketCapitalization', 'shareOutstanding', 'logo', 'phone')}
        if us:
            try:
                m = fh('stock/metric', {'symbol': sym, 'metric': 'all'}, ttl=6 * 3600, persist=True).get('metric') or {}
            except Upstream:
                m = {}
            out['valuation'] = {k: m.get(k) for k in ('peTTM', 'pbAnnual', 'psTTM', 'epsTTM', 'dividendYieldIndicatedAnnual',
                                                       'revenueGrowthTTMYoy', 'netProfitMarginTTM', 'grossMarginTTM',
                                                       'roeTTM', 'currentRatioAnnual', 'beta', '10DayAverageTradingVolume',
                                                       '3MonthAverageTradingVolume')}
        out['wiki'] = wiki_profile(name)
    # Trading statistics from Yahoo daily prices
    if sym.startswith('YH:'):
        ys = sym[3:]
    elif sym.startswith('BINANCE:'):
        pair = sym[8:]
        ys = (pair[:-4] + '-USD') if pair.endswith('USDT') else (pair[:-3] + '-' + pair[-3:])
    elif sym.startswith('OANDA:'):
        ys = (oanda_spec(sym) or (None,) * 5)[4]
    else:
        ys = yahoo_symbol(sym)
    bench = None
    if out['kind'] == 'equity':
        bench = '^GSPC' if us else '^GDAXI' if (isin or '').startswith('DE') or suffix in ('DE', 'F') else '^STOXX50E'
    out['trading'] = trading_stats(sym, ys, bench) if ys else {'note': 'No daily price history for this instrument.'}
    return out


# ---------- index members (MEMB): constituents from Wikipedia, daily changes from Yahoo batch quotes ----------
from html.parser import HTMLParser


class _Tables(HTMLParser):
    """Every table as rows of cell texts; nested tables (logos) are kept separate."""

    def __init__(self):
        super().__init__()
        self.stack, self.done = [], []

    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.stack.append({'rows': [], 'row': None, 'cell': None})
        elif self.stack:
            t = self.stack[-1]
            if tag == 'tr':
                t['row'] = []
            elif tag in ('td', 'th') and t['row'] is not None:
                t['cell'] = []
            elif tag == 'br' and t['cell'] is not None:
                t['cell'].append(' ')

    def handle_endtag(self, tag):
        if not self.stack:
            return
        t = self.stack[-1]
        if tag in ('td', 'th') and t['cell'] is not None:
            t['row'].append(re.sub(r'\s+', ' ', ''.join(t['cell'])).strip())
            t['cell'] = None
        elif tag == 'tr' and t['row'] is not None:
            if t['row']:
                t['rows'].append(t['row'])
            t['row'] = None
        elif tag == 'table':
            self.done.append(self.stack.pop()['rows'])

    def handle_data(self, data):
        if self.stack and self.stack[-1]['cell'] is not None:
            self.stack[-1]['cell'].append(data)


# index future -> (Wikipedia page, ticker style, display name)
INDEX_PAGES = {
    'OANDA:SPX500_USD': ('List of S&P 500 companies', 'us', 'S&P 500'), 'YH:ES=F': ('List of S&P 500 companies', 'us', 'S&P 500'),
    'OANDA:NAS100_USD': ('List of NASDAQ-100 companies', 'us', 'Nasdaq-100'), 'YH:NQ=F': ('List of NASDAQ-100 companies', 'us', 'Nasdaq-100'),
    'OANDA:US30_USD': ('List of Dow Jones Industrial Average companies', 'us', 'Dow Jones'), 'YH:YM=F': ('List of Dow Jones Industrial Average companies', 'us', 'Dow Jones'),
    'OANDA:DE30_EUR': ('DAX', 'yahoo', 'DAX'), 'OANDA:EU50_EUR': ('EURO STOXX 50', 'yahoo', 'Euro Stoxx 50'),
    'OANDA:HK33_HKD': ('Hang Seng Index', 'hk', 'Hang Seng'),
}


def index_members(page, style):
    key = 'memb:' + page
    ok, v = cache.get(key)
    if ok:
        return v
    raw = json.loads(http_get('https://en.wikipedia.org/w/api.php?' + urllib.parse.urlencode(
        {'action': 'parse', 'page': page, 'prop': 'text', 'format': 'json', 'formatversion': 2, 'redirects': 1}),
        {'User-Agent': WIKI_UA}, timeout=20))['parse']['text']
    tp = _Tables()
    tp.feed(raw)
    best = None
    for rows in tp.done:
        if len(rows) < 20:
            continue
        hdr = [h.lower() for h in rows[0]]
        ti = next((i for i, h in enumerate(hdr) if re.search(r'ticker|symbol|code', h)), None)
        ni = next((i for i, h in enumerate(hdr) if re.search(r'company|security|name', h)), None)
        if ti is not None and ni is not None and (best is None or len(rows) > len(best[0])):
            si = next((i for i, h in enumerate(hdr) if re.search(r'sector|industry', h)), None)
            wi = next((i for i, h in enumerate(hdr) if 'weight' in h), None)
            ii = next((i for i, h in enumerate(hdr) if h == 'isin'), None)
            best = (rows, ti, ni, si, wi, ii)
    out = []
    if best:
        rows, ti, ni, si, wi, ii = best
        for r in rows[1:]:
            if len(r) <= max(ti, ni):
                continue
            t = re.sub(r'\[.*?\]', '', r[ti]).strip()
            if style == 'hk':
                code = re.sub(r'\D', '', t)
                if not code:
                    continue
                ys = sym = code.zfill(4) + '.HK'
            elif style == 'us':
                t = t.split()[0] if t else ''
                if not re.fullmatch(r'[A-Z.]{1,6}', t):
                    continue
                sym, ys = t, t.replace('.', '-')
            else:
                t = t.split()[0] if t else ''
                if not re.fullmatch(r'[A-Z0-9.\-]{2,12}', t):
                    continue
                sym = ys = t
            w = None
            if wi is not None and wi < len(r):
                try:
                    w = float(re.sub(r'[^\d.]', '', r[wi]) or 'nan')
                except ValueError:
                    w = None
            out.append({'symbol': sym, 'ys': ys, 'name': re.sub(r'\[.*?\]', '', r[ni]).strip(),
                        'sector': re.sub(r'\[.*?\]', '', r[si]).strip() if si is not None and si < len(r) else '',
                        'weight': w if w == w else None,
                        'isin': r[ii].strip() if ii is not None and ii < len(r) and ISIN_RE.match(r[ii].strip()) else None})
    cache.put(key, out, 7 * 86400 if out else 3600, persist=True)
    return out


def yahoo_spark(symbols, rng='1d'):
    """Price and previous close for many symbols, 20 per Yahoo request, cached 2 minutes per chunk."""
    out, busy = {}, False
    for i in range(0, len(symbols), 20):
        chunk = symbols[i:i + 20]
        key = 'spark:' + ('' if rng == '1d' else rng + ':') + ','.join(chunk)
        ok, v = cache.get(key)
        if not ok:
            if time.time() < yahoo_cooldown[0]:
                busy = True
                continue
            try:
                j = json.loads(http_get('https://query1.finance.yahoo.com/v7/finance/spark?' + urllib.parse.urlencode(
                    {'symbols': ','.join(chunk), 'range': rng, 'interval': '1d'})))
                v = {}
                for r in (j.get('spark') or {}).get('result') or []:
                    m = ((r.get('response') or [{}])[0].get('meta')) or {}
                    if m.get('regularMarketPrice') is not None:
                        v[r['symbol']] = {'last': m.get('regularMarketPrice'), 'prev': m.get('chartPreviousClose') or m.get('previousClose'),
                                          't': m.get('regularMarketTime'), 'ccy': m.get('currency')}
                cache.put(key, v, 120 if rng == '1d' else 1800)
            except Upstream as e:
                if e.status == 429:
                    yahoo_cooldown[0] = time.time() + 300
                busy = True
                continue
            except (ValueError, KeyError, TypeError):
                continue
            time.sleep(0.3)
        out.update(v)
    return out, busy



# ---------- WEI: world equity indices from Yahoo spark ----------
WEI = [('Americas', [('^GSPC', 'S&P 500', 'US'), ('^NDX', 'Nasdaq 100', 'US'), ('^DJI', 'Dow Jones', 'US'), ('^RUT', 'Russell 2000', 'US'),
                     ('^GSPTSE', 'S&P/TSX', 'CA'), ('^BVSP', 'Ibovespa', 'BR'), ('^MXX', 'IPC Mexico', 'MX')]),
       ('EMEA', [('^STOXX50E', 'Euro Stoxx 50', 'EU'), ('^STOXX', 'Stoxx 600', 'EU'), ('^GDAXI', 'DAX', 'DE'), ('^MDAXI', 'MDAX', 'DE'),
                 ('^FTSE', 'FTSE 100', 'GB'), ('^FCHI', 'CAC 40', 'FR'), ('^SSMI', 'SMI', 'CH'), ('FTSEMIB.MI', 'FTSE MIB', 'IT'),
                 ('^IBEX', 'IBEX 35', 'ES'), ('^AEX', 'AEX', 'NL'), ('^OMX', 'OMX Stockholm 30', 'SE'), ('XU100.IS', 'BIST 100', 'TR')]),
       ('Asia-Pacific', [('^N225', 'Nikkei 225', 'JP'), ('^HSI', 'Hang Seng', 'HK'), ('000001.SS', 'Shanghai Composite', 'CN'),
                         ('000300.SS', 'CSI 300', 'CN'), ('^KS11', 'KOSPI', 'KR'), ('^TWII', 'Taiex', 'TW'), ('^AXJO', 'S&P/ASX 200', 'AU'),
                         ('^BSESN', 'Sensex', 'IN'), ('^NSEI', 'Nifty 50', 'IN'), ('^STI', 'Straits Times', 'SG')])]


def wei():
    syms = [s for _, rows in WEI for s, _, _ in rows]
    live, busy = yahoo_spark(syms)
    ytd, busy2 = yahoo_spark(syms, 'ytd')
    regions = [{'region': reg, 'rows': [{'symbol': s, 'name': n, 'country': c, **live.get(s, {}), 'yearRef': (ytd.get(s) or {}).get('prev')}
                                        for s, n, c in rows]} for reg, rows in WEI]
    busy = busy or busy2
    return {'regions': regions, 'notes': ['Yahoo is rate limiting; some indices are missing, retrying in a few minutes.'] if busy else []}


# ---------- MOST: US top movers (Yahoo predefined screeners) ----------
MOST_SCR = {'gainers': 'day_gainers', 'losers': 'day_losers', 'active': 'most_actives'}


def most(kind):
    scr = MOST_SCR.get(kind, 'day_gainers')
    ok, v = cache.get('most:' + scr)
    if ok:
        return v
    if time.time() < yahoo_cooldown[0]:
        raise Upstream(429, 'Yahoo is rate limiting; try again in a few minutes')
    try:
        j = json.loads(http_get('https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?' + urllib.parse.urlencode({'scrIds': scr, 'count': 50})))
    except Upstream as e:
        if e.status == 429:
            yahoo_cooldown[0] = time.time() + 300
        raise
    rows = []
    for q in ((j.get('finance') or {}).get('result') or [{}])[0].get('quotes') or []:
        rows.append({k: q.get(k) for k in ('symbol', 'shortName', 'regularMarketPrice', 'regularMarketChange', 'regularMarketChangePercent',
                                           'regularMarketVolume', 'averageDailyVolume3Month', 'marketCap', 'currency', 'fullExchangeName', 'regularMarketTime')})
    v = {'kind': kind, 'rows': rows}
    cache.put('most:' + scr, v, 60)
    return v


# ---------- ECO: this week's economic calendar (Forex Factory's public JSON export; no actuals) ----------
ECO_FILE = CACHE_FILE.with_name('eco.json')
eco_next = [0.0]


def eco():
    """The feed allows only a few requests per hour: fetch at most hourly, keep the last copy on disk, serve it on errors."""
    try:
        last = json.loads(ECO_FILE.read_text())
    except (OSError, ValueError):
        last = None
    if last and time.time() - last.get('fetched', 0) < 3600 or time.time() < eco_next[0]:
        if last:
            return last
        raise Upstream(429, 'The calendar feed is rate limiting; retrying in a few minutes')
    try:
        raw = json.loads(http_get('https://nfs.faireconomy.media/ff_calendar_thisweek.json'))
    except (Upstream, ValueError) as e:
        eco_next[0] = time.time() + 600
        if last:
            return {**last, 'note': f'Showing the copy from {time.strftime("%H:%M", time.localtime(last["fetched"]))}; the feed failed ({e}).'}
        raise
    v = {'fetched': time.time(), 'events': [{k: e.get(k) for k in ('title', 'country', 'date', 'impact', 'forecast', 'previous')} for e in raw if isinstance(e, dict)]}
    try:
        ECO_FILE.write_text(json.dumps(v))
    except OSError:
        pass
    return v


# ---------- Nasdaq company data (US listings): holders, short interest, analysts ----------
NQ_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131 Safari/537.36'


def nq_num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    t = re.sub(r'[$,%\s]', '', str(v))
    try:
        return float(t)
    except ValueError:
        return None


def nq_get(path, ttl):
    ok, v = cache.get('nq:' + path)
    if ok:
        return v
    j = json.loads(http_get('https://api.nasdaq.com/api/' + path, headers={'User-Agent': NQ_UA, 'Accept': 'application/json'}, timeout=15))
    v = j.get('data') or {}
    cache.put('nq:' + path, v, ttl)
    return v


def nq_data(sym, kind):
    if not re.fullmatch(r'[A-Z]{1,5}(\.[A-Z])?', sym or ''):
        return {'error': 'Holders, short interest and analyst data cover US-listed stocks only.'}
    t = sym.replace('.', '-')
    if kind == 'holders':
        d = nq_get(f'company/{t}/institutional-holdings?limit=40&type=TOTAL&sortColumn=marketValue&sortOrder=DESC', 12 * 3600)
        summ = d.get('ownershipSummary') or {}
        rows = (((d.get('holdingsTransactions') or {}).get('table') or {}).get('rows')) or []
        return {'instPct': nq_num((summ.get('SharesOutstandingPCT') or {}).get('value')),
                'holders': [{'name': r.get('ownerName'), 'date': r.get('date'), 'shares': nq_num(r.get('sharesHeld')),
                             'chg': nq_num(r.get('sharesChange')), 'chgPct': nq_num(r.get('sharesChangePCT')),
                             'value': (nq_num(r.get('marketValue')) or 0) * 1000} for r in rows]}
    if kind == 'short':
        d = nq_get(f'quote/{t}/short-interest?assetClass=stocks', 12 * 3600)
        rows = ((d.get('shortInterestTable') or {}).get('rows')) or []
        out = []
        for r in rows:
            try:
                m, dd, y = (int(x) for x in r['settlementDate'].split('/'))
            except (KeyError, ValueError):
                continue
            out.append({'date': f'{y:04d}-{m:02d}-{dd:02d}', 'interest': nq_num(r.get('interest')),
                        'adv': nq_num(r.get('avgDailyShareVolume')), 'dtc': nq_num(r.get('daysToCover'))})
        return {'rows': sorted(out, key=lambda r: r['date'])}
    if kind == 'analyst':
        tp = nq_get(f'analyst/{t}/targetprice', 6 * 3600)
        fc = nq_get(f'analyst/{t}/earnings-forecast', 6 * 3600)
        co = tp.get('consensusOverview') or {}
        hist = [{'t': h.get('x'), 'px': h.get('y'), **{k: (h.get('z') or {}).get(k) for k in ('buy', 'hold', 'sell', 'consensus')}}
                for h in tp.get('historicalConsensus') or []]
        pick = lambda f: [{'end': r.get('fiscalEnd'), 'eps': nq_num(r.get('consensusEPSForecast')), 'hi': nq_num(r.get('highEPSForecast')),
                           'lo': nq_num(r.get('lowEPSForecast')), 'n': r.get('noOfEstimates'), 'up': r.get('up'), 'down': r.get('down')}
                          for r in ((fc.get(f) or {}).get('rows') or [])]
        return {'target': nq_num(co.get('priceTarget')), 'lo': nq_num(co.get('lowPriceTarget')), 'hi': nq_num(co.get('highPriceTarget')),
                'buy': co.get('buy'), 'hold': co.get('hold'), 'sell': co.get('sell'), 'history': hist,
                'quarterly': pick('quarterlyForecast'), 'yearly': pick('yearlyForecast')}
    return {'error': 'Unknown data kind'}


# ---------- FA: financial statements (Nasdaq), CF: SEC filings (Finnhub) ----------
def fa_data(sym, freq):
    if not re.fullmatch(r'[A-Z]{1,5}(\.[A-Z])?', sym or ''):
        return {'error': 'Financial statements cover US-listed stocks only.'}
    d = nq_get(f"company/{sym.replace('.', '-')}/financials?frequency={2 if freq == 'quarterly' else 1}", 12 * 3600)
    out = {}
    for key, name in (('incomeStatementTable', 'Income statement'), ('balanceSheetTable', 'Balance sheet'),
                      ('cashFlowTable', 'Cash flow'), ('financialRatiosTable', 'Ratios')):
        t = d.get(key) or {}
        hd = t.get('headers') or {}
        cols = [hd[k] for k in sorted(hd, key=lambda k: int(k[5:])) if k != 'value1']
        rows = [[r.get('value1')] + [r.get(f'value{i}') for i in range(2, 2 + len(cols))] for r in t.get('rows') or []]
        out[name] = {'periods': cols, 'rows': rows}
    return {'tables': out, 'freq': freq}


def filings_data(sym):
    if not re.fullmatch(r'[A-Z]{1,5}(\.[A-Z])?', sym or ''):
        return {'error': 'SEC filings cover US-listed companies only.'}
    today = datetime.now(timezone.utc).date()
    rows = fh('stock/filings', {'symbol': sym, 'from': str(today - timedelta(days=3 * 365)), 'to': str(today)}, ttl=3600) or []
    return {'filings': [{'form': (f.get('form') or '').strip(), 'filed': (f.get('filedDate') or '')[:10], 'accepted': f.get('acceptedDate'),
                         'url': f.get('reportUrl') or f.get('filingUrl')} for f in rows]}


# ---------- market tiles on the home screen (Yahoo spark) ----------
TILES = [('^GSPC', 'SPX'), ('^NDX', 'NDX'), ('^DJI', 'INDU'), ('^VIX', 'VIX'), ('^TNX', 'US10Y'), ('DX-Y.NYB', 'DXY'),
         ('GC=F', 'GOLD'), ('CL=F', 'WTI'), ('BTC-USD', 'BTC'), ('EURUSD=X', 'EURUSD')]


def tiles():
    live, busy = yahoo_spark([s for s, _ in TILES])
    return {'tiles': [{'symbol': s, 'label': l, **live.get(s, {})} for s, l in TILES], 'busy': busy}


# ---------- FX converter: ECB reference rates (daily, ~30 currencies) with Yahoo's live quotes on top ----------
FX_NAMES = {'EUR': 'Euro', 'USD': 'US dollar', 'GBP': 'British pound', 'CHF': 'Swiss franc', 'JPY': 'Japanese yen', 'CNY': 'Chinese yuan',
            'HKD': 'Hong Kong dollar', 'CAD': 'Canadian dollar', 'AUD': 'Australian dollar', 'NZD': 'New Zealand dollar', 'SEK': 'Swedish krona',
            'NOK': 'Norwegian krone', 'DKK': 'Danish krone', 'PLN': 'Polish złoty', 'CZK': 'Czech koruna', 'HUF': 'Hungarian forint',
            'RON': 'Romanian leu', 'BGN': 'Bulgarian lev', 'ISK': 'Icelandic króna', 'TRY': 'Turkish lira', 'SGD': 'Singapore dollar',
            'KRW': 'South Korean won', 'INR': 'Indian rupee', 'IDR': 'Indonesian rupiah', 'MYR': 'Malaysian ringgit', 'PHP': 'Philippine peso',
            'THB': 'Thai baht', 'ILS': 'Israeli shekel', 'ZAR': 'South African rand', 'BRL': 'Brazilian real', 'MXN': 'Mexican peso',
            'TWD': 'Taiwan dollar', 'AED': 'UAE dirham', 'SAR': 'Saudi riyal', 'BTC': 'Bitcoin', 'ETH': 'Ether'}
FX_CRYPTO = ('BTC', 'ETH')


def ecb_rates():
    ok, v = cache.get('ecb')
    if ok:
        return v
    raw = http_get('https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml').decode('utf-8', 'replace')
    day = re.search(r"time=['\"](\d{4}-\d{2}-\d{2})", raw)
    rates = {c: float(r) for c, r in re.findall(r"currency=['\"]([A-Z]{3})['\"]\s+rate=['\"]([\d.]+)", raw)}
    v = {'date': day.group(1) if day else None, 'rates': rates}
    cache.put('ecb', v, 3600)
    return v


def fx_rates():
    """Units of each currency per 1 EUR."""
    out, notes = {'EUR': {'rate': 1.0, 'src': 'base'}}, []
    try:
        e = ecb_rates()
        for c, r in e['rates'].items():
            out[c] = {'rate': r, 'src': 'ECB', 't': None, 'day': e['date']}
    except Upstream as ex:
        notes.append(f'ECB reference rates unavailable: {ex}')
    ccys = sorted((set(out) | set(FX_NAMES)) - {'EUR'})
    syms = {('%s-EUR' % c if c in FX_CRYPTO else 'EUR%s=X' % c): c for c in ccys}
    live, busy = yahoo_spark(sorted(syms))
    for ys, q in live.items():
        c = syms.get(ys)
        if not c or not q.get('last'):
            continue
        rate = 1 / q['last'] if c in FX_CRYPTO else q['last']
        prev = (1 / q['prev'] if c in FX_CRYPTO else q['prev']) if q.get('prev') else None
        out[c] = {'rate': rate, 'prev': prev, 'src': 'Yahoo', 't': q.get('t')}
    if busy:
        notes.append('Yahoo is rate limiting; some rates are ECB reference rates (set daily around 16:00 CET).')
    return {'base': 'EUR', 'rates': out, 'names': {c: FX_NAMES.get(c, c) for c in out}, 'notes': notes}

def members(sym):
    spec = INDEX_PAGES.get(sym)
    if not spec:
        return {'index': None, 'items': [], 'note': 'No member list for this index on the free sources (Nikkei 225 and KOSPI are not covered).'}
    page, style, label = spec
    try:
        mem = index_members(page, style)
    except (Upstream, ValueError, KeyError) as e:
        return {'index': label, 'items': [], 'note': f'Could not load the member list from Wikipedia: {e}'}
    quotes, busy = yahoo_spark([m['ys'] for m in mem])
    items = []
    for m in mem:
        q = quotes.get(m['ys']) or {}
        pct = (q['last'] / q['prev'] - 1) * 100 if q.get('last') and q.get('prev') else None
        items.append({k: m[k] for k in ('symbol', 'name', 'sector', 'weight', 'isin')} | {'last': q.get('last'), 'prev': q.get('prev'), 'pct': pct, 'ccy': q.get('ccy'), 't': q.get('t')})
    priced = sum(1 for i in items if i['pct'] is not None)
    note = None
    if busy:
        note = f'Yahoo is rate limiting; {priced} of {len(items)} members priced so far. The rest fills in on the next refresh.'
    return {'index': label, 'source': f'Members: Wikipedia ({page}). Prices: Yahoo Finance, may be delayed.', 'items': items, 'note': note}


# ---------- earnings watcher: big companies' results within minutes, via Google News ----------
REPORTER_MIN_CAP = 10000  # $10bn (Finnhub market caps are in millions)
REPORTERS = []  # [(regex for the company name or ticker, symbol)] of today's reporters worth >= $10bn


def reporter_result(title):
    """A press release with actual results by one of today's big reporters -> a hot score."""
    if not RESULT_RX.search(title) or PREVIEW_RX.search(title):
        return None
    head = title[:60]
    return (HOT_MIN + 2, ['Earnings']) if any(rx.search(head) for rx, _ in REPORTERS) else None


def refresh_reporters(cal):
    out = []
    for e in cal.get('items') or []:
        if (e.get('marketCap') or 0) < REPORTER_MIN_CAP:
            continue
        name = re.sub(r'[,.]?\s+(inc|corp|corporation|co|ltd|plc|company|holdings?|group)\.?$', '', e.get('name') or '', flags=re.I).strip()
        alts = [re.escape(n) for n in (name, e['symbol']) if n and len(n) >= 2]
        if alts:
            out.append((re.compile(r'(?<![\w-])(' + '|'.join(alts) + r')(?![\w-])', re.I if name else 0), e['symbol']))
    REPORTERS[:] = out
EARN_WINDOW_UTC = {'bmo': (10.0, 14.5), 'amc': (19.5, 23.0), 'dmh': (13.0, 21.0), '': (11.0, 23.0)}
EARN_RX = next(rx for lbl, w, rx in HOT_RES if lbl == 'Earnings')
watch_feed = Feed('Earnings watch (Google News)', 'watch:earnings', 'companies', 'US', 120)
watch_state = {}  # symbol -> {'last': last poll, 'hit': first time results were found}


sweep_feed = Feed('Earnings sweep (Google News)', 'watch:sweep', 'companies', 'Global', 120)
SWEEP_QUERIES = [
    ('(earnings OR "quarterly results" OR "quarter results" OR "beats estimates" OR "misses estimates" OR "tops estimates" '
     'OR "raises guidance" OR "cuts guidance" OR "profit warning") when:1h', {'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'}),
    ('(Quartalszahlen OR Geschäftszahlen OR Quartalsergebnis OR Gewinnwarnung OR "hebt Prognose" OR "senkt Prognose" '
     'OR "bestätigt Prognose") when:1h', {'hl': 'de', 'gl': 'DE', 'ceid': 'DE:de'}),
]
sweep_state = {'last': 0}


def earnings_sweep(now):
    """Every 2 minutes, independent of any calendar: results of big companies anywhere (DAX, Europe, US)."""
    if now - sweep_state['last'] < 120:
        return
    sweep_state['last'] = now
    found = []
    for q, loc in SWEEP_QUERIES:
        try:
            root = ET.fromstring(http_get('https://news.google.com/rss/search?' + urllib.parse.urlencode({'q': q, **loc}),
                                          {'User-Agent': FEED_UA}, timeout=15))
        except (Upstream, ET.ParseError) as ex:
            sweep_feed.status, sweep_feed.error = 'error', str(ex)[:160]
            continue
        for it in root.iter('item'):
            title, src = it.findtext('title') or '', it.findtext('source') or ''
            if src and title.endswith(' - ' + src):
                title = title[:-len(src) - 3]
            link = it.findtext('link') or ''
            if not (title and link.startswith('http')) or LOW_QUALITY.search(src) or not lang_ok(title):
                continue
            if not RESULT_RX.search(title) or PREVIEW_RX.search(title):
                continue
            big = BIG_COMPANIES.search(title[:70]) or any(rx.search(title[:60]) for rx, _ in REPORTERS)
            if big:
                found.append({'title': clean(title), 'url': link, 'source': src, 'ts': parse_date(it.findtext('pubDate')),
                              'hot': (HOT_MIN + 2, ['Earnings'])})
    if found:
        news_store.add(sweep_feed, found)
        HOT_CACHE['t'] = 0
    if sweep_feed.status != 'error' or found:
        sweep_feed.status, sweep_feed.error, sweep_feed.last_ok = 'ok', None, now
    sweep_feed.items += len(found)


def earnings_watch_loop():
    """While a company worth ≥ $10bn is in its reporting window, search Google News for its results every 1–2 minutes."""
    while True:
        time.sleep(20)
        try:
            cal = earnings()
            refresh_reporters(cal)
        except Exception:  # noqa: BLE001
            continue
        now = time.time()
        hour = datetime.now(timezone.utc).hour + datetime.now(timezone.utc).minute / 60
        targets = []
        for e in cal.get('items') or []:
            lo, hi = EARN_WINDOW_UTC.get(e.get('hour') or '', EARN_WINDOW_UTC[''])
            st = watch_state.setdefault(f"{cal['date']}:{e['symbol']}", {'last': 0, 'hit': None})
            if (e.get('marketCap') or 0) >= REPORTER_MIN_CAP and lo <= hour <= hi and not (st['hit'] and now - st['hit'] > 1200):
                targets.append((e, st))
        earnings_sweep(now)
        for rank, (e, st) in enumerate(targets[:15]):
            if now - st['last'] < (60 if rank < 5 else 120):
                continue
            st['last'] = now
            name = re.sub(r'[,.]?\s+(inc|corp|corporation|co|ltd|plc|company|holdings?|group)\.?$', '', e.get('name') or '', flags=re.I).strip()
            terms = ' OR '.join(f'"{t}"' for t in dict.fromkeys(filter(None, [e['symbol'], name])))
            q = f'({terms}) (results OR earnings OR quarter OR guidance OR outlook OR EPS) when:1h'
            try:
                root = ET.fromstring(http_get('https://news.google.com/rss/search?' + urllib.parse.urlencode(
                    {'q': q, 'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'}), {'User-Agent': FEED_UA}, timeout=15))
            except (Upstream, ET.ParseError) as ex:
                watch_feed.status, watch_feed.error = 'error', str(ex)[:160]
                continue
            match = name_matcher(e['symbol'], name or e['symbol'])
            found = []
            for it in root.iter('item'):
                title, src = it.findtext('title') or '', it.findtext('source') or ''
                if src and title.endswith(' - ' + src):
                    title = title[:-len(src) - 3]
                link = it.findtext('link') or ''
                if not (title and link.startswith('http')) or LOW_QUALITY.search(src) or 'preview' in title.lower():
                    continue
                if match and match(title) and RESULT_RX.search(title) and not PREVIEW_RX.search(title) and lang_ok(title):
                    found.append({'title': clean(title), 'url': link, 'source': src, 'ts': parse_date(it.findtext('pubDate')),
                                  'hot': (HOT_MIN + 2, ['Earnings'])})
            if found:
                st['hit'] = st['hit'] or now
                news_store.add(watch_feed, found)
                HOT_CACHE['t'] = 0
            watch_feed.status, watch_feed.error, watch_feed.last_ok = 'ok', None, now
            watch_feed.items = watch_feed.items + len(found)


# ---------- Finnhub trade stream (minimal websocket client) ----------
class Stream:
    MAX = 50  # free-plan symbol limit per connection

    def __init__(self):
        self.lock, self.wlock, self.slock = threading.Lock(), threading.Lock(), threading.Lock()
        self.wanted, self.clients, self.subs = {}, {}, set()
        self.sock, self.buf, self.connected, self.error = None, b'', False, None
        self.last_trade, self.bad_since, self.last_heal = time.time(), None, 0.0
        threading.Thread(target=self.run, daemon=True, name='finnhub-stream').start()

    def attach(self, q, syms):
        with self.lock:
            self.clients[q] = set(syms)
            for s in syms:
                self.wanted.setdefault(s, [0, 0.0])[0] += 1
        self.sync()

    def detach(self, q):
        with self.lock:
            for s in self.clients.pop(q, set()):
                w = self.wanted.get(s)
                if w:
                    w[0] = max(0, w[0] - 1)
                    if w[0] == 0:
                        w[1] = time.time()
        self.sync()

    def desired(self):
        now = time.time()
        with self.lock:
            for s in [s for s, w in self.wanted.items() if w[0] == 0 and now - w[1] > 300]:
                del self.wanted[s]
            ranked = sorted(self.wanted.items(), key=lambda kv: (kv[1][0] == 0, -kv[1][1]))
            # The futures proxies stay subscribed around the clock so their settlement prices are known.
            others = [s for s, _ in ranked if s not in PINNED]
            return PINNED | set(others[:self.MAX - len(PINNED)])

    def sync(self):
        with self.slock:
            if not self.connected:
                return
            want = self.desired()
            try:
                for s in want - self.subs:
                    self.send({'type': 'subscribe', 'symbol': s})
                for s in self.subs - want:
                    self.send({'type': 'unsubscribe', 'symbol': s})
                self.subs = want
            except OSError:
                pass

    def frame(self, opcode, payload):
        mask, n = os.urandom(4), len(payload)
        hdr = bytes([0x80 | opcode])
        if n < 126:
            hdr += bytes([0x80 | n])
        elif n < 65536:
            hdr += bytes([0x80 | 126]) + struct.pack('>H', n)
        else:
            hdr += bytes([0x80 | 127]) + struct.pack('>Q', n)
        body = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        with self.wlock:
            self.sock.sendall(hdr + mask + body)

    def send(self, obj):
        self.frame(1, json.dumps(obj).encode())

    def connect(self):
        host = 'ws.finnhub.io'
        raw = socket.create_connection((host, 443), timeout=15)
        s = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode()
        s.sendall((f'GET /?token={urllib.parse.quote(KEY)} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n'
                   f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n'
                   f'User-Agent: {UA}\r\n\r\n').encode())
        buf = b''
        while b'\r\n\r\n' not in buf:
            chunk = s.recv(4096)
            if not chunk or len(buf) > 65536:
                raise OSError('handshake failed')
            buf += chunk
        head, rest = buf.split(b'\r\n\r\n', 1)
        status = head.split(b'\r\n', 1)[0].decode('latin-1')
        if ' 101 ' not in status + ' ':
            raise OSError(f'Finnhub refused the stream: {status}')
        s.settimeout(30)
        self.sock, self.buf = s, rest

    def recvn(self, n, idle_ok=False):
        while len(self.buf) < n:
            try:
                c = self.sock.recv(65536)
            except socket.timeout:
                if idle_ok and not self.buf:
                    raise
                continue
            if not c:
                raise OSError('stream closed by Finnhub')
            self.buf += c
        d, self.buf = self.buf[:n], self.buf[n:]
        return d

    def check_health(self):
        # Finnhub sometimes leaves a connection open but dead: no trades, or "Invalid symbol" for valid
        # symbols (2026-11-11, 13:30–15:42). The pinned BTC/EUR trades around the clock, so silence means a stale stream.
        now = time.time()
        if now - self.last_trade > 180:
            raise OSError('no trades for 3 minutes, reconnecting')
        if self.bad_since and now - self.bad_since > 60 and now - self.last_heal > 600:
            self.last_heal = now
            raise OSError('Finnhub rejected pinned symbols, reconnecting')

    def read_loop(self):
        parts = []
        while True:
            self.check_health()
            try:
                b0, b1 = self.recvn(2, idle_ok=True)
            except socket.timeout:
                self.frame(9, b'')
                self.sync()
                continue
            fin, op, n = b0 & 0x80, b0 & 0x0F, b1 & 0x7F
            if n == 126:
                n = struct.unpack('>H', self.recvn(2))[0]
            elif n == 127:
                n = struct.unpack('>Q', self.recvn(8))[0]
            mask = self.recvn(4) if b1 & 0x80 else None
            p = self.recvn(n)
            if mask:
                p = bytes(b ^ mask[i & 3] for i, b in enumerate(p))
            if op == 8:
                raise OSError('Finnhub closed the stream')
            if op == 9:
                self.frame(10, p)
                continue
            if op == 10:
                continue
            parts.append(p)
            if not fin:
                continue
            msg, parts = b''.join(parts), []
            self.handle(msg)

    def handle(self, raw):
        try:
            m = json.loads(raw)
        except ValueError:
            return
        if m.get('type') == 'trade':
            data = [{'s': d['s'], 'p': d['p'], 'v': d.get('v') or 0, 't': d['t']} for d in m.get('data', [])]
            self.last_trade = time.time()
            bars.add(data)
            with self.lock:
                targets = list(self.clients.items())
            for q, syms in targets:
                mine = [d for d in data if d['s'] in syms]
                if mine:
                    try:
                        q.put_nowait(mine)
                    except queue.Full:
                        pass
        elif m.get('type') == 'error':
            self.error = m.get('msg')
            if 'Invalid symbol' in (self.error or '') and self.error.split()[-1] in PINNED and not self.bad_since:
                self.bad_since = time.time()
            log(f'stream error: {self.error}')

    def run(self):
        backoff = 2
        while True:
            try:
                self.connect()
                with self.slock:
                    self.connected, self.error, self.subs = True, None, set()
                self.last_trade, self.bad_since = time.time(), None
                self.sync()
                log('trade stream connected')
                up = time.time()
                try:
                    self.read_loop()
                finally:
                    # Finnhub allows one stream per key and drops the older one when another connects;
                    # only a connection that held for a minute resets the back-off, so two clients don't storm into 429s.
                    if time.time() - up > 60:
                        backoff = 2
            except Exception as e:  # noqa: BLE001 - reconnect on anything
                self.error = str(e)
                log(f'trade stream dropped: {e}')
            finally:
                self.connected = False
                try:
                    if self.sock:
                        self.sock.close()
                except OSError:
                    pass
                self.sock = None
            time.sleep(backoff)
            backoff = min(60, backoff * 2)


stream = None


def parse_syms(raw):
    return [s for s in dict.fromkeys(x.strip() for x in raw.split(',')) if s and SYM_RE.match(s)][:60]


def one_sym(qs):
    s = qs.get('symbol', '').strip()
    if not SYM_RE.match(s):
        raise Upstream(400, 'Invalid symbol')
    return s


NEWS_TABS = {'all', 'hot', 'selected', 'markets', 'economy', 'cb', 'europe', 'asia', 'crypto', 'fxc', 'wires'}


class Handler(BaseHTTPRequestHandler):
    server_version = 'Lodestar/1.0'
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        host = (self.headers.get('Host') or '').split(':')[0]
        if host not in ('127.0.0.1', 'localhost'):
            return self.json({'error': 'Forbidden host'}, 403)
        u = urllib.parse.urlsplit(self.path)
        qs = dict(urllib.parse.parse_qsl(u.query))
        p = u.path
        try:
            if p in ('/', '/index.html'):
                body = (HERE / 'index.html').read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Cache-Control', 'no-store')
                self.end_headers()
                self.wfile.write(body)
                return
            if p == '/api/stream':
                return self.stream(parse_syms(qs.get('symbols', '')))
            tab = qs.get('tab', 'all') if qs.get('tab') in NEWS_TABS else 'all'
            routes = {
                '/api/search': lambda: search(qs.get('q', '')),
                '/api/quote': lambda: quotes(parse_syms(qs.get('symbols', ''))),
                '/api/profile': lambda: profile(one_sym(qs), qs.get('name', '')),
                '/api/history': lambda: history(one_sym(qs)),
                '/api/listings': lambda: listings(one_sym(qs), qs.get('isin', '').upper()),
                '/api/delayed': lambda: delayed.get(one_sym(qs), qs.get('isin', '').upper(), qs.get('sel') == '1'),
                '/api/news': lambda: news(tab, qs.get('q', '')[:80],
                                          one_sym(qs) if qs.get('symbol') else '', qs.get('name', '')[:80]),
                '/api/feeds': feed_status,
                '/api/gnews': lambda: gnews_public(qs.get('section', 'top') if qs.get('section') in GN_SECTIONS else 'top'),
                '/api/actions': lambda: actions(one_sym(qs), qs.get('name', '')[:80]),
                '/api/members': lambda: members(one_sym(qs)),
                '/api/fx': lambda: fx_rates(),
                '/api/wei': wei,
                '/api/tiles': tiles,
                '/api/most': lambda: most(qs.get('kind', 'gainers')),
                '/api/eco': eco,
                '/api/fa': lambda: fa_data(qs.get('symbol', '').upper()[:12], qs.get('freq', 'annual')),
                '/api/filings': lambda: filings_data(qs.get('symbol', '').upper()[:12]),
                '/api/nq': lambda: nq_data(qs.get('symbol', '').upper()[:12], qs.get('kind', '')),
                '/api/des': lambda: des(one_sym(qs), qs.get('name', '')[:80], qs.get('isin', '').upper()[:12]),
                '/api/earnings': earnings,
                '/api/refs': lambda: futures_refs(parse_syms(qs.get('symbols', ''))),
                '/api/yquote': lambda: yahoo_quote(qs.get('symbol', '')) if re.fullmatch(r'\^?[A-Z0-9.=\-]{1,15}', qs.get('symbol', '')) else {'error': 'Invalid symbol', 'bars': []},
                '/api/status': lambda: {**fh('stock/market-status', {'exchange': 'US'}, ttl=60),
                                        'stream': {'connected': stream.connected, 'error': stream.error}},
            }
            if p not in routes:
                return self.json({'error': 'Not found'}, 404)
            self.json(routes[p]())
        except Upstream as e:
            self.json({'error': str(e)}, e.status if e.status in (400, 403, 429) else 502)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:  # noqa: BLE001
            log(f'error on {p}: {e!r}')
            self.json({'error': f'Server error: {e}'}, 500)

    def stream(self, syms):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Connection', 'keep-alive')
        self.end_headers()
        q = queue.Queue(maxsize=4000)
        stream.attach(q, syms)
        self.close_connection = True
        try:
            self.wfile.write(b'retry: 3000\n\n')
            last_status = 0.0
            while True:
                batch = []
                try:
                    batch.extend(q.get(timeout=0.3))
                except queue.Empty:
                    pass
                while True:
                    try:
                        batch.extend(q.get_nowait())
                    except queue.Empty:
                        break
                out = b''
                if batch:
                    out += b'data: ' + json.dumps({'trades': batch}).encode() + b'\n\n'
                now = time.time()
                if now - last_status > 5:
                    out += b'data: ' + json.dumps({'status': {'connected': stream.connected, 'error': stream.error}}).encode() + b'\n\n'
                    last_status = now
                if out:
                    self.wfile.write(out)
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            stream.detach(q)


def main():
    global stream, delayed
    stream = Stream()
    delayed = Delayed()
    threading.Thread(target=feed_loop, daemon=True, name='feeds').start()
    threading.Thread(target=cap_worker, daemon=True, name='market-caps').start()
    threading.Thread(target=earnings_watch_loop, daemon=True, name='earnings-watch').start()
    bars.load()

    def housekeeping():
        while True:
            time.sleep(60)
            cache.flush()
            try:
                bars.save(PINNED)
            except OSError as e:
                log(f'could not save futures bars: {e}')
    threading.Thread(target=housekeeping, daemon=True, name='housekeeping').start()
    srv = ThreadingHTTPServer(('127.0.0.1', PORT), Handler)
    srv.daemon_threads = True
    url = f'http://127.0.0.1:{PORT}/'
    log(f'Lodestar running on {url}  ({len(feeds)} news sources; Ctrl+C to stop)')
    if '--no-open' not in sys.argv:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        cache.flush()


if __name__ == '__main__':
    main()
