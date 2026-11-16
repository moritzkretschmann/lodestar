# Lodestar: replication guide

A Bloomberg-style market terminal that runs locally. **Two files, Python standard library only, no build step, no npm.**

- `server.py` (~3,100 lines): a local HTTP server on `127.0.0.1:8765`. It serves the page, proxies every data source so the API key never reaches the browser, caches responses and relays a websocket stream to the browser over Server-Sent Events.
- `index.html` (~1,900 lines): the whole UI, with inline CSS and JS, a canvas chart and no external libraries.

This guide is written for a Claude Code session (or a person) who wants to run it as is, or rebuild or extend it.

---

## 1. Run it as is (5 minutes)

Requirements: Python 3.10+ (developed on 3.14, macOS), internet access, and a free Finnhub API key.

1. Get a free key at https://finnhub.io/register.
2. Store the key in a file, never in the code:
   ```bash
   mkdir -p ~/.config/lodestar
   printf '%s' 'YOUR_FINNHUB_KEY' > ~/.config/lodestar/finnhub.key
   chmod 600 ~/.config/lodestar/finnhub.key
   ```
3. Put `server.py` and `index.html` in the same folder (the original uses `~/lodestar/`) and run:
   ```bash
   python3 ~/lodestar/server.py            # opens the browser
   python3 ~/lodestar/server.py --no-open  # headless
   LODESTAR_PORT=9000 python3 ~/lodestar/server.py   # other port
   ```
4. Open http://127.0.0.1:8765.

Optional keys, in the same folder (the code paths for these two are **untested**):
- `oanda.key`: an OANDA v20 practice token, for CFD intraday history.
- `alpaca.key`: `KEY_ID:SECRET`, for US stock intraday history.

State lives in `~/.config/lodestar/`:
- `cache.json`: the response cache
- `bars.json`: recorded 1-minute bars of the pinned futures
- `delayed/`: Deutsche Börse files

The folder, config and localStorage names all use "lodestar", the project name.

### Auto-start on macOS (optional)
`~/Library/LaunchAgents/com.lodestar.server.plist`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.lodestar.server</string>
  <key>ProgramArguments</key><array>
    <string>/usr/bin/env</string><string>python3</string><string>-I</string>
    <string>/Users/YOU/lodestar/server.py</string><string>--no-open</string></array>
  <key>WorkingDirectory</key><string>/Users/YOU/lodestar</string>
  <key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>30</integer>
  <key>StandardOutPath</key><string>/Users/YOU/Library/Logs/Lodestar.log</string>
  <key>StandardErrorPath</key><string>/Users/YOU/Library/Logs/Lodestar.log</string>
</dict></plist>
```
Load it with `launchctl load ~/Library/LaunchAgents/com.lodestar.server.plist`. After a code change, restart with `launchctl kickstart -k gui/$(id -u)/com.lodestar.server`. Don't start a second manual copy, or the two will clash on the port. Changes to `index.html` only need a browser reload.

---

## 2. What the terminal does

**Layout.** There is a top bar with the command line, the FXCA button, the US market status and world clocks. Below it runs a ticker tape. The main area is a four-column grid:
- the watchlist sidebar
- the chart, with the quote header above it
- Time & Sales, with corporate actions or index members below
- news, TV, Hot and earnings

### Command line (Bloomberg-style)
- Search by name, ticker, ISIN or Bloomberg code (`NVD GY`, `SAP GR Equity`, `US67066G1040`).
- Result order: **main listing first** (the US line for US ISINs, GY for German ones), then the typed listing, then real-time venues, then delayed venues, then the rest.
- `<ticker | ISIN | name> DES`: description page. A bare `DES` opens it for the current instrument.
- `FXCA`, `100 USD FXCA` or `100 USD CHF FXCA`: FX converter page. `FXC` also works.
- `WEI`: world equity indices page (30 indices in Americas, EMEA and Asia-Pacific from Yahoo spark, `/api/wei`). A row click opens the index chart as a `YH:^…` symbol.
- `MOST`: US top movers page with Gainers / Losers / Most active tabs (Yahoo predefined screeners `day_gainers`, `day_losers`, `most_actives` via `/api/most?kind=`, no crumb needed, cached 60 s), with relative volume vs the 3-month average. A row click opens the stock.
- `ECO`: this week's economic calendar from Forex Factory's public export (`nfs.faireconomy.media/ff_calendar_thisweek.json`, `/api/eco`), in local time, filterable by impact and currency. **The feed rate-limits hard (429 after two quick requests):** the server fetches at most hourly, keeps a copy in `~/.config/lodestar/eco.json`, and backs off 10 minutes after an error. No actual values, only forecast and previous.
- Only one overlay page is open at a time (`closePages()`).
- `ALRT 123.45` (or `123.45 ALRT`): one-shot price alert for the selected instrument; the direction comes from the current price. A bare `ALRT` opens the alert list. Alerts live in localStorage, are checked every 1.5 s against `x.last`, and fire with a toast, a beep and a desktop notification. Setting one adds the instrument to the watchlist, which keeps its price updating.
- Clicking the empty command line lists the commands (WEI, MOST, ECO, FXCA, ALRT, DES); typing 2+ letters of one puts it first above the search results. There are no command buttons in the top bar.
- Source notes live in tooltips (status bar, chart and Time & Sales headers) to keep the screen quiet. The news panel opens on TV; the Google News tab was removed.
- `/` focuses the command line. Esc closes dropdowns and pages. ↑/↓ moves through the watchlist.

### Look (Nov 2026 redesign)
- Neutral black/grey palette, IBM Plex Mono everywhere, compact 26 px panel headers in title case, green (`--acc`) for active tabs.
- Quote Monitor (left) is a table: tabs All / Equities / Indices / Commodities / FX / Crypto, columns Ticker, Last, Chg, Chg %.
- News is a one-line table (time, headline, ticker, source); hot headlines are red, details in the tooltip.
- Bottom-right panel: Earnings / Active (US most active, gainers, losers) / Heatmap.
- Corporate-actions panel has tabs for US-listed stocks: Corporate actions, **Holders** (13F treemap, colour = change in shares), **Short interest** (charts of shares short and days to cover), **Analysts** (price target and range, Buy/Hold/Sell, quarterly and yearly EPS estimates). Data: Nasdaq's public JSON API (`api.nasdaq.com`, browser UA, cached 6–12 h) via `/api/nq?symbol=&kind=holders|short|analyst`.

### Gödel-style additions
- Default UI scale: `html{zoom:.84}`. The chart divides mouse coordinates by the zoom (`zoomF()`) and sizes its canvas from `clientWidth`; keep the browser itself at 100 %.
- `FA`, `EM`, `CF`, `CN` work like `DES`: after a security (`AAPL FA`) or bare for the selected one.
  - **FA**: income statement, balance sheet, cash flow, ratios; annual or quarterly (Nasdaq `company/<T>/financials?frequency=1|2`, values in USD thousands) via `/api/fa`.
  - **EM**: consensus EPS by fiscal year and quarter with range, growth, implied P/E, revisions, plus price target and Buy/Hold/Sell (`/api/nq?kind=analyst`).
  - **CF**: 3 years of SEC filings (Finnhub `stock/filings`) grouped into Annual, Quarterly, 8-K, Proxy, Ownership, Insiders, Offerings via `/api/filings`.
  - **CN**: switches the news panel to the selected stock.
- The Time & sales slot has a **Snapshot** view (default): 1-year sparkline, market, trading, performance, valuation and fundamentals from `/api/des`.
- Panel borders are draggable (three column borders, the actions/snapshot border, the row border); sizes are saved in localStorage `lodestar.layout`, double-click a border to reset. Only on wide screens (> 1100 px); the holders treemap and short-interest charts re-render on resize.
- WEI has a YTD column (second Yahoo spark call with `range=ytd`; `chartPreviousClose` = last close of the prior year).
- The Quote Monitor has an input at the bottom that adds one or many tickers (comma-separated names, tickers, Bloomberg codes or ISINs).

### Watchlist sidebar
- Groups: Equities, Index futures, Commodity futures, FX, Crypto.
- Defaults are the US mega caps plus these "futures", which are OANDA CFDs through Finnhub:
  - Indices: S&P 500, Nasdaq 100, Dow, DAX, Euro Stoxx 50, Nikkei 225, Hang Seng, plus KOSPI via Yahoo
  - Commodities: Gold, Silver, WTI, Brent
  - FX: EUR/USD
  - Crypto: BTC/EUR
- Each row shows the price, day %, a sparkline and the currency.
- A ☾ in its own left column marks a closed market (◐ in US extended hours). Closed rows are dimmed.
- Futures day change is measured against the CFD's own price at the last settlement: NY 17:00, Berlin 17:30, Tokyo 15:45, HK 16:00, BTC 00:00 UTC.

### Chart
- Canvas candles or line, timeframes, SMA20/EMA50.
- Drag to measure: abs/% change, bars and time span, anchored in data space.
- Extended-hours bars are shaded, with a dotted regular-close line.
- The header says "closes HH:MM" or "opens Day HH:MM" in local time.
- Gap handling after the computer sleeps: reload history on `visibilitychange`, on stream reconnect and every 10 minutes. The server merges backfill into gaps, and recorded bars win.

### Quote header
- Last price with currency, change, O/H/L/previous close.
- Pre-market/after-hours price for US stocks.
- A "Delayed 15 min" badge and bid/ask for Deutsche Börse venues.

### Time & Sales
- Streamed trades from Finnhub, or the delayed Deutsche Börse prints. A source disclaimer is shown.

### Bottom panel
- **Corporate actions**, only for the selected instrument:
  - SEC filings, insider trades, EPS history, analyst ratings and the next earnings date (Finnhub)
  - dividends and splits (Yahoo)
  - 12 months of Google News headlines classified into events (spin-off, M&A, AGM, capital measures…), grouped and deduplicated
- **Index members**, when an equity index future is selected:
  - members from Wikipedia, prices from Yahoo spark
  - advancers/decliners and top gainers/losers
  - a sortable list with a price timestamp, or "Close" in amber when the market is closed
  - clicking a row opens the stock

### News panel
- Tabs:
  - **Google News**, the default: the German edition by RSS section
  - All (56 RSS feeds plus Finnhub categories, English and German only, sorted newest first)
  - **Hot**
  - Selected instrument
  - **TV**: Bloomberg TV on YouTube, video `QB5BNdBFujE`, muted. A transparent shield hides YouTube's controls until you click. A URL field loads other YouTube links.
- **Hot** = red highlights and a list under the TV player. Each headline gets a rule-based score (`hot_score()`, threshold 4):
  - hot on their own: central banks, named macro data (CPI, payrolls…), OPEC+ decisions, deals ≥ $1bn
  - earnings and market moves count only for big companies and big markets
  - medium scores become hot when ≥ 3 other outlets carry the story
- **Earnings**:
  - today's US calendar sorted by market cap, fetched once a day
  - fast results: PR Newswire and Business Wire are polled every 60 s, plus a Google News watcher on each ≥ $10bn reporter during its bmo/amc window
  - EU/DAX results come from a Google News sweep every 2 minutes
  - previews are excluded

### DES page
- Separate overlay page; the home screen stays unchanged.
- Description from Wikipedia and Finnhub, facts from Wikidata (current CEO/HQ only).
- Main venue, volume and turnover today plus 30-day averages, 30/90-day volatility, beta, performance, 52-week range, valuation (US only) and a 1-year sparkline.

### FXCA page
- Amount, from, swap, to, a large result, the rate both ways and today's change.
- A table with the amount in all 36 currencies (fiat plus BTC and ETH); a row click sets the target.
- Amounts accept `1.250,50`, `1,250.50`, `10k` and `3mio`.
- Rates are live from Yahoo, with the ECB daily reference rates as fallback. Cross rates go via EUR.

### Currency labels
- Shown next to every price.
- None for FX pairs, index, rate and FX futures (they trade in points), or plain indices.

---

## 3. Data sources (all free; the respective terms apply)

| Source | Used for | Notes |
|---|---|---|
| Finnhub free plan (key) | WS trades: US stocks and ETFs, Binance crypto, OANDA FX and CFDs. REST: quote (US and crypto only), search, profile2, company-news, news categories, earnings calendar (US), market-status, stock/metric (US), filings, insiders, recommendations, forex/symbol | 60 calls/min, so the server limits to 55. **No** candles, non-US quotes, FX quote, dividends or splits on the free plan |
| Yahoo Finance v8 chart / v7 spark | History (1m with `includePrePost`, daily), dividends and splits, index-member prices, futures `YH:KC=F`, KOSPI, FX rates | Unofficial; answers 429 easily, so there's a shared 5-minute cooldown. No cookie/crumb tricks |
| Binance klines | Crypto history | |
| OpenFIGI v3 mapping | Bloomberg codes and listings per ISIN | No key, about 25 requests/min, cached 7 days |
| Deutsche Börse MiFIR delayed data | Xetra (GY), Börse Frankfurt (GF), Tradegate (TH) prints, bars and quotes, **15 min delayed** | `https://mfs.deutsche-boerse.com/api/<DETR\|DFRA\|DGAT>-<pre\|post>trade` lists the files; `/api/download/<file>` gives NDJSON.gz per minute. Free for non-commercial use. HTTP 429 after about 30 quick downloads, so the budget is 25 per 10 minutes |
| Business Insider suggest | ISIN lookup | |
| Google News RSS | Google News tab, corporate-action history, earnings watcher and sweep | |
| ~56 RSS feeds | News | Polled with ETag/If-Modified-Since |
| Wikipedia / Wikidata | DES text and facts, index member lists | Keep requests sparse (429) |
| ECB eurofxref-daily.xml | FX fallback | |

What doesn't exist for free and legitimately: real-time German/EU quotes. The terms of Tradegate, LSX and L&S forbid scraping. gettex blocks bots; don't work around it. SEC EDGAR direct access needs a contact email in the User-Agent.

---

## 4. Server architecture (`server.py`)

- `ThreadingHTTPServer`, bound to 127.0.0.1 only.
- `Cache`: in memory, persisted to `cache.json`, TTL per key.
- `Limiter(55, 60)`: Finnhub budget, with `used()` and `try_acquire()`. Background workers yield when more than 35 calls of the minute are used.
- A minimal websocket client for `wss://ws.finnhub.io` with subscribe/unsubscribe per browser interest. The pinned futures stay subscribed 24/7 and their 1-minute bars are recorded.
- `/api/stream`: SSE to the browser (trades plus heartbeats).
- Background threads:
  - RSS poller
  - earnings watcher and sweep
  - market-cap filler for the earnings list
  - Deutsche Börse downloader, sequential with back-off

Endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /api/search?q=` | Instruments: Finnhub, OpenFIGI codes, OANDA list, `YH_FUTURES` table, ordered as in section 2 |
| `/api/quote?symbols=` | Finnhub quotes |
| `/api/profile?symbol=&name=` | profile2 plus ISIN |
| `/api/history?symbol=` | 1m and 1s bars (Binance, OANDA, Alpaca, Yahoo, recorded bars) |
| `/api/listings?symbol=&isin=` | OpenFIGI listings |
| `/api/delayed?symbol=&isin=&sel=1` | Deutsche Börse prints, bars and book |
| `/api/news?tab=all\|hot\|selected…&q=` | Merged news |
| `/api/gnews?section=top\|business\|world\|nation\|technology` | Google News |
| `/api/feeds` | Feed health |
| `/api/actions?symbol=&name=` | Corporate actions |
| `/api/members?symbol=` | Index members and prices |
| `/api/fx` | FX rates (EUR base) |
| `/api/des?symbol=&name=&isin=` | DES page data |
| `/api/earnings` | Today's earnings by market cap |
| `/api/refs?symbols=` | Futures settlement reference prices |
| `/api/yquote?symbol=` | Yahoo quote and 1m bars |
| `/api/status` | Market status |
| `/api/stream` | SSE trades |

Key tables to edit:
- `FEEDS`: RSS sources
- `HOT_RULES` / `HOT_MIN`, `BIG_COMPANIES`: the Hot logic
- `FUTURES`: settlement times
- `YH_FUTURES`: exchange futures and their German aliases
- `INDEX_PAGES`: Wikipedia member lists
- `FX_NAMES`: FX converter currencies
- `REPORTER_MIN_CAP`: earnings watcher threshold

## 5. Client architecture (`index.html`)

- State lives in the `inst` map of instruments: `sym`, `group`, `isin`, `bbg`, `last`, `q`, bars `m`/`s`, `prints`, `prof`, `delayed`, `ccy`. The watchlist and its metadata are kept in localStorage.
- Trading hours are a static `SESS` / `FUT_SESS` table (time zones plus intervals) through `sessionState`, `tzParts`, `zoned` and `sessionIntervals`. US holidays come from Finnhub; other holidays are not covered.
- `extInfo()` handles pre-market/after-hours, `ccyOf()` returns the currency per instrument, and `priceTime()` formats member price timestamps.
- Pages (DES, FXCA) are `<section class="des-page" hidden>` overlays. **Keep the global CSS rule `[hidden]{display:none!important}`**: without it, `display:flex` wins and the page can't be closed.
- Colours come from CSS variables (`--bg --panel --line --fg --dim --amber --cyan --up --down`) on a dark theme.

## 6. Gotchas learned the hard way

- On the Finnhub free plan, the `/quote` `c` field is the **regular** close, even in after-hours. Extended-hours prices come from the stream or Yahoo 1m with pre/post.
- Yahoo 429s: share one cooldown across all Yahoo calls and batch with spark (20 symbols per call).
- When a laptop sleeps, the server pauses too, which leaves chart gaps. That is why gaps are refilled. `caffeinate` keeps the machine awake if you need that.
- Company-name matching in headlines needs care. "Advanced" matched AMD, and "Portland General Electric" matched GE. Use the full name when the first word is generic, check the preceding capitalised word, and drop portal pages with `|` in the title.
- An index's own Wikipedia article often no longer holds the member table; use the "List of … companies" pages.
- Index futures are quoted in points: show no currency next to them.

## 7. Prompt to hand to another Claude

> Here are `server.py`, `index.html` and `REPLICATE.md` for Lodestar, a local Bloomberg-style terminal (Python stdlib server plus a single-page HTML UI). Read REPLICATE.md first. Set it up on my machine: put the files in `~/lodestar/`, ask me for my Finnhub key and store it in `~/.config/lodestar/finnhub.key` (chmod 600, never in code or chat logs), start the server and confirm http://127.0.0.1:8765 loads quotes. Then [optional: set up the LaunchAgent / extend feature X]. Keep it stdlib-only and keep the API key server-side.
