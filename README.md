# VibeTerminal

A Bloomberg-style market terminal that runs locally: a Python standard-library server (`server.py`) and a single-page UI (`index.html`). No dependencies, no build step.

- Watchlist, canvas chart, Time & Sales, news (RSS, Google News, Hot), earnings, corporate actions, index members
- Bloomberg-style command line (`NVD GY`, ISINs, `DES`, `FXCA`)
- All data from free sources; your Finnhub key stays server-side

## Quick start

```bash
mkdir -p ~/.config/lodestar
printf '%s' 'YOUR_FINNHUB_KEY' > ~/.config/lodestar/finnhub.key   # free key: https://finnhub.io/register
chmod 600 ~/.config/lodestar/finnhub.key
python3 server.py            # opens http://127.0.0.1:8765
```

Runs on Python 3.9+. To start it on demand when you open the bookmark (macOS), see [`launchd/com.vibeterminal.server.plist`](launchd/com.vibeterminal.server.plist).

Full guide (features, data sources, architecture, gotchas): [REPLICATE.md](REPLICATE.md).

Market data comes from third-party services under their own terms (Finnhub, Yahoo Finance, Deutsche Börse delayed data for non-commercial use, and others listed in REPLICATE.md). For personal, non-commercial use.
