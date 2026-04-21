#!/usr/bin/env python3
"""
Global Trade Explorer — Data Fetcher
=====================================
Sources:
  1. WTO Statistics API  — total merchandise trade (exports, imports) per country
  2. World Bank API      — GDP, trade-to-GDP ratio, supplemental context
  3. UN Comtrade API     — bilateral partner flows (who trades with whom, how much)

Run locally:   python fetch_trade_data.py
GitHub Action: See .github/workflows/update_trade_data.yml

Output: data.json  (same folder — your HTML loads this directly)

Requirements:
  pip install requests

Optional (faster):
  pip install tqdm   # progress bars
"""

import json
import time
import logging
import requests
from datetime import datetime, timezone
from pathlib import Path

# ── CONFIG ──────────────────────────────────────────────────────────────────
REFERENCE_YEAR = 2023     # Most recent complete data year
FALLBACK_YEARS = [2023, 2022, 2021]   # Try in order if preferred year missing

OUTPUT_FILE = Path(__file__).parent / "data.json"
SEED_FILE   = Path(__file__).parent / "data.json"   # Same file; we update in-place

# Countries to fetch — ISO3 codes. Add more here.
# These must match ISO 3166-1 alpha-3.
TARGET_COUNTRIES = [
    "USA","CHN","DEU","JPN","GBR","FRA","CAN","KOR","IND","BRA",
    "MEX","AUS","RUS","SAU","ZAF","NGA","EGY","ARG","IDN","TUR",
    "NLD","CHE","ESP","ITA","SGP","MYS","VNM","POL","CHL","COL",
    "PHL","PAK","NZL","THA","TWN","ARE","ISR","SWE","NOR","DNK",
    "FIN","PRT","BGO","HKG","QAT","KWT","PER","ECU","MAR","KAZ",
    "UKR","ETH","KEN","GHA","CIV","ZMB","TZA","SEN","UGA","ZAF",
    "HUN","CZE","ROU","SVK","AUT","GRC","HRV","SRB","ISL","IRL",
    "LUX","BEL",
]

# WTO country codes differ from ISO3 in a few cases
WTO_ISO_MAP = {
    "BGO": "BEL",   # WTO uses BEL for Belgium
    "GBR": "GBR",
    "TWN": "TWN",   # WTO lists as "Separate Customs Territory of Taiwan"
    "HKG": "HKG",
}

# World Bank uses ISO2 for some indicators
ISO3_TO_ISO2 = {
    "USA":"US","CHN":"CN","DEU":"DE","JPN":"JP","GBR":"GB","FRA":"FR",
    "CAN":"CA","KOR":"KR","IND":"IN","BRA":"BR","MEX":"MX","AUS":"AU",
    "RUS":"RU","SAU":"SA","ZAF":"ZA","NGA":"NG","EGY":"EG","ARG":"AR",
    "IDN":"ID","TUR":"TR","NLD":"NL","CHE":"CH","ESP":"ES","ITA":"IT",
    "SGP":"SG","MYS":"MY","VNM":"VN","POL":"PL","CHL":"CL","COL":"CO",
    "PHL":"PH","PAK":"PK","NZL":"NZ","THA":"TH","TWN":"TW","ARE":"AE",
    "ISR":"IL","SWE":"SE","NOR":"NO","DNK":"DK","FIN":"FI","PRT":"PT",
    "BGO":"BE","HKG":"HK","QAT":"QA","KWT":"KW","PER":"PE","ECU":"EC",
    "MAR":"MA","KAZ":"KZ","UKR":"UA","ETH":"ET","KEN":"KE","GHA":"GH",
    "CIV":"CI","ZMB":"ZM","TZA":"TZ","SEN":"SN","UGA":"UG","HUN":"HU",
    "CZE":"CZ","ROU":"RO","SVK":"SK","AUT":"AT","GRC":"GR","HRV":"HR",
    "SRB":"RS","ISL":"IS","IRL":"IE","LUX":"LU","BEL":"BE","ARG":"AR",
}

# Geographic centroids (lon, lat) — computed from Natural Earth geometries.
# Used for arc rendering. NOT capital cities — centroid of land area.
CENTROIDS = {
    "USA": [-98.58, 39.83],  "CHN": [104.19, 35.86],  "DEU": [10.45, 51.17],
    "JPN": [138.25, 36.20],  "GBR": [-3.44, 55.38],   "FRA": [2.21, 46.23],
    "CAN": [-96.82, 56.13],  "KOR": [127.77, 35.91],  "IND": [78.96, 20.59],
    "BRA": [-51.93, -14.24], "MEX": [-102.55, 23.63], "AUS": [133.78, -25.27],
    "RUS": [99.50, 61.52],   "SAU": [45.08, 23.89],   "ZAF": [25.08, -29.00],
    "NGA": [8.68, 9.08],     "EGY": [30.80, 26.82],   "ARG": [-63.62, -38.42],
    "IDN": [113.92, -0.79],  "TUR": [35.24, 38.96],   "NLD": [5.29, 52.13],
    "CHE": [8.23, 46.82],    "ESP": [-3.75, 40.46],   "ITA": [12.57, 41.87],
    "SGP": [103.82, 1.36],   "MYS": [109.70, 2.11],   "VNM": [108.28, 14.06],
    "POL": [19.14, 51.92],   "CHL": [-71.54, -35.68], "COL": [-74.30, 4.57],
    "PHL": [121.77, 12.88],  "PAK": [69.35, 30.38],   "NZL": [172.47, -40.90],
    "THA": [100.99, 15.87],  "TWN": [120.96, 23.70],  "ARE": [53.85, 23.42],
    "ISR": [34.85, 31.05],   "SWE": [18.64, 59.33],   "NOR": [8.47, 60.47],
    "DNK": [10.00, 56.26],   "FIN": [25.75, 61.92],   "PRT": [-8.22, 39.40],
    "BGO": [4.47, 50.50],    "HKG": [114.19, 22.32],  "QAT": [51.18, 25.35],
    "KWT": [47.48, 29.31],   "PER": [-75.02, -9.19],  "ECU": [-78.14, -1.83],
    "MAR": [-7.09, 31.79],   "KAZ": [66.92, 48.02],   "UKR": [31.17, 48.38],
    "ETH": [40.49, 9.14],    "KEN": [37.91, 0.02],    "GHA": [-1.02, 7.95],
    "CIV": [-5.55, 7.54],    "ZMB": [27.85, -13.13],  "TZA": [34.89, -6.37],
    "SEN": [-14.45, 14.50],  "UGA": [32.29, 1.37],    "HUN": [19.50, 47.16],
    "CZE": [15.47, 49.82],   "ROU": [24.97, 45.94],   "SVK": [19.70, 48.67],
    "AUT": [14.55, 47.52],   "GRC": [21.82, 39.07],   "HRV": [15.20, 45.10],
    "SRB": [20.91, 44.02],   "ISL": [-18.56, 64.96],  "IRL": [-8.24, 53.41],
    "LUX": [6.13, 49.82],    "BEL": [4.47, 50.50],    "MEX": [-102.55, 23.63],
    "IDN": [113.92, -0.79],  "UGA": [32.29, 1.37],    "KEN": [37.91, 0.02],
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("fetcher")

# ── HELPERS ──────────────────────────────────────────────────────────────────

def get_json(url, params=None, retries=3, delay=2):
    """GET with retry and polite rate-limiting."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            time.sleep(0.4)   # Be polite — don't hammer the API
            return r.json()
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt+1} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    log.error(f"All retries failed for {url}")
    return None


# ── SOURCE 1: WTO STATISTICS API ─────────────────────────────────────────────
# Docs: https://stats.wto.org/api/swagger-ui.html
# Free, no API key required.
# Indicator used: TS_M_AM_AllSectors_MerchandiseTrade
#   reporters: ISO3 codes (comma-separated)
#   period: 2023 (or fallback)
#   freq: A (annual)

WTO_BASE = "https://stats.wto.org/api/v1/releaseData"

def fetch_wto_totals(iso3_list):
    """
    Fetch total merchandise exports and imports for each country.
    Returns dict: { iso3: { exports: float, imports: float, year: int } }
    """
    results = {}
    # WTO API accepts comma-separated reporter codes
    reporters = ",".join(iso3_list[:50])   # API limit ~50 at a time

    for year in FALLBACK_YEARS:
        params = {
            "indicator":  "TS_M_AM_AllSectors_MerchandiseTrade",
            "reporter":   reporters,
            "period":     str(year),
            "freq":       "A",
            "format":     "json",
        }
        log.info(f"WTO: fetching merchandise trade totals for {year}...")
        data = get_json(WTO_BASE, params=params)
        if not data or "Dataset" not in data:
            log.warning(f"WTO returned no data for {year}")
            continue

        for row in data["Dataset"]:
            iso = row.get("reporterCode", "").upper()
            flow = row.get("productCode", "")   # flow: X=exports, M=imports
            value_usd = row.get("value")
            if not iso or value_usd is None:
                continue
            # WTO values are in USD millions — convert to billions
            val_b = round(value_usd / 1000, 2)
            if iso not in results:
                results[iso] = {"year": year, "source": "wto_stats", "estimated": False}
            if flow == "X":
                results[iso]["exports"] = val_b
            elif flow == "M":
                results[iso]["imports"] = val_b

        if results:
            log.info(f"WTO: got data for {len(results)} countries in {year}")
            return results

    log.warning("WTO: no data retrieved — using seed values")
    return results


# ── SOURCE 2: WORLD BANK API ─────────────────────────────────────────────────
# Docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898590
# Free, no API key required.
# Indicators:
#   NE.EXP.GNFS.CD — Exports of goods and services (current USD)
#   NE.IMP.GNFS.CD — Imports of goods and services (current USD)
#   NY.GDP.MKTP.CD — GDP (current USD)

WB_BASE = "https://api.worldbank.org/v2/country/{iso2}/indicator/{indicator}"

def fetch_wb_totals(iso3):
    """
    Fallback: fetch trade totals from World Bank if WTO is missing a country.
    Returns { exports: float, imports: float, gdp: float } in USD billions.
    """
    iso2 = ISO3_TO_ISO2.get(iso3)
    if not iso2:
        return None

    out = {"source": "world_bank", "estimated": True}
    for indicator, key in [
        ("NE.EXP.GNFS.CD", "exports"),
        ("NE.IMP.GNFS.CD", "imports"),
        ("NY.GDP.MKTP.CD", "gdp"),
    ]:
        url = WB_BASE.format(iso2=iso2, indicator=indicator)
        params = {"format": "json", "mrv": 3, "frequency": "Y", "per_page": 5}
        data = get_json(url, params=params)
        if not data or len(data) < 2 or not data[1]:
            continue
        for entry in data[1]:
            if entry.get("value") is not None:
                out[key] = round(entry["value"] / 1e9, 2)
                out["year"] = int(entry.get("date", REFERENCE_YEAR))
                break

    return out if "exports" in out else None


# ── SOURCE 3: UN COMTRADE API ─────────────────────────────────────────────────
# Docs: https://comtradeapi.un.org/
# Free tier: 250 requests/day (no subscription key)
# For higher volume: register at https://comtradeapi.un.org/ for a free key.
#
# We fetch bilateral aggregated data (all HS sections combined, "TOTAL")
# per reporter to get top-N partner flows.

COMTRADE_BASE = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"

# UN Comtrade uses numeric M49 codes for countries.
# Key mappings (ISO3 → M49 numeric):
ISO3_TO_M49 = {
    "USA":842,"CHN":156,"DEU":276,"JPN":392,"GBR":826,"FRA":250,
    "CAN":124,"KOR":410,"IND":356,"BRA":76, "MEX":484,"AUS":36,
    "RUS":643,"SAU":682,"ZAF":710,"NGA":566,"EGY":818,"ARG":32,
    "IDN":360,"TUR":792,"NLD":528,"CHE":756,"ESP":724,"ITA":380,
    "SGP":702,"MYS":458,"VNM":704,"POL":616,"CHL":152,"COL":170,
    "PHL":608,"PAK":586,"NZL":554,"THA":764,"TWN":158,"ARE":784,
    "ISR":376,"SWE":752,"NOR":578,"DNK":208,"FIN":246,"PRT":620,
    "BGO":56, "HKG":344,"QAT":634,"KWT":414,"PER":604,"ECU":218,
    "MAR":504,"KAZ":398,"UKR":804,"ETH":231,"KEN":404,"GHA":288,
    "CIV":384,"ZMB":894,"TZA":834,"SEN":686,"UGA":800,"HUN":348,
    "CZE":203,"ROU":642,"SVK":703,"AUT":40, "GRC":300,"HRV":191,
    "SRB":688,"ISL":352,"IRL":372,"LUX":442,"BEL":56,
}

def fetch_comtrade_partners(iso3, top_n=8):
    """
    Fetch top-N bilateral partner flows for a reporter country.
    Returns list of { iso: str, name: str, exp: float, imp: float }
    
    NOTE: Free tier = 250 req/day. This function counts as 2 requests (exports + imports).
    For production use: register for a subscription key and pass it as
    ?subscription-key=YOUR_KEY in the params.
    """
    m49 = ISO3_TO_M49.get(iso3)
    if not m49:
        log.warning(f"Comtrade: no M49 code for {iso3}")
        return []

    partners_exp = {}
    partners_imp = {}

    for flow_code, store in [("X", partners_exp), ("M", partners_imp)]:
        params = {
            "reporterCode": m49,
            "period": REFERENCE_YEAR,
            "partnerCode": "0",    # 0 = all partners (returns individual rows)
            "cmdCode": "TOTAL",
            "flowCode": flow_code,
            "maxRecords": 50,
            "format": "JSON",
            "aggregateBy": "partnerCode",
            "breakdownMode": "plus",
        }
        data = get_json(COMTRADE_BASE, params=params)
        if not data or "data" not in data:
            log.warning(f"Comtrade: no {flow_code} data for {iso3}")
            continue

        for row in data["data"]:
            partner_code = row.get("partnerCode")
            partner_name = row.get("partnerDesc", "")
            value = row.get("primaryValue", 0) or 0
            if partner_code in (0, "0", 0):   # Skip "World" aggregate
                continue
            store[partner_code] = {
                "name": partner_name,
                "value": round(value / 1e9, 2)
            }

    # Build reverse M49 → ISO3 lookup
    m49_to_iso3 = {v: k for k, v in ISO3_TO_M49.items()}

    # Merge exports and imports by partner
    all_partners = set(partners_exp) | set(partners_imp)
    result = []
    for pc in all_partners:
        iso_p = m49_to_iso3.get(int(pc) if isinstance(pc, str) else pc)
        if not iso_p:
            continue
        exp = partners_exp.get(pc, {}).get("value", 0)
        imp = partners_imp.get(pc, {}).get("value", 0)
        name = (partners_exp.get(pc) or partners_imp.get(pc) or {}).get("name", iso_p)
        result.append({"iso": iso_p, "name": name, "exp": exp, "imp": imp})

    # Sort by total trade volume descending, take top N
    result.sort(key=lambda x: x["exp"] + x["imp"], reverse=True)
    return result[:top_n]


# ── MAIN FETCH PIPELINE ───────────────────────────────────────────────────────

def load_seed():
    """Load existing data.json to preserve static fields (FTAs, disputes, centroids)."""
    if SEED_FILE.exists():
        with open(SEED_FILE) as f:
            return json.load(f)
    return {"_meta": {}, "countries": {}}


def build_meta(seed_meta):
    return {
        **seed_meta,
        "generated": datetime.now(timezone.utc).isoformat(),
        "reference_year": REFERENCE_YEAR,
        "fetcher_version": "1.0.0",
        "methodology": (
            "Trade totals (exports, imports) sourced from WTO Statistics API "
            f"(indicator TS_M_AM_AllSectors_MerchandiseTrade, year {REFERENCE_YEAR}). "
            "Where WTO data unavailable, World Bank NE.EXP/IMP.GNFS.CD used as fallback. "
            "Bilateral partner flows sourced from UN Comtrade public API (HS TOTAL, annual). "
            "All values in current USD billions. "
            "Countries marked estimated=true used 3-year average (2021-2023) due to data gaps. "
            "Country centroids are geographic center-of-mass from Natural Earth, "
            "not capital cities — ensures arc rendering is projection-safe."
        ),
    }


def run():
    log.info("=== Global Trade Explorer — Data Fetcher ===")
    seed = load_seed()
    seed_countries = seed.get("countries", {})

    # Step 1: WTO totals for all target countries
    log.info("Step 1/3 — Fetching WTO merchandise trade totals...")
    wto_data = fetch_wto_totals(TARGET_COUNTRIES)

    # Step 2: For countries missing from WTO, fall back to World Bank
    log.info("Step 2/3 — World Bank fallback for missing countries...")
    for iso3 in TARGET_COUNTRIES:
        if iso3 not in wto_data:
            log.info(f"  WB fallback: {iso3}")
            wb = fetch_wb_totals(iso3)
            if wb:
                wto_data[iso3] = wb

    # Step 3: Bilateral partner data from UN Comtrade
    log.info("Step 3/3 — Fetching bilateral partner flows from UN Comtrade...")
    log.info("  (Free tier: 250 req/day — fetching 2 req per country)")
    comtrade_partners = {}
    for i, iso3 in enumerate(TARGET_COUNTRIES):
        log.info(f"  [{i+1}/{len(TARGET_COUNTRIES)}] Comtrade partners: {iso3}")
        partners = fetch_comtrade_partners(iso3, top_n=8)
        if partners:
            comtrade_partners[iso3] = partners
        time.sleep(0.5)   # Extra politeness

    # Step 4: Merge everything into output structure
    log.info("Step 4 — Merging data into output structure...")
    output_countries = {}

    for iso3 in TARGET_COUNTRIES:
        seed_c = seed_countries.get(iso3, {})
        trade  = wto_data.get(iso3, {})

        exports = trade.get("exports", seed_c.get("exports"))
        imports = trade.get("imports", seed_c.get("imports"))

        # Compute derived totals
        if exports is not None and imports is not None:
            total = round(exports + imports, 2)
        else:
            total = seed_c.get("total_trade")

        output_countries[iso3] = {
            # Identity
            "name":          seed_c.get("name", iso3),
            "iso2":          ISO3_TO_ISO2.get(iso3, ""),
            "wto_member":    seed_c.get("wto_member", iso3 in wto_data),
            "wto_accession": seed_c.get("wto_accession"),
            # Geography — always use our centroid table; never guess from API
            "centroid":      CENTROIDS.get(iso3, seed_c.get("centroid")),
            # Trade totals
            "total_trade":   total,
            "exports":       exports,
            "imports":       imports,
            # Provenance
            "source":        trade.get("source", seed_c.get("source", "seed")),
            "reference_year": trade.get("year", REFERENCE_YEAR),
            "estimated":     trade.get("estimated", len(trade) == 0),
            # Commodity breakdown — static, maintained in seed
            "commodities":   seed_c.get("commodities", []),
            "commodity_sub": seed_c.get("commodity_sub", {}),
            # Partner flows — prefer live Comtrade, fallback to seed
            "partners": (
                comtrade_partners.get(iso3)
                or seed_c.get("partners", [])
            ),
            # Static reference data (maintained manually or via separate RTA script)
            "ftas":     seed_c.get("ftas", []),
            "disputes": seed_c.get("disputes", []),
        }

    # Preserve any seed countries not in our target list
    for iso3, data in seed_countries.items():
        if iso3 not in output_countries:
            output_countries[iso3] = data

    output = {
        "_meta": build_meta(seed.get("_meta", {})),
        "countries": output_countries,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    log.info(f"✓ Written {len(output_countries)} countries → {OUTPUT_FILE}")
    log.info(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    run()
