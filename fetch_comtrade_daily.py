#!/usr/bin/env python3
"""
Global Trade Explorer — Daily Comtrade Fetcher (Fixed)
=======================================================
The UN Comtrade free preview API blocks GitHub Actions IP addresses.
You MUST register for a free API key at:
  https://comtradedeveloper.un.org

Steps:
  1. Go to https://comtradedeveloper.un.org
  2. Click "Sign Up" — it's free
  3. Subscribe to the "comtrade - v1" product (free tier)
  4. Copy your primary subscription key
  5. In your GitHub repo go to:
     Settings → Secrets and variables → Actions → New repository secret
  6. Name: COMTRADE_API_KEY  Value: (your key)

Free tier limits:
  - 250 requests/day
  - 1 request/second rate limit
  - Annual data available 2000-2023

Without a key this script falls back to World Bank only (no commodity/partner data).

Requirements: pip install requests comtradeapicall
"""

import json
import time
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("comtrade_daily")

# ── CONFIG ────────────────────────────────────────────────────────────────────
OUTPUT_FILE      = Path(__file__).parent / "data.json"
REFERENCE_YEAR   = 2023
FALLBACK_YEAR    = 2022
DAILY_REQ_LIMIT  = 200        # Conservative — 250 limit minus buffer for retries
REQS_PER_COUNTRY = 5          # 2 WB + 1 partner exp + 1 partner imp + 1 commodity
COUNTRIES_PER_DAY = DAILY_REQ_LIMIT // REQS_PER_COUNTRY  # ~40

COMTRADE_KEY = os.environ.get("COMTRADE_API_KEY", "").strip()

if not COMTRADE_KEY:
    log.warning("=" * 60)
    log.warning("COMTRADE_API_KEY not set.")
    log.warning("Register free at: https://comtradedeveloper.un.org")
    log.warning("Add as GitHub Secret: COMTRADE_API_KEY")
    log.warning("Falling back to World Bank only (no commodity/partner data)")
    log.warning("=" * 60)

# ── TOP 100 TRADING NATIONS ───────────────────────────────────────────────────
TOP_100 = [
    "CHN","USA","DEU","JPN","GBR","FRA","KOR","NLD","HKG","ITA",
    "CAN","BEL","MEX","SGP","RUS","IND","TWN","AUS","ESP","CHE",
    "SAU","BRA","TUR","POL","SWE","AUT","THA","ARE","NOR","MYS",
    "IDN","ZAF","VNM","CZE","DNK","IRL","PHL","ARG","ISR","NZL",
    "HUN","FIN","CHL","PRT","COL","ROU","PER","NGA","EGY","PAK",
    "BGD","KAZ","UKR","QAT","KWT","IRQ","DZA","MAR","SVK","GRC",
    "AGO","LBY","AZE","OMN","BHR","TUN","LUX","HRV","SRB","SVN",
    "BGR","LTU","LVA","EST","ISL","GEO","BLR","UZB","TZA","ETH",
    "KEN","GHA","CIV","ZMB","CMR","SEN","UGA","MOZ","ZWE","BOL",
    "PRY","URY","ECU","GTM","CRI","PAN","DOM","JAM","TTO","PNG",
]

# ── ISO3 → M49 ────────────────────────────────────────────────────────────────
ISO3_TO_M49 = {
    "AFG":4,"ALB":8,"DZA":12,"AGO":24,"ARG":32,"AUS":36,"AUT":40,"AZE":31,
    "BGD":50,"BEL":56,"BLR":112,"BOL":68,"BIH":70,"BRA":76,"BGR":100,"CMR":120,
    "CAN":124,"CHL":152,"CHN":156,"COL":170,"CRI":188,"HRV":191,"CZE":203,"DNK":208,
    "DOM":214,"ECU":218,"EGY":818,"EST":233,"ETH":231,"FIN":246,"FRA":250,"GEO":268,
    "DEU":276,"GHA":288,"GRC":300,"GTM":320,"HKG":344,"HUN":348,"ISL":352,"IND":356,
    "IDN":360,"IRQ":368,"IRL":372,"ISR":376,"ITA":380,"JAM":388,"JPN":392,"JOR":400,
    "KAZ":398,"KEN":404,"KOR":410,"KWT":414,"LVA":428,"LBY":434,"LTU":440,"LUX":442,
    "MYS":458,"MLT":470,"MEX":484,"MAR":504,"MOZ":508,"MMR":104,"NLD":528,"NZL":554,
    "NGA":566,"NOR":578,"OMN":512,"PAK":586,"PAN":591,"PNG":598,"PRY":600,"PER":604,
    "PHL":608,"POL":616,"PRT":620,"QAT":634,"ROU":642,"RUS":643,"SAU":682,"SEN":686,
    "SRB":688,"SGP":702,"SVK":703,"SVN":705,"ZAF":710,"ESP":724,"LKA":144,"SWE":752,
    "CHE":756,"TWN":158,"TZA":834,"THA":764,"TTO":780,"TUN":788,"TUR":792,"UGA":800,
    "UKR":804,"ARE":784,"GBR":826,"USA":840,"URY":858,"UZB":860,"VNM":704,"ZMB":894,
    "ZWE":716,"BHR":48,"CIV":384,"BGR":100,"LBY":434,"AGO":24,"BOL":68,"GTM":320,
    "DOM":214,"JAM":388,"TTO":780,"PNG":598,"IRL":372,"BEL":56,
}
M49_TO_ISO3 = {v: k for k, v in ISO3_TO_M49.items()}

# ── HS CHAPTER → SECTION MAPPING ─────────────────────────────────────────────
def chapter_to_section(ch):
    if ch <= 5:   return "Live Animals & Food"
    if ch <= 24:  return "Food, Beverages & Tobacco"
    if ch <= 27:  return "Mineral Products & Fuels"
    if ch <= 38:  return "Chemicals & Pharmaceuticals"
    if ch <= 40:  return "Plastics & Rubber"
    if ch <= 43:  return "Hides & Leather"
    if ch <= 49:  return "Wood, Paper & Publishing"
    if ch <= 63:  return "Textiles & Garments"
    if ch <= 67:  return "Footwear & Headgear"
    if ch <= 71:  return "Stone, Ceramic & Precious Metals"
    if ch <= 83:  return "Metals & Steel"
    if ch == 84:  return "Machinery & Equipment"
    if ch == 85:  return "Electronics & Electrical"
    if ch <= 89:  return "Vehicles & Transport"
    if ch <= 92:  return "Optical & Medical Instruments"
    if ch == 93:  return "Arms & Ammunition"
    if ch <= 96:  return "Furniture & Miscellaneous"
    return "Other"

HS_CHAPTER_LABELS = {
    1:"Live Animals",2:"Meat & Offal",3:"Fish & Seafood",4:"Dairy & Eggs",
    5:"Other Animal Products",6:"Plants & Flowers",7:"Vegetables",8:"Fruit & Nuts",
    9:"Coffee, Tea & Spices",10:"Cereals",11:"Milling Products",12:"Oil Seeds",
    13:"Gums & Resins",14:"Vegetable Materials",15:"Fats & Oils",
    16:"Prepared Meat & Fish",17:"Sugar & Confectionery",18:"Cocoa & Chocolate",
    19:"Baked Goods",20:"Prepared Vegetables",21:"Misc. Food",22:"Beverages & Spirits",
    23:"Animal Feed",24:"Tobacco",25:"Salt, Stone & Cement",26:"Ores & Minerals",
    27:"Mineral Fuels & Oil",28:"Inorganic Chemicals",29:"Organic Chemicals",
    30:"Pharmaceuticals",31:"Fertilizers",32:"Dyes & Pigments",33:"Cosmetics & Perfumes",
    34:"Soaps & Cleaners",35:"Enzymes & Starches",36:"Explosives",
    37:"Photographic Products",38:"Misc. Chemicals",39:"Plastics",40:"Rubber",
    41:"Hides & Skins",42:"Leather Goods",43:"Furs",44:"Wood & Lumber",
    45:"Cork",46:"Basketwork",47:"Pulp",48:"Paper & Paperboard",49:"Printed Materials",
    50:"Silk",51:"Wool",52:"Cotton",53:"Vegetable Fibers",54:"Synthetic Filaments",
    55:"Synthetic Fibers",56:"Wadding & Felt",57:"Carpets",58:"Special Fabrics",
    59:"Coated Textiles",60:"Knitted Fabrics",61:"Knitted Apparel",62:"Woven Apparel",
    63:"Other Textile Articles",64:"Footwear",65:"Headgear",66:"Umbrellas",
    67:"Feathers & Flowers",68:"Stone Articles",69:"Ceramics",70:"Glass",
    71:"Precious Stones & Metals",72:"Iron & Steel",73:"Steel Articles",74:"Copper",
    75:"Nickel",76:"Aluminum",77:"Reserved",78:"Lead",79:"Zinc",80:"Tin",
    81:"Other Metals",82:"Tools & Cutlery",83:"Misc. Metal Articles",
    84:"Machinery & Equipment",85:"Electronics & Electrical Equipment",
    86:"Railway Equipment",87:"Vehicles & Auto Parts",88:"Aircraft & Spacecraft",
    89:"Ships & Boats",90:"Optical & Medical Instruments",91:"Clocks & Watches",
    92:"Musical Instruments",93:"Arms & Ammunition",94:"Furniture",
    95:"Toys & Games",96:"Misc. Manufactures",97:"Art & Antiques",
}

# ── WORLD BANK FALLBACK ───────────────────────────────────────────────────────
ISO3_TO_ISO2 = {
    "USA":"US","CHN":"CN","DEU":"DE","JPN":"JP","GBR":"GB","FRA":"FR","CAN":"CA",
    "KOR":"KR","IND":"IN","BRA":"BR","MEX":"MX","AUS":"AU","RUS":"RU","SAU":"SA",
    "ZAF":"ZA","NGA":"NG","EGY":"EG","ARG":"AR","IDN":"ID","TUR":"TR","NLD":"NL",
    "CHE":"CH","ESP":"ES","ITA":"IT","SGP":"SG","MYS":"MY","VNM":"VN","POL":"PL",
    "CHL":"CL","COL":"CO","PHL":"PH","PAK":"PK","NZL":"NZ","THA":"TH","TWN":"TW",
    "ARE":"AE","ISR":"IL","SWE":"SE","NOR":"NO","DNK":"DK","FIN":"FI","PRT":"PT",
    "HKG":"HK","QAT":"QA","KWT":"KW","PER":"PE","ECU":"EC","MAR":"MA","KAZ":"KZ",
    "UKR":"UA","ETH":"ET","KEN":"KE","GHA":"GH","CIV":"CI","ZMB":"ZM","TZA":"TZ",
    "SEN":"SN","UGA":"UG","HUN":"HU","CZE":"CZ","ROU":"RO","SVK":"SK","AUT":"AT",
    "GRC":"GR","HRV":"HR","SRB":"RS","ISL":"IS","IRL":"IE","LUX":"LU","BEL":"BE",
    "BGD":"BD","AGO":"AO","LBY":"LY","AZE":"AZ","OMN":"OM","BHR":"BH","TUN":"TN",
    "SVN":"SI","BGR":"BG","LTU":"LT","LVA":"LV","EST":"EE","GEO":"GE","BLR":"BY",
    "UZB":"UZ","CMR":"CM","MOZ":"MZ","ZWE":"ZW","BOL":"BO","PRY":"PY","URY":"UY",
    "GTM":"GT","CRI":"CR","PAN":"PA","DOM":"DO","JAM":"JM","TTO":"TT","PNG":"PG",
    "IRQ":"IQ","DZA":"DZ","KWT":"KW","AGO":"AO","CIV":"CI",
}

import requests

def get_json(url, params=None, headers=None, retries=3, delay=3):
    h = headers or {}
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=h, timeout=30)
            if r.status_code == 429:
                wait = 65
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                log.error("403 Forbidden — API key may be missing or invalid")
                return None
            if r.status_code == 404:
                log.warning(f"404 — No data found for this query")
                return None
            r.raise_for_status()
            time.sleep(1.5)   # Respect 1 req/sec rate limit with buffer
            return r.json()
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    log.error(f"All retries failed")
    return None


def fetch_comtrade(reporter_m49, flow, cmd_code, year, partner_code=None):
    """
    Call the Comtrade v1 API with a subscription key.
    Endpoint: https://comtradeapi.un.org/data/v1/get/C/A/HS

    For partner flows: cmdCode="TOTAL", partnerCode omitted (returns all partners)
    For commodities:   cmdCode="AG2" means 2-digit HS aggregation level,
                       partnerCode="0" means aggregate all partners into one total
    """
    url = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
    headers = {"Ocp-Apim-Subscription-Key": COMTRADE_KEY}
    params = {
        "reporterCode": reporter_m49,
        "period":       year,
        "flowCode":     flow,
        "cmdCode":      cmd_code,
        "maxRecords":   500,
        "includeDesc":  True,
    }
    # For commodity breakdown we want all partners summed together (partnerCode=0)
    # For partner flows we omit partnerCode to get individual partner rows
    if partner_code is not None:
        params["partnerCode"] = partner_code

    log.debug(f"  GET {url} params={params}")
    data = get_json(url, params=params, headers=headers)
    if not data:
        return []
    rows = data.get("data", [])
    log.debug(f"  → {len(rows)} rows returned")
    return rows


def fetch_partners(iso3, flow="X", year=REFERENCE_YEAR):
    """Fetch top bilateral partner flows."""
    m49 = ISO3_TO_M49.get(iso3)
    if not m49 or not COMTRADE_KEY:
        return {}

    # No partnerCode = API returns one row per partner country
    rows = fetch_comtrade(m49, flow, "TOTAL", year)

    if not rows and year == REFERENCE_YEAR:
        log.info(f"  No {year} partner data for {iso3}, trying {FALLBACK_YEAR}")
        rows = fetch_comtrade(m49, flow, "TOTAL", FALLBACK_YEAR)

    if not rows:
        log.warning(f"  No partner data returned for {iso3} {flow}")
        return {}

    result = {}
    for row in rows:
        pc = row.get("partnerCode")
        if not pc or pc in (0, 896, "0", "896"):
            continue
        val = (row.get("primaryValue") or 0) / 1e9
        if val > 0.001:
            result[int(pc)] = {
                "name": row.get("partnerDesc", ""),
                "value": round(val, 3)
            }
    log.info(f"  Partners ({flow}): {len(result)} countries")
    return result


def fetch_commodities(iso3, flow="X", year=REFERENCE_YEAR):
    """
    Fetch commodity breakdown at HS 2-digit chapter level.
    partnerCode=0 means sum across all partners (world total for this reporter).
    cmdCode=AG2 tells Comtrade to return 2-digit HS chapter aggregates.
    """
    m49 = ISO3_TO_M49.get(iso3)
    if not m49 or not COMTRADE_KEY:
        return {}

    # partnerCode=0 = world total, cmdCode=AG2 = 2-digit chapter aggregation
    rows = fetch_comtrade(m49, flow, "AG2", year, partner_code=0)

    if not rows and year == REFERENCE_YEAR:
        log.info(f"  No {year} commodity data for {iso3}, trying {FALLBACK_YEAR}")
        rows = fetch_comtrade(m49, flow, "AG2", FALLBACK_YEAR, partner_code=0)

    if not rows:
        log.warning(f"  No commodity data returned for {iso3} {flow}")
        return {}

    log.info(f"  Commodity rows ({flow}): {len(rows)}")
    if rows:
        # Log first row to see actual field names coming back
        sample = rows[0]
        log.info(f"  Sample row keys: {list(sample.keys())}")
        log.info(f"  Sample cmdCode={sample.get('cmdCode')} cmdDesc={sample.get('cmdDesc')} val={sample.get('primaryValue')}")

    chapters = {}
    for row in rows:
        # Comtrade may return cmdCode as "01", "84", etc. — strip leading zeros
        cmd = str(row.get("cmdCode", "")).strip()
        # Skip non-numeric or aggregate codes
        if not cmd.isdigit():
            continue
        try:
            ch = int(cmd)
        except (ValueError, TypeError):
            continue
        if ch < 1 or ch > 97:
            continue
        val = (row.get("primaryValue") or 0) / 1e9
        if val > 0:
            chapters[ch] = {
                "label": HS_CHAPTER_LABELS.get(ch, f"HS {ch:02d}"),
                "section": chapter_to_section(ch),
                "value": round(val, 3)
            }
    log.info(f"  Parsed {len(chapters)} chapters for {iso3}")
    return chapters


def build_commodity_lists(chapters, total_exports):
    """Aggregate HS chapters into sections with drill-down."""
    if not chapters or not total_exports:
        return [], {}

    # Aggregate into sections
    sections = {}
    for ch, info in chapters.items():
        sec = info["section"]
        if sec not in sections:
            sections[sec] = {"value": 0.0, "chapters": []}
        sections[sec]["value"] += info["value"]
        sections[sec]["chapters"].append((info["label"], info["value"]))

    sorted_secs = sorted(sections.items(), key=lambda x: x[1]["value"], reverse=True)

    commodities = []
    commodity_sub = {}
    covered = 0.0

    for sec_name, sec_data in sorted_secs:
        pct = round(sec_data["value"] / total_exports * 100, 1)
        if pct < 0.3:
            continue
        commodities.append([sec_name, pct])
        covered += pct

        # Build chapter-level drill-down
        chaps = sorted(sec_data["chapters"], key=lambda x: x[1], reverse=True)
        sub = []
        for label, val in chaps:
            if sec_data["value"] > 0:
                ch_pct = round(val / sec_data["value"] * 100, 1)
                if ch_pct >= 1.0:
                    sub.append([label, ch_pct])
        if sub:
            # Normalize subcategory percentages
            sub_total = sum(p for _, p in sub)
            if sub_total > 0:
                sub = [[n, round(p/sub_total*100,1)] for n,p in sub]
            commodity_sub[sec_name] = sub

    # Normalize top-level percentages
    total_pct = sum(p for _, p in commodities)
    if total_pct > 0:
        commodities = [[n, round(p/total_pct*100,1)] for n,p in commodities]

    return commodities[:12], commodity_sub


def merge_partners(exp_data, imp_data, top_n=10):
    """Merge export/import partner data."""
    NAME_FIXES = {
        "United States of America": "United States",
        "China, mainland": "China",
        "Rep. of Korea": "South Korea",
        "Russian Federation": "Russia",
        "Viet Nam": "Vietnam",
        "Türkiye": "Turkey",
        "Iran (Islamic Rep. of)": "Iran",
        "Bolivia (Plurinational State of)": "Bolivia",
        "United Rep. of Tanzania": "Tanzania",
        "Dem. Rep. of the Congo": "DR Congo",
        "Lao People's Dem. Rep.": "Laos",
        "Venezuela (Bolivarian Republic of)": "Venezuela",
    }
    all_codes = set(exp_data) | set(imp_data)
    partners = []
    for pc in all_codes:
        iso3 = M49_TO_ISO3.get(int(pc))
        if not iso3:
            continue
        exp_val = exp_data.get(pc, {}).get("value", 0)
        imp_val = imp_data.get(pc, {}).get("value", 0)
        raw_name = (exp_data.get(pc) or imp_data.get(pc) or {}).get("name", iso3)
        partners.append({
            "iso": iso3,
            "name": NAME_FIXES.get(raw_name, raw_name),
            "exp": exp_val,
            "imp": imp_val
        })
    partners.sort(key=lambda x: x["exp"] + x["imp"], reverse=True)
    return partners[:top_n]


def fetch_wb_totals(iso3):
    """Fetch trade totals from World Bank."""
    iso2 = ISO3_TO_ISO2.get(iso3)
    if not iso2:
        return None
    out = {"source": "world_bank", "estimated": False}
    for indicator, key in [
        ("NE.EXP.GNFS.CD", "exports"),
        ("NE.IMP.GNFS.CD", "imports"),
    ]:
        url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{indicator}"
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


# ── STATIC DATA ───────────────────────────────────────────────────────────────
COUNTRY_NAMES = {
    "CHN":"China","USA":"United States","DEU":"Germany","JPN":"Japan",
    "GBR":"United Kingdom","FRA":"France","KOR":"South Korea","NLD":"Netherlands",
    "HKG":"Hong Kong","ITA":"Italy","CAN":"Canada","BEL":"Belgium","MEX":"Mexico",
    "SGP":"Singapore","RUS":"Russia","IND":"India","TWN":"Taiwan","AUS":"Australia",
    "ESP":"Spain","CHE":"Switzerland","SAU":"Saudi Arabia","BRA":"Brazil",
    "TUR":"Turkey","POL":"Poland","SWE":"Sweden","AUT":"Austria","THA":"Thailand",
    "ARE":"UAE","NOR":"Norway","MYS":"Malaysia","IDN":"Indonesia","ZAF":"South Africa",
    "VNM":"Vietnam","CZE":"Czechia","DNK":"Denmark","IRL":"Ireland","PHL":"Philippines",
    "ARG":"Argentina","ISR":"Israel","NZL":"New Zealand","HUN":"Hungary","FIN":"Finland",
    "CHL":"Chile","PRT":"Portugal","COL":"Colombia","ROU":"Romania","PER":"Peru",
    "NGA":"Nigeria","EGY":"Egypt","PAK":"Pakistan","BGD":"Bangladesh","KAZ":"Kazakhstan",
    "UKR":"Ukraine","QAT":"Qatar","KWT":"Kuwait","IRQ":"Iraq","DZA":"Algeria",
    "MAR":"Morocco","SVK":"Slovakia","GRC":"Greece","AGO":"Angola","LBY":"Libya",
    "AZE":"Azerbaijan","OMN":"Oman","BHR":"Bahrain","TUN":"Tunisia","LUX":"Luxembourg",
    "HRV":"Croatia","SRB":"Serbia","SVN":"Slovenia","BGR":"Bulgaria","LTU":"Lithuania",
    "LVA":"Latvia","EST":"Estonia","ISL":"Iceland","GEO":"Georgia","BLR":"Belarus",
    "UZB":"Uzbekistan","TZA":"Tanzania","ETH":"Ethiopia","KEN":"Kenya","GHA":"Ghana",
    "CIV":"Côte d'Ivoire","ZMB":"Zambia","CMR":"Cameroon","SEN":"Senegal",
    "UGA":"Uganda","MOZ":"Mozambique","ZWE":"Zimbabwe","BOL":"Bolivia",
    "PRY":"Paraguay","URY":"Uruguay","ECU":"Ecuador","GTM":"Guatemala",
    "CRI":"Costa Rica","PAN":"Panama","DOM":"Dominican Republic","JAM":"Jamaica",
    "TTO":"Trinidad & Tobago","PNG":"Papua New Guinea",
}

WTO_ACCESSION = {
    "CHN":2001,"RUS":2012,"TWN":2002,"SAU":2005,"VNM":2007,"KAZ":2015,
    "UKR":2008,"SRB":2013,"AZE":None,"BLR":None,"UZB":None,"DZA":None,"IRQ":None,
}

CENTROIDS = {
    "USA":[-98.58,39.83],"CHN":[104.19,35.86],"DEU":[10.45,51.17],"JPN":[138.25,36.20],
    "GBR":[-3.44,55.38],"FRA":[2.21,46.23],"KOR":[127.77,35.91],"NLD":[5.29,52.13],
    "HKG":[114.19,22.32],"ITA":[12.57,41.87],"CAN":[-96.82,56.13],"BEL":[4.47,50.50],
    "MEX":[-102.55,23.63],"SGP":[103.82,1.36],"RUS":[99.50,61.52],"IND":[78.96,20.59],
    "TWN":[120.96,23.70],"AUS":[133.78,-25.27],"ESP":[-3.75,40.46],"CHE":[8.23,46.82],
    "SAU":[45.08,23.89],"BRA":[-51.93,-14.24],"TUR":[35.24,38.96],"POL":[19.14,51.92],
    "SWE":[18.64,59.33],"AUT":[14.55,47.52],"THA":[100.99,15.87],"ARE":[53.85,23.42],
    "NOR":[8.47,60.47],"MYS":[109.70,2.11],"IDN":[113.92,-0.79],"ZAF":[25.08,-29.00],
    "VNM":[108.28,14.06],"CZE":[15.47,49.82],"DNK":[10.00,56.26],"IRL":[-8.24,53.41],
    "PHL":[121.77,12.88],"ARG":[-63.62,-38.42],"ISR":[34.85,31.05],"NZL":[172.47,-40.90],
    "HUN":[19.50,47.16],"FIN":[25.75,61.92],"CHL":[-71.54,-35.68],"PRT":[-8.22,39.40],
    "COL":[-74.30,4.57],"ROU":[24.97,45.94],"PER":[-75.02,-9.19],"NGA":[8.68,9.08],
    "EGY":[30.80,26.82],"PAK":[69.35,30.38],"BGD":[90.35,23.68],"KAZ":[66.92,48.02],
    "UKR":[31.17,48.38],"QAT":[51.18,25.35],"KWT":[47.48,29.31],"IRQ":[43.68,33.22],
    "DZA":[2.63,28.16],"MAR":[-7.09,31.79],"SVK":[19.70,48.67],"GRC":[21.82,39.07],
    "AGO":[17.87,-11.20],"LBY":[17.23,26.34],"AZE":[47.58,40.14],"OMN":[57.55,21.51],
    "BHR":[50.56,26.07],"TUN":[9.56,33.89],"LUX":[6.13,49.82],"HRV":[15.20,45.10],
    "SRB":[20.91,44.02],"SVN":[14.82,46.15],"BGR":[25.49,42.73],"LTU":[23.88,55.17],
    "LVA":[24.60,56.88],"EST":[25.01,58.60],"ISL":[-18.56,64.96],"GEO":[43.36,42.32],
    "BLR":[28.05,53.71],"UZB":[63.85,41.38],"TZA":[34.89,-6.37],"ETH":[40.49,9.14],
    "KEN":[37.91,0.02],"GHA":[-1.02,7.95],"CIV":[-5.55,7.54],"ZMB":[27.85,-13.13],
    "CMR":[12.35,5.69],"SEN":[-14.45,14.50],"UGA":[32.29,1.37],"MOZ":[35.55,-18.67],
    "ZWE":[29.15,-19.02],"BOL":[-64.67,-16.29],"PRY":[-58.44,-23.44],"URY":[-55.77,-32.52],
    "ECU":[-78.14,-1.83],"GTM":[-90.23,15.78],"CRI":[-83.75,9.75],"PAN":[-80.78,8.54],
    "DOM":[-70.16,18.74],"JAM":[-77.30,18.11],"TTO":[-61.22,10.45],"PNG":[143.96,-6.31],
}


# ── MAIN ──────────────────────────────────────────────────────────────────────
def load_data():
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            return json.load(f)
    return {"_meta": {}, "_fetch_state": {}, "countries": {}}


def save_data(d):
    with open(OUTPUT_FILE, "w") as f:
        json.dump(d, f, indent=2, ensure_ascii=False)


def get_next_batch(fetch_state):
    """Pick the countries updated least recently."""
    last_updated = {
        iso: fetch_state.get(iso, {}).get("last_updated", "1970-01-01")
        for iso in TOP_100
    }
    sorted_by_age = sorted(last_updated.items(), key=lambda x: x[1])
    return [iso for iso, _ in sorted_by_age[:COUNTRIES_PER_DAY]]


def run():
    log.info("=== Daily Comtrade Fetcher ===")
    log.info(f"API key present: {'YES' if COMTRADE_KEY else 'NO — World Bank only mode'}")

    d = load_data()
    fetch_state = d.get("_fetch_state", {})
    batch = get_next_batch(fetch_state)

    log.info(f"Fetching {len(batch)} countries: {', '.join(batch[:8])}...")

    req_count = 0
    updated = 0

    for iso in batch:
        if req_count + REQS_PER_COUNTRY > DAILY_REQ_LIMIT:
            log.info(f"Request limit reached ({req_count}). Stopping for today.")
            break

        name = COUNTRY_NAMES.get(iso, iso)
        log.info(f"[{req_count}/{DAILY_REQ_LIMIT}] {iso} ({name})")
        existing = d["countries"].get(iso, {})

        # ── World Bank totals (always fetch regardless of Comtrade key) ──
        wb = fetch_wb_totals(iso)
        exports = wb.get("exports") if wb else existing.get("exports")
        imports = wb.get("imports") if wb else existing.get("imports")
        ref_year = wb.get("year", REFERENCE_YEAR) if wb else existing.get("reference_year", REFERENCE_YEAR)
        req_count += 2  # WB uses 2 requests (exp + imp indicators)

        if COMTRADE_KEY:
            # ── Partner flows ─────────────────────────────────────────────
            exp_p = fetch_partners(iso, "X")
            req_count += 1
            imp_p = fetch_partners(iso, "M")
            req_count += 1

            # ── Commodity breakdown ───────────────────────────────────────
            exp_chap = fetch_commodities(iso, "X")
            req_count += 1

            partners = merge_partners(exp_p, imp_p) if (exp_p or imp_p) else existing.get("partners", [])
            if exp_chap and exports:
                commodities, commodity_sub = build_commodity_lists(exp_chap, exports)
            else:
                commodities = existing.get("commodities", [])
                commodity_sub = existing.get("commodity_sub", {})

            source = "world_bank+comtrade"
        else:
            # No Comtrade key — keep existing partner/commodity data
            partners = existing.get("partners", [])
            commodities = existing.get("commodities", [])
            commodity_sub = existing.get("commodity_sub", {})
            source = "world_bank"

        total = round((exports or 0) + (imports or 0), 2)

        d["countries"][iso] = {
            "name":           COUNTRY_NAMES.get(iso, existing.get("name", iso)),
            "iso2":           ISO3_TO_ISO2.get(iso, existing.get("iso2", "")),
            "wto_member":     WTO_ACCESSION.get(iso, 1995) is not None,
            "wto_accession":  WTO_ACCESSION.get(iso, 1995),
            "centroid":       CENTROIDS.get(iso, existing.get("centroid")),
            "total_trade":    total,
            "exports":        exports,
            "imports":        imports,
            "source":         source,
            "reference_year": ref_year,
            "estimated":      False,
            "commodities":    commodities,
            "commodity_sub":  commodity_sub,
            "partners":       partners,
            "ftas":           existing.get("ftas", []),
            "disputes":       existing.get("disputes", []),
        }

        fetch_state[iso] = {
            "last_updated": datetime.now(timezone.utc).date().isoformat(),
            "source": source,
            "partners_count": len(partners),
            "commodity_sections": len(commodities),
        }
        updated += 1
        log.info(f"  ✓ {iso}: exports={fmt_b(exports)} imports={fmt_b(imports)} partners={len(partners)} commodities={len(commodities)}")

    d["_fetch_state"] = fetch_state
    d["_meta"] = {
        **d.get("_meta", {}),
        "last_daily_run": datetime.now(timezone.utc).isoformat(),
        "countries_updated_today": updated,
        "total_countries": len(d["countries"]),
        "requests_used_today": req_count,
        "reference_year": REFERENCE_YEAR,
        "value_units": "USD billions (current)",
        "methodology": (
            "Trade totals from World Bank API (NE.EXP/IMP.GNFS.CD, most recent year). "
            "Bilateral partner flows and HS chapter commodity breakdowns from UN Comtrade v1 API "
            f"(requires free subscription key from comtradedeveloper.un.org). "
            f"Reference year: {REFERENCE_YEAR}, fallback: {FALLBACK_YEAR}. "
            "FTAs and WTO disputes maintained manually."
        ),
        "data_sources": [
            {
                "id": "world_bank",
                "name": "World Bank Open Data API",
                "url": "https://api.worldbank.org/v2/",
                "description": "Total exports and imports of goods and services (current USD)",
                "coverage": "1960–2023",
                "license": "CC BY 4.0",
                "citation": "World Bank (2024). World Development Indicators. https://databank.worldbank.org"
            },
            {
                "id": "un_comtrade",
                "name": "UN Comtrade API v1",
                "url": "https://comtradeapi.un.org/",
                "description": "Bilateral trade partner flows and commodity breakdown by HS chapter (AG2). Requires free key from comtradedeveloper.un.org.",
                "coverage": "2000–2023",
                "license": "UN open data (free tier: 250 req/day with key)",
                "citation": "United Nations Statistics Division (2024). UN Comtrade Database. https://comtrade.un.org"
            }
        ],
    }

    save_data(d)

    cycle_done = sum(1 for iso in TOP_100 if iso in fetch_state)
    log.info(f"\n✓ Done. Updated {updated} countries today. {req_count} requests used.")
    log.info(f"  Cycle progress: {cycle_done}/{len(TOP_100)} countries fetched at least once.")
    if not COMTRADE_KEY:
        log.info("  ⚠ No Comtrade key — partner/commodity data not updated.")
        log.info("  Register free at: https://comtradedeveloper.un.org")


def fmt_b(v):
    if v is None: return "N/A"
    if v >= 1000: return f"${v/1000:.1f}T"
    return f"${v:.0f}B"


if __name__ == "__main__":
    run()
