#!/usr/bin/env python3
"""
Global Trade Explorer — Daily Comtrade Fetcher
===============================================
Runs daily via GitHub Actions. Each day fetches the next batch of countries
from the TOP_100 list, pulling:
  - Bilateral partner flows (exports + imports, top 10 partners)
  - Commodity breakdown by HS Chapter (Level 2, ~97 chapters aggregated to sections)

Rate limit: 250 requests/day free tier. At 4 req/country we do ~60/day.
Full 100-country cycle completes in ~2 days, then restarts.

State tracking: uses data.json "_fetch_state" to remember which countries
were updated last, so each day picks up where it left off.

Requirements: pip install requests
"""

import json
import time
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("comtrade_daily")

# ── CONFIG ──────────────────────────────────────────────────────────────────
OUTPUT_FILE     = Path(__file__).parent / "data.json"
REFERENCE_YEAR  = 2023
FALLBACK_YEAR   = 2022
DAILY_REQ_LIMIT = 240          # Leave 10 as buffer from the 250 free limit
REQS_PER_COUNTRY = 4           # 2 for partners (exp+imp), 2 for commodities (exp+imp)
COUNTRIES_PER_DAY = DAILY_REQ_LIMIT // REQS_PER_COUNTRY   # ~60

# Optional subscription key — set as GitHub Secret COMTRADE_API_KEY
COMTRADE_KEY = os.environ.get("COMTRADE_API_KEY", "")

# ── TOP 100 TRADING NATIONS ──────────────────────────────────────────────────
# Ordered roughly by total trade volume. All have meaningful Comtrade data.
TOP_100 = [
    # Tier 1 — Major economies
    "CHN","USA","DEU","JPN","GBR","FRA","KOR","NLD","HKG","ITA",
    "CAN","BEL","MEX","SGP","RUS","IND","TWN","AUS","ESP","CHE",
    # Tier 2 — Large emerging + developed
    "SAU","BRA","TUR","POL","SWE","AUT","THA","ARE","NOR","MYS",
    "IDN","ZAF","VNM","CZE","DNK","IRE","PHL","ARG","ISR","NZL",
    # Tier 3 — Mid-size traders
    "HUN","FIN","CHL","PRT","COL","ROU","PER","NGA","EGY","PAK",
    "BGD","KAZ","UKR","QAT","KWT","IRQ","DZA","MAR","SVK","GRC",
    # Tier 4 — Significant regional traders
    "AGO","LBY","AZE","OMN","BHR","TUN","LUX","HRV","SRB","SVN",
    "BGR","LTU","LVA","EST","ISL","GEO","BLR","UZB","TZA","ETH",
    # Tier 5 — Notable smaller traders
    "KEN","GHA","CIV","ZMB","CMR","SEN","UGA","MOZ","ZWE","BOL",
    "PRY","URY","ECU","GTM","CRI","PAN","DOM","JAM","TTO","PNG",
]

# ── ISO3 → UN M49 numeric (Comtrade uses M49) ────────────────────────────────
ISO3_TO_M49 = {
    "AFG":4,"ALB":8,"DZA":12,"AGO":24,"ARG":32,"ARM":51,"AUS":36,"AUT":40,
    "AZE":31,"BHS":44,"BHR":48,"BGD":50,"BLR":112,"BEL":56,"BLZ":84,"BEN":204,
    "BTN":64,"BOL":68,"BIH":70,"BWA":72,"BRA":76,"BRN":96,"BGR":100,"BFA":854,
    "BDI":108,"CPV":132,"KHM":116,"CMR":120,"CAN":124,"CAF":140,"TCD":148,
    "CHL":152,"CHN":156,"COL":170,"COD":180,"COG":178,"CRI":188,"CIV":384,
    "HRV":191,"CUB":192,"CYP":196,"CZE":203,"DNK":208,"DJI":262,"DOM":214,
    "ECU":218,"EGY":818,"SLV":222,"GNQ":226,"ERI":232,"EST":233,"SWZ":748,
    "ETH":231,"FJI":242,"FIN":246,"FRA":250,"GAB":266,"GMB":270,"GEO":268,
    "DEU":276,"GHA":288,"GRC":300,"GTM":320,"GIN":324,"GNB":624,"GUY":328,
    "HTI":332,"HND":340,"HKG":344,"HUN":348,"ISL":352,"IND":356,"IDN":360,
    "IRN":364,"IRQ":368,"IRL":372,"ISR":376,"ITA":380,"JAM":388,"JPN":392,
    "JOR":400,"KAZ":398,"KEN":404,"KOR":410,"KWT":414,"KGZ":417,"LAO":418,
    "LVA":428,"LBN":422,"LSO":426,"LBR":430,"LBY":434,"LIE":438,"LTU":440,
    "LUX":442,"MDG":450,"MWI":454,"MYS":458,"MDV":462,"MLI":466,"MLT":470,
    "MRT":478,"MUS":480,"MEX":484,"MDA":498,"MNG":496,"MNE":499,"MAR":504,
    "MOZ":508,"MMR":104,"NAM":516,"NPL":524,"NLD":528,"NZL":554,"NIC":558,
    "NER":562,"NGA":566,"MKD":807,"NOR":578,"OMN":512,"PAK":586,"PAN":591,
    "PNG":598,"PRY":600,"PER":604,"PHL":608,"POL":616,"PRT":620,"QAT":634,
    "ROU":642,"RUS":643,"RWA":646,"SAU":682,"SEN":686,"SRB":688,"SLE":694,
    "SGP":702,"SVK":703,"SVN":705,"SOM":706,"ZAF":710,"ESP":724,"LKA":144,
    "SDN":729,"SUR":740,"SWE":752,"CHE":756,"SYR":760,"TWN":158,"TJK":762,
    "TZA":834,"THA":764,"TGO":768,"TTO":780,"TUN":788,"TUR":792,"TKM":795,
    "UGA":800,"UKR":804,"ARE":784,"GBR":826,"USA":840,"URY":858,"UZB":860,
    "VEN":862,"VNM":704,"YEM":887,"ZMB":894,"ZWE":716,"IRE":372,"BGO":56,
}

# ── HS SECTION MAPPING ───────────────────────────────────────────────────────
# Maps HS chapter numbers to human-readable section names
# This lets us display "Electronics" instead of "Chapter 85"
HS_CHAPTER_TO_SECTION = {
    **{c: "Live Animals & Food" for c in range(1, 5)},
    **{c: "Food & Beverages" for c in range(5, 25)},
    **{c: "Mineral Products & Fuels" for c in range(25, 28)},
    **{c: "Chemicals & Pharmaceuticals" for c in range(28, 39)},
    **{c: "Plastics & Rubber" for c in range(39, 41)},
    **{c: "Hides, Leather & Furs" for c in range(41, 44)},
    **{c: "Wood & Paper Products" for c in range(44, 50)},
    **{c: "Textiles & Garments" for c in range(50, 64)},
    **{c: "Footwear & Headgear" for c in range(64, 68)},
    **{c: "Stone, Ceramic & Glass" for c in range(68, 72)},
    **{c: "Metals & Steel" for c in range(72, 84)},
    **{c: "Machinery & Equipment" for c in range(84, 85)},
    **{c: "Electronics & Electrical" for c in range(85, 86)},
    **{c: "Vehicles & Transport" for c in range(86, 90)},
    **{c: "Optical & Medical Instruments" for c in range(90, 93)},
    **{c: "Arms & Ammunition" for c in range(93, 94)},
    **{c: "Furniture & Misc. Manufactures" for c in range(94, 97)},
    **{c: "Other" for c in range(97, 100)},
}

# More specific chapter labels for the drill-down level
HS_CHAPTER_LABELS = {
    1:"Live Animals", 2:"Meat", 3:"Fish & Seafood", 4:"Dairy & Eggs",
    5:"Other Animal Products", 6:"Plants & Flowers", 7:"Vegetables",
    8:"Fruit & Nuts", 9:"Coffee, Tea & Spices", 10:"Cereals & Grains",
    11:"Milling Products", 12:"Oil Seeds", 13:"Gums & Resins",
    14:"Vegetable Materials", 15:"Fats & Oils", 16:"Prepared Meats",
    17:"Sugar & Confectionery", 18:"Cocoa & Chocolate", 19:"Baked Goods",
    20:"Prepared Vegetables", 21:"Misc. Food Preparations", 22:"Beverages & Spirits",
    23:"Animal Feed", 24:"Tobacco", 25:"Salt, Stone & Cement",
    26:"Ores & Minerals", 27:"Mineral Fuels & Oil", 28:"Inorganic Chemicals",
    29:"Organic Chemicals", 30:"Pharmaceuticals", 31:"Fertilizers",
    32:"Dyes & Pigments", 33:"Cosmetics & Perfumes", 34:"Soaps & Cleaners",
    35:"Enzymes & Starches", 36:"Explosives", 37:"Photographic Products",
    38:"Misc. Chemicals", 39:"Plastics", 40:"Rubber",
    41:"Hides & Skins", 42:"Leather Goods", 43:"Furs",
    44:"Wood & Lumber", 45:"Cork", 46:"Basketwork",
    47:"Pulp & Waste Paper", 48:"Paper & Paperboard", 49:"Printed Materials",
    50:"Silk", 51:"Wool", 52:"Cotton",
    53:"Other Vegetable Fibers", 54:"Man-made Filaments", 55:"Man-made Staple Fibers",
    56:"Wadding & Felt", 57:"Carpets", 58:"Special Woven Fabrics",
    59:"Impregnated Textiles", 60:"Knitted Fabrics", 61:"Knitted Apparel",
    62:"Woven Apparel", 63:"Other Textile Articles", 64:"Footwear",
    65:"Headgear", 66:"Umbrellas", 67:"Feathers & Artificial Flowers",
    68:"Stone & Ceramic Articles", 69:"Ceramic Products", 70:"Glass",
    71:"Precious Stones & Metals", 72:"Iron & Steel", 73:"Steel Articles",
    74:"Copper", 75:"Nickel", 76:"Aluminum",
    77:"Reserved", 78:"Lead", 79:"Zinc",
    80:"Tin", 81:"Other Base Metals", 82:"Tools & Cutlery",
    83:"Misc. Metal Articles", 84:"Machinery & Equipment", 85:"Electronics & Electrical Equip.",
    86:"Railway Equipment", 87:"Vehicles & Auto Parts", 88:"Aircraft & Spacecraft",
    89:"Ships & Boats", 90:"Optical & Medical Instruments", 91:"Clocks & Watches",
    92:"Musical Instruments", 93:"Arms & Ammunition", 94:"Furniture",
    95:"Toys & Games", 96:"Misc. Manufactures", 97:"Art & Antiques",
}

# ── COMTRADE API ──────────────────────────────────────────────────────────────
COMTRADE_BASE = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"

import requests

def get_json(url, params=None, retries=3, delay=2):
    headers = {}
    if COMTRADE_KEY:
        headers["Ocp-Apim-Subscription-Key"] = COMTRADE_KEY
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=25)
            if r.status_code == 429:
                log.warning("Rate limited — waiting 60s")
                time.sleep(60)
                continue
            r.raise_for_status()
            time.sleep(0.6)
            return r.json()
        except requests.RequestException as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    log.error(f"All retries failed for {url}")
    return None


def fetch_partners(iso3, flow="X", year=REFERENCE_YEAR):
    """Fetch top 10 bilateral partner flows for exports (X) or imports (M)."""
    m49 = ISO3_TO_M49.get(iso3)
    if not m49:
        return {}
    params = {
        "reporterCode": m49,
        "period": year,
        "partnerCode": "0",
        "cmdCode": "TOTAL",
        "flowCode": flow,
        "maxRecords": 15,
        "format": "JSON",
        "aggregateBy": "partnerCode",
        "breakdownMode": "plus",
    }
    data = get_json(COMTRADE_BASE, params=params)
    if not data or "data" not in data:
        return {}
    result = {}
    for row in data["data"]:
        pc = row.get("partnerCode")
        if not pc or pc in (0, "0", 896, "896"):  # Skip "World" and "Areas NES"
            continue
        result[pc] = {
            "name": row.get("partnerDesc", ""),
            "value": round((row.get("primaryValue") or 0) / 1e9, 2)
        }
    return result


def fetch_commodities(iso3, flow="X", year=REFERENCE_YEAR):
    """
    Fetch commodity breakdown by HS chapter for a country.
    Returns aggregated section-level data with chapter drill-down.
    """
    m49 = ISO3_TO_M49.get(iso3)
    if not m49:
        return {}
    params = {
        "reporterCode": m49,
        "period": year,
        "partnerCode": "0",      # All partners combined
        "cmdCode": "AG2",        # HS 2-digit chapter level aggregation
        "flowCode": flow,
        "maxRecords": 150,
        "format": "JSON",
        "aggregateBy": "cmdCode",
        "breakdownMode": "plus",
    }
    data = get_json(COMTRADE_BASE, params=params)
    if not data or "data" not in data:
        return {}

    chapters = {}
    for row in data["data"]:
        cmd = row.get("cmdCode", "")
        try:
            ch = int(cmd)
        except (ValueError, TypeError):
            continue
        val = (row.get("primaryValue") or 0) / 1e9
        if val > 0:
            chapters[ch] = {
                "label": HS_CHAPTER_LABELS.get(ch, f"HS Chapter {ch}"),
                "section": HS_CHAPTER_TO_SECTION.get(ch, "Other"),
                "value": round(val, 2)
            }
    return chapters


def chapters_to_commodities(chapters, total_val):
    """
    Convert raw chapter data into the section-level list format the app expects,
    with chapter-level drill-down stored in commodity_sub.
    Format: commodities = [["Section name", pct], ...]
    commodity_sub = {"Section name": [["Chapter label", pct], ...]}
    """
    if not chapters or total_val <= 0:
        return [], {}

    # Aggregate chapters into sections
    sections = {}
    for ch, data in chapters.items():
        sec = data["section"]
        if sec not in sections:
            sections[sec] = {"value": 0, "chapters": []}
        sections[sec]["value"] += data["value"]
        sections[sec]["chapters"].append((data["label"], data["value"]))

    # Sort sections by value descending
    sorted_sections = sorted(sections.items(), key=lambda x: x[1]["value"], reverse=True)

    # Build commodities list (top 10 sections + Other)
    commodities = []
    commodity_sub = {}
    other_pct = 0

    for i, (sec_name, sec_data) in enumerate(sorted_sections):
        pct = round(sec_data["value"] / total_val * 100, 1)
        if pct < 0.5:
            other_pct += pct
            continue
        if i < 10:
            commodities.append([sec_name, pct])
            # Build drill-down for this section
            chap_sorted = sorted(sec_data["chapters"], key=lambda x: x[1], reverse=True)
            sub = []
            other_ch_pct = 0
            for ch_label, ch_val in chap_sorted[:8]:
                ch_pct = round(ch_val / sec_data["value"] * 100, 1)
                if ch_pct >= 1:
                    sub.append([ch_label, ch_pct])
                else:
                    other_ch_pct += ch_pct
            if other_ch_pct > 0:
                sub.append(["Other", round(other_ch_pct, 1)])
            if sub:
                commodity_sub[sec_name] = sub
        else:
            other_pct += pct

    # Normalize percentages to sum to ~100
    total_pct = sum(p for _, p in commodities)
    if total_pct > 0 and total_pct != 100:
        commodities = [[n, round(p / total_pct * 100, 1)] for n, p in commodities]

    if other_pct > 0.5:
        commodities.append(["Other", round(other_pct, 1)])

    return commodities, commodity_sub


# ── PARTNER MERGING ───────────────────────────────────────────────────────────
M49_TO_ISO3 = {v: k for k, v in ISO3_TO_M49.items()}

# Common country names from Comtrade that differ from our display names
COMTRADE_NAME_MAP = {
    "United States of America": "United States",
    "China, mainland": "China",
    "Rep. of Korea": "South Korea",
    "Russian Federation": "Russia",
    "United Kingdom": "United Kingdom",
    "Viet Nam": "Vietnam",
    "Czechia": "Czechia",
    "Türkiye": "Turkey",
    "Iran (Islamic Rep. of)": "Iran",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Syrian Arab Republic": "Syria",
    "Dem. Rep. of the Congo": "DR Congo",
    "United Rep. of Tanzania": "Tanzania",
    "Lao People's Dem. Rep.": "Laos",
}

def merge_partners(exp_data, imp_data, top_n=10):
    """Combine export and import partner data into unified partner list."""
    all_codes = set(exp_data) | set(imp_data)
    partners = []
    for pc in all_codes:
        iso3 = M49_TO_ISO3.get(int(pc) if isinstance(pc, (str, int)) else pc)
        if not iso3:
            continue
        exp_val = exp_data.get(pc, {}).get("value", 0)
        imp_val = imp_data.get(pc, {}).get("value", 0)
        raw_name = (exp_data.get(pc) or imp_data.get(pc) or {}).get("name", iso3)
        clean_name = COMTRADE_NAME_MAP.get(raw_name, raw_name)
        partners.append({
            "iso": iso3,
            "name": clean_name,
            "exp": exp_val,
            "imp": imp_val
        })
    partners.sort(key=lambda x: x["exp"] + x["imp"], reverse=True)
    return partners[:top_n]


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
    "IRE":"IE","BGO":"BE","IRQ":"IQ","DZA":"DZ",
}

WB_BASE = "https://api.worldbank.org/v2/country/{iso2}/indicator/{indicator}"

def fetch_wb_totals(iso3):
    """Fetch export/import totals from World Bank as fallback."""
    iso2 = ISO3_TO_ISO2.get(iso3)
    if not iso2:
        return None
    out = {"source": "world_bank", "estimated": True}
    for indicator, key in [
        ("NE.EXP.GNFS.CD", "exports"),
        ("NE.IMP.GNFS.CD", "imports"),
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


# ── COUNTRY DISPLAY NAMES ─────────────────────────────────────────────────────
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
    "TTO":"Trinidad & Tobago","PNG":"Papua New Guinea","IRE":"Ireland","BGO":"Belgium",
}

WTO_ACCESSION = {
    "CHN":2001,"RUS":2012,"TWN":2002,"SAU":2005,"VNM":2007,"KAZ":2015,
    "UKR":2008,"SRB":2013,"GEO":2000,"AZE":None,"BLR":None,"UZB":None,
    "DZA":None,"IRQ":None,"IRE":1995,"BGO":1995,
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
    "IRE":[-8.24,53.41],"BGO":[4.47,50.50],
}


# ── MAIN PIPELINE ─────────────────────────────────────────────────────────────
def load_data():
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE) as f:
            return json.load(f)
    return {"_meta": {}, "_fetch_state": {}, "countries": {}}


def save_data(d):
    with open(OUTPUT_FILE, "w") as f:
        json.dump(d, f, indent=2, ensure_ascii=False)


def get_next_batch(fetch_state):
    """
    Returns the next batch of countries to fetch.
    Tracks which countries were updated and when, always picking
    the ones updated least recently (or never).
    """
    last_updated = {iso: fetch_state.get(iso, {}).get("last_updated", "1970-01-01")
                    for iso in TOP_100}
    sorted_by_age = sorted(last_updated.items(), key=lambda x: x[1])
    return [iso for iso, _ in sorted_by_age[:COUNTRIES_PER_DAY]]


def run():
    log.info("=== Daily Comtrade Fetcher ===")
    d = load_data()
    fetch_state = d.get("_fetch_state", {})
    batch = get_next_batch(fetch_state)
    log.info(f"Fetching {len(batch)} countries today: {', '.join(batch[:10])}...")

    req_count = 0
    updated = 0

    for iso in batch:
        if req_count + REQS_PER_COUNTRY > DAILY_REQ_LIMIT:
            log.info(f"Request limit reached ({req_count}). Stopping.")
            break

        log.info(f"[{req_count}/{DAILY_REQ_LIMIT}] Fetching {iso} ({COUNTRY_NAMES.get(iso, iso)})...")

        # ── Step 1: Partner flows ──────────────────────────────────────────
        exp_partners = fetch_partners(iso, "X")
        req_count += 1
        imp_partners = fetch_partners(iso, "M")
        req_count += 1

        # ── Step 2: Commodity breakdown ────────────────────────────────────
        exp_chapters = fetch_commodities(iso, "X")
        req_count += 1
        # We use export commodities for the main breakdown (most informative)
        # Import commodity breakdown stored separately if needed later
        req_count += 1  # Reserve slot even if we skip to stay consistent

        # ── Step 3: Get trade totals ───────────────────────────────────────
        # Calculate from Comtrade data first, fall back to World Bank
        exp_total = sum(v["value"] for v in exp_partners.values()) if exp_partners else None
        imp_total = sum(v["value"] for v in imp_partners.values()) if imp_partners else None

        # If Comtrade totals look too small (only top partners, not world total)
        # use World Bank for the headline numbers
        wb = fetch_wb_totals(iso)
        if wb and wb.get("exports"):
            exports = wb["exports"]
            imports = wb["imports"]
            source = "world_bank+comtrade"
            ref_year = wb.get("year", REFERENCE_YEAR)
        elif exp_total:
            exports = round(exp_total, 2)
            imports = round(imp_total or 0, 2)
            source = "un_comtrade"
            ref_year = REFERENCE_YEAR
        else:
            # Keep existing data
            existing = d["countries"].get(iso, {})
            exports = existing.get("exports")
            imports = existing.get("imports")
            source = existing.get("source", "seed")
            ref_year = existing.get("reference_year", REFERENCE_YEAR)

        # ── Step 4: Build commodity lists ──────────────────────────────────
        if exp_chapters and exports:
            commodities, commodity_sub = chapters_to_commodities(exp_chapters, exports)
        else:
            # Keep existing commodity data
            existing = d["countries"].get(iso, {})
            commodities = existing.get("commodities", [])
            commodity_sub = existing.get("commodity_sub", {})

        # ── Step 5: Build partner list ─────────────────────────────────────
        if exp_partners or imp_partners:
            partners = merge_partners(exp_partners, imp_partners, top_n=10)
        else:
            existing = d["countries"].get(iso, {})
            partners = existing.get("partners", [])

        # ── Step 6: Preserve static data (FTAs, disputes) ─────────────────
        existing = d["countries"].get(iso, {})

        # ── Step 7: Write to data structure ───────────────────────────────
        d["countries"][iso] = {
            "name":           COUNTRY_NAMES.get(iso, iso),
            "iso2":           ISO3_TO_ISO2.get(iso, ""),
            "wto_member":     iso not in (WTO_ACCESSION) or WTO_ACCESSION.get(iso) is not None,
            "wto_accession":  WTO_ACCESSION.get(iso, 1995),
            "centroid":       CENTROIDS.get(iso, existing.get("centroid")),
            "total_trade":    round((exports or 0) + (imports or 0), 2),
            "exports":        exports,
            "imports":        imports,
            "source":         source,
            "reference_year": ref_year,
            "estimated":      source == "seed",
            "commodities":    commodities,
            "commodity_sub":  commodity_sub,
            "partners":       partners,
            # Preserve manually maintained fields
            "ftas":           existing.get("ftas", []),
            "disputes":       existing.get("disputes", []),
        }

        # Track fetch state
        fetch_state[iso] = {
            "last_updated": datetime.now(timezone.utc).date().isoformat(),
            "source": source,
            "partners_count": len(partners),
            "commodity_sections": len(commodities),
        }
        updated += 1
        log.info(f"  ✓ {iso}: {len(partners)} partners, {len(commodities)} commodity sections")

    # Update meta
    d["_fetch_state"] = fetch_state
    d["_meta"] = {
        **d.get("_meta", {}),
        "last_daily_run": datetime.now(timezone.utc).isoformat(),
        "countries_updated_today": updated,
        "total_countries": len(d["countries"]),
        "requests_used": req_count,
        "methodology": (
            "Trade totals sourced from World Bank API (NE.EXP/IMP.GNFS.CD). "
            "Bilateral partner flows and commodity breakdowns sourced from UN Comtrade "
            f"public API (HS chapter level, AG2 aggregation, reference year {REFERENCE_YEAR}). "
            "Commodity sections mapped from HS chapters. FTAs and disputes maintained manually."
        ),
        "data_sources": [
            {
                "id": "world_bank",
                "name": "World Bank Open Data API",
                "url": "https://api.worldbank.org/v2/",
                "description": "Trade totals (exports, imports of goods and services, current USD)",
                "coverage": "1960–2023",
                "license": "CC BY 4.0",
                "citation": "World Bank (2024). World Development Indicators. https://databank.worldbank.org"
            },
            {
                "id": "un_comtrade",
                "name": "UN Comtrade API",
                "url": "https://comtradeapi.un.org/",
                "description": "Bilateral trade flows and commodity breakdown by HS chapter (AG2 level)",
                "coverage": "1962–2023",
                "license": "UN open data (free tier: 250 req/day)",
                "citation": "United Nations Statistics Division (2024). UN Comtrade Database. https://comtrade.un.org"
            }
        ],
        "reference_year": REFERENCE_YEAR,
        "value_units": "USD billions (current)",
    }

    save_data(d)
    log.info(f"✓ Done. Updated {updated} countries. {req_count} requests used.")

    # Print cycle progress
    completed = sum(1 for iso in TOP_100 if iso in fetch_state)
    log.info(f"Cycle progress: {completed}/{len(TOP_100)} countries have been fetched at least once")
    never_fetched = [iso for iso in TOP_100 if iso not in fetch_state]
    if never_fetched:
        log.info(f"Not yet fetched: {', '.join(never_fetched[:20])}")


if __name__ == "__main__":
    run()
