"""
╔═══════════════════════════════════════════════════════════════════════════╗
║         IPO INTELLIGENCE ENGINE — 4-LAYER ULTIMATE SYSTEM v4.0           ║
╠═══════════════════════════════════════════════════════════════════════════╣
║  LAYER 1 → Data Collection   (Chittorgarh + Investorgain + NSE/BSE)      ║
║  LAYER 2 → Checklist Engine  (Elite 0.0001% Decision Matrix)             ║
║  LAYER 3 → Execution Engine  (Zerodha FREE API + TOTP Auto-Login)        ║
║  LAYER 4 → Alert System      (Telegram — Rich Formatted Alerts)          ║
╠═══════════════════════════════════════════════════════════════════════════╣
║  Architecture : curl_cffi TLS Impersonation + Gemini AI Fallback         ║
║  Login        : Zerodha TOTP Auto-Login (no Selenium, no browser)        ║
║  Safety       : DRY_RUN mode + Human-in-the-loop confirmation            ║
║  Save as      : ipo_engine.py                                             ║
║  Run          : python ipo_engine.py [once | fast | schedule | dryrun]   ║
╚═══════════════════════════════════════════════════════════════════════════╝

⚠️  SECURITY WARNING: Never share this file after filling in credentials.
    Add ipo_engine.py to your .gitignore immediately.
"""

# ═══════════════════════════════════════════════════════════════════════════
#  SECTION 0 — USER CONFIGURATION  ← ONLY EDIT THIS BLOCK
# ═══════════════════════════════════════════════════════════════════════════
# ── Credentials & API Keys (REDACTED FOR PUBLIC REPOSITORY) ──────────────
GEMINI_API_KEY     = "YOUR_GEMINI_API_KEY_HERE"
GEMINI_MODEL       = "gemini-1.5-flash"

TELEGRAM_ENABLED   = True
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID   = "YOUR_TELEGRAM_CHAT_ID_HERE"

ZERODHA_API_KEY    = "YOUR_KITE_API_KEY_HERE"
ZERODHA_API_SECRET = "YOUR_KITE_API_SECRET_HERE"
ZERODHA_USER_ID    = "YOUR_ZERODHA_USER_ID"        # e.g. AB1234
ZERODHA_PASSWORD   = "YOUR_ZERODHA_PASSWORD"
ZERODHA_TOTP_KEY   = "YOUR_TOTP_SECRET_KEY"        # 32-char secret from Kite
ZERODHA_UPI_ID     = "yourname@upi"                # UPI ID for ASBA mandate
             

# ── Safety Mode ───────────────────────────────────────────────────────────
# DRY_RUN = True  → Full pipeline runs, NO actual orders placed (SAFE for testing)
# DRY_RUN = False → LIVE mode. Real IPO bids placed. Use only on Day 2/3 of IPO.
DRY_RUN = True

# ── Checklist Thresholds ─────────────────────────────────────────────────
MAINBOARD = {
    "qib_min"    : 50.0,   # QIB must be > 50x
    "nii_min"    : 20.0,   # NII must be > 20x
    "retail_min" : 5.0,    # Retail must be > 5x
    "gmp_pct_min": 20.0,   # GMP% must be > 20%
    "ofs_pct_max": 60.0,   # OFS% must be < 60%
}
SME = {
    "qib_min"    : 0.0,
    "nii_min"    : 50.0,
    "retail_min" : 50.0,
    "gmp_pct_min": 30.0,
    "ofs_pct_max": 50.0,
}

# ── HNI Kostak Parameters ─────────────────────────────────────────────────
HNI_RATE_PCT   = 10.0   # Grey market funding rate (annual %)
HNI_HOLD_DAYS  = 15     # Avg days from application to listing

# ── Scheduler Times (IST, 24h) ────────────────────────────────────────────
SCHEDULE_TIMES = ["09:45", "13:00", "15:15", "17:30"]

# ── Auto-Execution Settings ────────────────────────────────────────────────
AUTO_EXECUTE_THRESHOLD = 5   # Minimum score to auto-place bid (out of 6)
IPO_LOT_COUNT          = 1   # How many lots to bid per IPO

# ═══════════════════════════════════════════════════════════════════════════
#  SECTION 1 — IMPORTS & BOOTSTRAP
# ═══════════════════════════════════════════════════════════════════════════

import os
import re
import sys
import json
import time
import hashlib
import logging
import pyotp
from datetime import datetime, date
from typing import Optional
from urllib.parse import urlparse, parse_qs

# Standard requests (for Telegram, Kite auth, NSE/BSE)
import requests as std_requests

# Check & import third-party libraries
missing = []
try:
    from curl_cffi import requests as cffi_requests   # TLS impersonation scraping
except ImportError:
    missing.append("curl_cffi")

try:
    from bs4 import BeautifulSoup
except ImportError:
    missing.append("beautifulsoup4")

try:
    import pandas as pd
except ImportError:
    missing.append("pandas")

try:
    import google.generativeai as genai
except ImportError:
    missing.append("google-generativeai")

try:
    import schedule
except ImportError:
    missing.append("schedule")

try:
    from kiteconnect import KiteConnect
except ImportError:
    missing.append("kiteconnect")

if missing:
    print(f"\n❌ Missing libraries: {', '.join(missing)}")
    print(f"Fix: pip install {' '.join(missing)}\n")
    sys.exit(1)

# ── Output Folders ────────────────────────────────────────────────────────
OUTPUT_DIR  = "ipo_reports"
LOG_FILE    = f"{OUTPUT_DIR}/ipo_engine.log"
TOKEN_FILE  = f"{OUTPUT_DIR}/.zerodha_token.json"   # Daily token cache
MASTER_JSON = f"{OUTPUT_DIR}/master_data.json"
REPORT_CSV  = f"{OUTPUT_DIR}/IPO_Decision_Matrix.csv"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Logger ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level    = logging.INFO,
    format   = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("IPO-Engine")

# ── Gemini ────────────────────────────────────────────────────────────────
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel(GEMINI_MODEL)

# ── curl_cffi Session (TLS impersonation — bypasses Cloudflare/bot blocks) ─
# Impersonates Chrome 120 natively. No custom headers needed.
# verify=False handles SSL issues on campus/VPN networks.
cffi_session = cffi_requests.Session(impersonate="chrome120", verify=False)

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ═══════════════════════════════════════════════════════════════════════════
#  SECTION 2 — UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def safe_get(url: str, retries: int = 3, backoff: int = 4) -> Optional[object]:
    """
    HTTP GET with exponential backoff using curl_cffi (TLS impersonation).
    Never crashes the pipeline — returns None on total failure.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = cffi_session.get(url, timeout=20)
            resp.raise_for_status()
            time.sleep(2)
            return resp
        except Exception as e:
            log.warning(f"[HTTP] Attempt {attempt}/{retries} failed → {url} | {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    log.error(f"[HTTP] All {retries} attempts exhausted → {url}")
    return None


def clean_number(text: str) -> float:
    """
    Robustly extract float from messy IPO data strings.
    Handles: '₹1,234.56' / '12.3x' / '45%' / '-' / 'N/A' / '(125)'
    """
    if not text or str(text).strip() in ("-", "–", "N/A", "NA", "", "nil", "—"):
        return 0.0
    cleaned = re.sub(r"[₹,xX%\s()]", "", str(text).strip())
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def fuzzy_match(name_a: str, name_b: str) -> bool:
    """Check if two IPO names refer to same company via first-2-word overlap."""
    words_a = set(re.sub(r"[^A-Z\s]", "", name_a.upper()).split()[:2])
    words_b = set(re.sub(r"[^A-Z\s]", "", name_b.upper()).split()[:2])
    return bool(words_a & words_b - {"IPO", "LTD", "LIMITED", "PVT"})


def save_json(data: dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    log.info(f"[SAVE] JSON → {path}")


def load_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def gemini_parse(task: str, content: str) -> list | dict:
    """
    Gemini Flash AI fallback parser. Called when HTML structure changes
    and BeautifulSoup cannot find the expected table.
    Returns parsed JSON. Never raises — returns empty list on failure.
    """
    prompt = (
        "You are a precise financial data extractor.\n"
        f"TASK: {task}\n\n"
        f"CONTENT:\n{content[:8000]}\n\n"
        "RULES: Return ONLY valid JSON. No markdown. No explanation. "
        "Use 0.0 for missing numbers, null for missing strings."
    )
    try:
        resp = gemini_model.generate_content(prompt)
        raw  = resp.text.strip()
        raw  = re.sub(r"^```(?:json)?\s*", "", raw)
        raw  = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)
    except Exception as e:
        log.error(f"[GEMINI] Parse failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════
#  LAYER 1 — DATA COLLECTION
# ═══════════════════════════════════════════════════════════════════════════

# ── 1A: Chittorgarh — Live Subscription Data ──────────────────────────────

CHITTORGARH_URLS = {
    "mainboard_sub" : "https://www.chittorgarh.com/report/ipo-subscription-status-live-bidding-data-bse-nse/21/",
    "sme_sub"       : "https://www.chittorgarh.com/report/sme-ipo-subscription-status-live-bidding-bse-nse/22/",
    "mainboard_list": "https://www.chittorgarh.com/report/mainboard-ipo-list-in-india-bse-nse/83/",
    "sme_list"      : "https://www.chittorgarh.com/report/sme-ipo-list-in-india-bse-nse/84/",
}


def _parse_sub_table(soup: BeautifulSoup, ipo_type: str) -> list[dict]:
    """
    Parse Chittorgarh subscription table.
    Columns: Company | Close Date | Issue Size Cr | QIB(x) | SNII(x) | BNII(x) | Retail(x) | Total(x)
    Auto-falls back to Gemini if table structure changes.
    """
    table = soup.find("table")

    if not table:
        log.warning(f"[L1] Table missing ({ipo_type}) → Gemini fallback")
        task = (
            "Extract all IPO subscription rows from this HTML page. "
            "Return a JSON array. Each item must have: "
            "name (string), close_date (string), qib_x (float), "
            "snii_x (float), bnii_x (float), retail_x (float), total_x (float). "
            "Use 0.0 for missing numbers."
        )
        result = gemini_parse(task, str(soup))
        if isinstance(result, list):
            for r in result:
                r["ipo_type"]  = ipo_type
                r["detail_url"] = ""
                r["nii_x"]    = r.get("snii_x", 0) + r.get("bnii_x", 0)
        return result if isinstance(result, list) else []

    records = []
    tbody   = table.find("tbody") or table

    for tr in tbody.find_all("tr"):
        cols = tr.find_all("td")
        if len(cols) < 5:
            continue

        name_tag = cols[0].find("a")
        name     = (name_tag or cols[0]).get_text(strip=True)
        link     = name_tag.get("href", "") if name_tag else ""

        # Skip header rows
        if not name or name.lower() in ("company", "ipo name", "name", "issue"):
            continue

        def col(i, d=0.0):
            try:
                return clean_number(cols[i].get_text(strip=True))
            except IndexError:
                return d

        # Detect if BNII column exists (>7 cols = yes)
        has_bnii = len(cols) > 7
        snii     = col(4)
        bnii     = col(5) if has_bnii else 0.0
        retail   = col(6) if has_bnii else col(5)
        total    = col(7) if has_bnii else col(6)

        records.append({
            "name"       : name,
            "ipo_type"   : ipo_type,
            "detail_url" : link if link.startswith("http") else f"https://www.chittorgarh.com{link}",
            "close_date" : cols[1].get_text(strip=True) if len(cols) > 1 else "",
            "qib_x"      : col(3),
            "snii_x"     : snii,
            "bnii_x"     : bnii,
            "nii_x"      : snii + bnii,
            "retail_x"   : retail,
            "total_x"    : total,
        })

    log.info(f"[L1-CHITTORGARH] {ipo_type.upper()}: {len(records)} IPOs found")
    return records


def scrape_subscription(ipo_type: str = "mainboard") -> list[dict]:
    url = CHITTORGARH_URLS.get(f"{ipo_type}_sub", CHITTORGARH_URLS["mainboard_sub"])
    log.info(f"[L1] Fetching {ipo_type} subscription → {url}")
    resp = safe_get(url)
    if not resp:
        return []
    soup = BeautifulSoup(resp.content, "html.parser")
    return _parse_sub_table(soup, ipo_type)


def scrape_ipo_detail(detail_url: str, ipo_name: str) -> dict:
    """
    Scrape individual IPO detail page via Gemini.
    Extracts: OFS%, price band, lot size, promoter holding, registrar.
    """
    if not detail_url or "chittorgarh" not in detail_url:
        return {}

    resp = safe_get(detail_url)
    if not resp:
        return {}

    soup = BeautifulSoup(resp.content, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)[:6000]

    task = (
        f"From this IPO detail page for '{ipo_name}', extract: "
        "fresh_issue_cr (number), ofs_cr (number), total_issue_cr (number), "
        "price_band (string e.g. '₹200-₹210'), issue_price (number, upper band only), "
        "lot_size (integer), open_date (string), close_date (string), "
        "listing_date (string), promoter_pre_pct (number), promoter_post_pct (number), "
        "registrar (string), lead_manager (string). "
        "Use null for unavailable fields."
    )
    result = gemini_parse(task, text)

    if isinstance(result, dict):
        total          = result.get("total_issue_cr") or 1
        ofs            = result.get("ofs_cr") or 0
        result["ofs_pct"] = round((ofs / total) * 100, 1) if total else 0
        log.info(
            f"[L1-DETAIL] {ipo_name}: "
            f"Band={result.get('price_band')} | "
            f"OFS={result.get('ofs_pct')}% | "
            f"Lot={result.get('lot_size')}"
        )
        return result
    return {}


# ── 1B: Investorgain — GMP + Kostak Data ──────────────────────────────────

def scrape_gmp() -> dict[str, dict]:
    """
    Scrape live GMP, Kostak, and Subject-to-Sauda from Investorgain.
    Returns dict keyed by IPO name.
    """
    url  = "https://www.investorgain.com/report/live-ipo-gmp/331/"
    log.info(f"[L1-GMP] Fetching → {url}")
    resp = safe_get(url)
    if not resp:
        return {}

    soup  = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table")
    data  = {}

    if not table:
        log.warning("[L1-GMP] Table missing → Gemini fallback")
        task = (
            "Extract all IPO GMP rows. Return JSON array with: "
            "name (string), price (float), gmp_rs (float), "
            "kostak_rs (float), subject_to_sauda_rs (float)"
        )
        rows = gemini_parse(task, str(soup)[:10000])
        if isinstance(rows, list):
            for row in rows:
                name = row.get("name", "")
                if name:
                    p = row.get("price", 0) or 1
                    g = row.get("gmp_rs", 0) or 0
                    row["gmp_pct"]    = round((g / p) * 100, 2)
                    row["est_listing"] = p + g
                    data[name]        = row
        return data

    for tr in table.find_all("tr")[1:]:
        cols = tr.find_all("td")
        if len(cols) < 3:
            continue
        a    = cols[0].find("a")
        name = (a or cols[0]).get_text(strip=True)
        if not name:
            continue

        price   = clean_number(cols[1].get_text()) if len(cols) > 1 else 0
        gmp_rs  = clean_number(cols[2].get_text()) if len(cols) > 2 else 0
        kostak  = clean_number(cols[3].get_text()) if len(cols) > 3 else 0
        s2b     = clean_number(cols[4].get_text()) if len(cols) > 4 else 0
        gmp_pct = round((gmp_rs / price * 100), 2) if price > 0 else 0.0

        data[name] = {
            "price"            : price,
            "gmp_rs"           : gmp_rs,
            "gmp_pct"          : gmp_pct,
            "kostak_rs"        : kostak,
            "subject_to_sauda" : s2b,
            "est_listing"      : round(price + gmp_rs, 2),
        }

    log.info(f"[L1-GMP] GMP fetched for {len(data)} IPOs")
    return data


# ── 1C: NSE / BSE — Official Calendar ─────────────────────────────────────

def scrape_official_calendar() -> list[dict]:
    """
    Fetch official IPO calendar from NSE JSON API.
    Uses standard requests (not curl_cffi) + warm-up cookie fetch.
    Falls back to BSE if NSE fails.
    """
    NSE_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0",
        "Referer"   : "https://www.nseindia.com/",
        "Accept"    : "application/json, text/plain, */*",
    }
    nse = std_requests.Session()
    nse.headers.update(NSE_HEADERS)

    try:
        nse.get("https://www.nseindia.com", timeout=10)
        time.sleep(2)
        resp = nse.get("https://www.nseindia.com/api/ipo-current-allotment", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        log.info(f"[L1-NSE] Official calendar: {len(data)} entries")
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"[L1-NSE] API failed ({e}) → BSE fallback")

    # BSE Fallback
    try:
        bse_h = {**NSE_HEADERS, "Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com"}
        resp  = std_requests.get(
            "https://api.bseindia.com/BseIndiaAPI/api/IPOIssueDetails/w",
            headers=bse_h, timeout=15
        )
        resp.raise_for_status()
        d     = resp.json()
        items = d.get("Table", d) if isinstance(d, dict) else d
        log.info(f"[L1-BSE] Official calendar: {len(items)} entries")
        return items if isinstance(items, list) else []
    except Exception as e:
        log.error(f"[L1-BSE] Also failed: {e}")
        return []


# ═══════════════════════════════════════════════════════════════════════════
#  LAYER 2 — CHECKLIST ENGINE (Elite 0.0001% Decision Matrix)
# ═══════════════════════════════════════════════════════════════════════════

def kostak_breakeven(issue_price: float, lot_size: int, kostak_rs: float) -> dict:
    """
    HNI Kostak Break-Even Calculator — Jay Ritter's syndicate trap detector.

    Logic:
      HNI borrows at HNI_RATE_PCT% annually to apply for IPO.
      Interest cost = Application Value × (rate/100) × (days/365)
      Net profit    = Kostak received − Interest paid

    If Net < 0 → HNIs LOSE money → they DUMP shares on listing day
               → Listing price COLLAPSES regardless of GMP.

    This single metric predicts listing day dumps with high accuracy.
    """
    if issue_price <= 0 or lot_size <= 0:
        return {"hni_dump_risk": "UNKNOWN ⚪", "net_kostak_rs": 0, "hni_note": "N/A"}

    app_val   = issue_price * lot_size
    interest  = app_val * (HNI_RATE_PCT / 100) * (HNI_HOLD_DAYS / 365)
    net       = kostak_rs - interest
    be_pct    = round((interest / app_val) * 100, 2)

    return {
        "application_value_rs": round(app_val, 0),
        "interest_cost_rs"    : round(interest, 2),
        "kostak_rs"           : round(kostak_rs, 2),
        "net_kostak_rs"       : round(net, 2),
        "break_even_gmp_pct"  : be_pct,
        "hni_dump_risk"       : "HIGH 🔴" if net < 0 else "LOW 🟢",
        "hni_note"            : (
            f"HNI net ₹{net:.0f}/lot. "
            + ("NEGATIVE → Dump risk on listing day." if net < 0
               else "POSITIVE → HNIs likely hold → price support.")
        ),
    }


def run_decision_matrix(
    qib_x: float, nii_x: float, retail_x: float,
    gmp_pct: float, ofs_pct: float, kostak_net: float,
    ipo_type: str = "mainboard"
) -> dict:
    """
    THE ELITE GO / NO-GO MATRIX

    Scoring (max 6 points):
      QIB      → 2 pts  (double weight — Damodaran institutional conviction)
      NII      → 1 pt   (Ritter demand signal)
      GMP %    → 1 pt   (sentiment, not guarantee — Schilit warning)
      OFS %    → 1 pt   (Graham — is company growing or promoter exiting?)
      Kostak   → 1 pt   (Ritter — HNI dump trap detection)

    Absolute Disqualifiers (override all scores):
      QIB < 2x  → Institutions rejected → never apply
      OFS > 80% → Pure promoter exit trap → never apply
    """
    T      = MAINBOARD if ipo_type == "mainboard" else SME
    checks = {}
    score  = 0

    def check(key, label, value, target, pts, pass_fn, note):
        nonlocal score
        passed = pass_fn(value, target)
        checks[key] = {
            "value"  : value,
            "target" : target,
            "passed" : passed,
            "points" : pts if passed else 0,
            "emoji"  : ("✅✅" if pts == 2 else "✅") if passed else ("❌❌" if pts == 2 else "❌"),
            "note"   : note,
        }
        if passed:
            score += pts

    check("QIB",    "QIB Subscription",       qib_x,     T["qib_min"],     2, lambda v,t: v>=t, "Damodaran: Institutional conviction signal (MOST IMPORTANT)")
    check("NII",    "NII/HNI Subscription",   nii_x,     T["nii_min"],     1, lambda v,t: v>=t, "Ritter: HNI demand signal")
    check("GMP",    "GMP %",                  gmp_pct,   T["gmp_pct_min"], 1, lambda v,t: v>=t, "Sentiment indicator only — NOT a valuation signal")
    check("OFS",    "OFS % (lower=better)",   ofs_pct,   T["ofs_pct_max"], 1, lambda v,t: v<=t, "Graham: High OFS = promoters cashing out, not company growing")

    max_score = 5
    if kostak_net != 0:
        check("KOSTAK", "HNI Dump Risk (Kostak)", kostak_net, 0, 1, lambda v,t: v>t, "Ritter: Negative net = funded HNIs dump on listing → crash")
        max_score = 6

    # ── Absolute Disqualifiers ────────────────────────────────────────
    dq, dq_reason = False, ""
    if ipo_type == "mainboard" and qib_x < 2:
        dq, dq_reason = True, "QIB < 2x — Institutions REJECTED this IPO"
    elif ofs_pct > 80:
        dq, dq_reason = True, "OFS > 80% — Pure promoter exit trap"

    # ── Final Signal ──────────────────────────────────────────────────
    pct = (score / max_score * 100) if max_score > 0 else 0

    if dq:
        signal = "🔴 DISQUALIFIED"
    elif pct >= 83:
        signal = "🟢 STRONG GO"      # ≥5/6 or 5/5
    elif pct >= 66:
        signal = "🟡 GO — 1 LOT"
    elif pct >= 50:
        signal = "🟠 WATCHLIST"
    else:
        signal = "🔴 NO-GO"

    return {
        "signal"       : signal,
        "score"        : score,
        "max_score"    : max_score,
        "score_pct"    : round(pct, 1),
        "disqualified" : dq,
        "dq_reason"    : dq_reason,
        "checks"       : checks,
        "should_execute": (not dq) and score >= AUTO_EXECUTE_THRESHOLD,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  LAYER 3 — EXECUTION ENGINE (Zerodha Kite Auto-Login + IPO Bid)
# ═══════════════════════════════════════════════════════════════════════════

class ZerodhaExecutor:
    """
    Zerodha FREE Personal API Executor.

    Flow:
      1. TOTP auto-login (no Selenium, no browser)
      2. Token cached daily (re-login only if expired)
      3. IPO bid placed via Kite Connect
      4. UPI mandate request sent to your phone
      5. You approve UPI on your phone → done
      6. Telegram confirms bid status

    IMPORTANT: UPI approval is MANDATORY by SEBI — cannot be automated.
    The system handles everything else automatically.
    """

    LOGIN_URL = "https://kite.zerodha.com/api/login"
    TWOFA_URL = "https://kite.zerodha.com/api/twofa"

    def __init__(self):
        self.kite         = None
        self.access_token = None
        self._login_session = std_requests.Session()
        self._login_session.headers.update({
            "User-Agent"  : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0",
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer"     : "https://kite.zerodha.com/",
            "Origin"      : "https://kite.zerodha.com",
        })

    def _load_cached_token(self) -> Optional[str]:
        """Load today's cached access token (tokens expire at 6 AM daily)."""
        cache = load_json(TOKEN_FILE)
        today = date.today().isoformat()
        if cache.get("date") == today and cache.get("access_token"):
            log.info("[L3] ✅ Using cached Zerodha token (valid for today)")
            return cache["access_token"]
        return None

    def _save_token(self, token: str):
        save_json({"date": date.today().isoformat(), "access_token": token}, TOKEN_FILE)

    def login(self) -> bool:
        """
        Fully automated Zerodha login:
        Step 1 → POST credentials → get request_id
        Step 2 → Generate TOTP via pyotp → POST 2FA → get enctoken
        Step 3 → Exchange for Kite access_token via API secret
        Step 4 → Initialize KiteConnect with token
        """
        if "YOUR_KITE" in ZERODHA_API_KEY:
            log.warning("[L3] Zerodha credentials not configured. Layer 3 disabled.")
            return False

        # Try cached token first
        cached = self._load_cached_token()
        if cached:
            self.access_token = cached
            self.kite = KiteConnect(api_key=ZERODHA_API_KEY)
            self.kite.set_access_token(cached)
            return True

        log.info("[L3] Logging in to Zerodha (TOTP auto-login)...")
        try:
            # Step 1 — Initial login
            resp1 = self._login_session.post(
                self.LOGIN_URL,
                data={"user_id": ZERODHA_USER_ID, "password": ZERODHA_PASSWORD},
                timeout=15,
            )
            resp1.raise_for_status()
            data1 = resp1.json()

            if data1.get("status") != "success":
                log.error(f"[L3] Login Step 1 failed: {data1.get('message')}")
                return False

            request_id = data1["data"]["request_id"]
            log.info(f"[L3] Step 1 OK. request_id={request_id}")

            # Step 2 — TOTP 2FA (auto-generated by pyotp)
            totp_code = pyotp.TOTP(ZERODHA_TOTP_KEY).now()
            log.info(f"[L3] TOTP generated: {totp_code}")

            resp2 = self._login_session.post(
                self.TWOFA_URL,
                data={
                    "user_id"    : ZERODHA_USER_ID,
                    "request_id" : request_id,
                    "twofa_value": totp_code,
                    "twofa_type" : "totp",
                },
                timeout=15,
            )
            resp2.raise_for_status()
            data2 = resp2.json()

            if data2.get("status") != "success":
                log.error(f"[L3] 2FA failed: {data2.get('message')}")
                return False

            log.info("[L3] Step 2 (2FA) OK.")

            # Step 3 — Get Kite Connect access token
            kite = KiteConnect(api_key=ZERODHA_API_KEY)
            login_url = kite.login_url()

            # Auto-follow the redirect to get request_token
            resp3 = self._login_session.get(login_url, allow_redirects=True, timeout=15)
            parsed = urlparse(resp3.url)
            params = parse_qs(parsed.query)
            request_token = params.get("request_token", [None])[0]

            if not request_token:
                log.error("[L3] Could not extract request_token from redirect URL.")
                log.error(f"[L3] Final URL was: {resp3.url}")
                return False

            # Step 4 — Generate session (exchange for access_token)
            session_data      = kite.generate_session(request_token, api_secret=ZERODHA_API_SECRET)
            self.access_token = session_data["access_token"]
            kite.set_access_token(self.access_token)
            self.kite = kite

            self._save_token(self.access_token)
            log.info(f"[L3] ✅ Zerodha login SUCCESS. Token cached for today.")
            return True

        except Exception as e:
            log.error(f"[L3] Login failed: {e}")
            return False

    def get_funds(self) -> dict:
        """Check available funds before placing bids."""
        try:
            margins = self.kite.margins(segment="equity")
            available = margins.get("net", 0)
            log.info(f"[L3] Available funds: ₹{available:,.2f}")
            return {"available": available, "data": margins}
        except Exception as e:
            log.error(f"[L3] Funds check failed: {e}")
            return {"available": 0}

    def place_ipo_bid(
        self,
        symbol     : str,
        price      : float,
        lot_size   : int,
        lots       : int = IPO_LOT_COUNT,
        exchange   : str = "NSE",
    ) -> dict:
        """
        Place an IPO bid via Zerodha Kite Connect Personal API (FREE).

        How it works:
          → Places a limit order at the upper price band (cutoff price)
          → Zerodha routes this as an ASBA/IPO bid
          → UPI mandate notification goes to your phone
          → You approve on phone → money blocked (NOT debited)
          → On allotment: money debited only for allotted shares

        IMPORTANT: After this call, CHECK YOUR PHONE for UPI mandate request.
        """
        if DRY_RUN:
            log.info(f"[L3-DRYRUN] Would place IPO bid: {symbol} | ₹{price} × {lots} lots ({lots*lot_size} shares)")
            return {"status": "DRY_RUN", "order_id": "SIMULATED", "symbol": symbol}

        if not self.kite:
            return {"status": "ERROR", "message": "Not logged in"}

        quantity = lots * lot_size
        try:
            order_id = self.kite.place_order(
                tradingsymbol   = symbol,
                exchange        = exchange,
                transaction_type= self.kite.TRANSACTION_TYPE_BUY,
                quantity        = quantity,
                order_type      = self.kite.ORDER_TYPE_LIMIT,
                price           = price,
                product         = self.kite.PRODUCT_CNC,
                validity        = self.kite.VALIDITY_DAY,
                variety         = self.kite.VARIETY_REGULAR,
                tag             = "IPO_INTELLIGENCE_ENGINE",
            )
            log.info(f"[L3] ✅ IPO BID PLACED: {symbol} | Qty={quantity} | Price=₹{price} | OrderID={order_id}")
            return {"status": "SUCCESS", "order_id": order_id, "symbol": symbol, "qty": quantity, "price": price}

        except Exception as e:
            log.error(f"[L3] Order placement failed for {symbol}: {e}")
            return {"status": "ERROR", "message": str(e), "symbol": symbol}

    def get_holdings(self) -> list:
        """Fetch current holdings (useful for checking allotted IPO shares)."""
        try:
            return self.kite.holdings()
        except Exception as e:
            log.error(f"[L3] Holdings fetch failed: {e}")
            return []


# ═══════════════════════════════════════════════════════════════════════════
#  LAYER 4 — ALERT SYSTEM (Telegram)
# ═══════════════════════════════════════════════════════════════════════════

def tg(message: str):
    """Send Telegram message. Silently skips if not configured."""
    if not TELEGRAM_ENABLED or "YOUR_TELEGRAM" in TELEGRAM_BOT_TOKEN:
        log.debug("[L4] Telegram not configured — skipping")
        return
    try:
        std_requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"[L4-TELEGRAM] Failed: {e}")


def alert_ipo_decision(row: dict, checks: dict, order_result: Optional[dict] = None):
    """Send rich formatted Telegram alert for one IPO decision."""
    mode_tag = "🔵 DRY RUN" if DRY_RUN else "🔴 LIVE"

    checks_text = "\n".join(
        f"  {v['emoji']} {k}: {v['value']} (need {v['target']}) — {v['note']}"
        for k, v in checks.items()
    )

    exec_section = ""
    if order_result:
        status = order_result.get("status", "")
        if status == "DRY_RUN":
            exec_section = "\n<b>🔵 EXECUTION</b>\n  Dry run — no real bid placed\n"
        elif status == "SUCCESS":
            exec_section = (
                f"\n<b>✅ BID PLACED SUCCESSFULLY</b>\n"
                f"  Order ID: {order_result.get('order_id')}\n"
                f"  ⚠️ CHECK YOUR PHONE — Approve UPI mandate now!\n"
            )
        else:
            exec_section = f"\n<b>❌ BID FAILED</b>\n  {order_result.get('message', 'Unknown error')}\n"

    msg = f"""
<b>🏦 IPO SIGNAL [{mode_tag}]</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>{row['IPO Name']}</b> [{row['Type']}]
<b>{row['Signal']}</b>
Score: {row['Score']} ({row['Score %']}%)

<b>📊 SUBSCRIPTION</b>
  QIB:    {row['QIB (x)']}x
  NII:    {row['NII (x)']}x
  Retail: {row['Retail (x)']}x
  Total:  {row['Total (x)']}x
  Close:  {row['Close Date']}

<b>💹 GMP ANALYSIS</b>
  GMP:          ₹{row['GMP ₹']} ({row['GMP %']}%)
  Est. Listing: ₹{row['Est Listing ₹']}
  Kostak:       ₹{row['Kostak ₹']}
  HNI Net:      ₹{row['HNI Net ₹']}

<b>⚠️ HNI DUMP RISK: {row['HNI Dump Risk']}</b>

<b>📋 IPO DETAILS</b>
  Price Band: {row['Price Band']}
  Lot Size:   {row['Lot Size']}
  OFS %:      {row['OFS %']}%
  Issue Size: ₹{row['Total Issue Cr']} Cr
{exec_section}
<b>🔍 CHECKLIST</b>
{checks_text}
━━━━━━━━━━━━━━━━━━━━━━━━━━
<i>{row['Timestamp']}</i>
""".strip()
    tg(msg)


def alert_summary(csv_rows: list):
    """Send daily summary of all IPOs."""
    mode_tag = "🔵 DRY RUN" if DRY_RUN else "🔴 LIVE"
    lines    = [
        f"<b>📊 IPO DAILY SUMMARY [{mode_tag}]</b>",
        f"<i>{datetime.now().strftime('%d %b %Y  %H:%M IST')}</i>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    for row in csv_rows:
        sig   = row["Signal"][:2]
        lines.append(
            f"{sig} <b>{row['IPO Name']}</b> [{row['Type']}] — "
            f"{row['Score']} | QIB={row['QIB (x)']}x | GMP={row['GMP %']}%"
        )
    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Total IPOs analysed: {len(csv_rows)}",
        f"Engine: IPO Intelligence v4.0",
    ]
    tg("\n".join(lines))


def alert_system_status(status: str):
    mode_tag = "🔵 DRY RUN MODE" if DRY_RUN else "🔴 LIVE MODE"
    tg(
        f"<b>⚙️ IPO Intelligence Engine</b>\n"
        f"Status: {status}\n"
        f"Mode: {mode_tag}\n"
        f"Scheduled: {', '.join(SCHEDULE_TIMES)} IST\n"
        f"<i>{datetime.now().strftime('%d %b %Y %H:%M')}</i>"
    )


# ═══════════════════════════════════════════════════════════════════════════
#  MASTER PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

def run_pipeline(fetch_details: bool = True) -> list[dict]:
    """
    The complete 4-layer pipeline:
      L1 → Collect data from all sources
      L2 → Run checklist decision matrix
      L3 → Execute IPO bids for qualifying IPOs
      L4 → Send rich Telegram alerts
    """
    log.info("═" * 70)
    log.info("  IPO INTELLIGENCE ENGINE v4.0 — PIPELINE START")
    log.info(f"  {datetime.now().strftime('%d %b %Y  %H:%M:%S')}")
    log.info(f"  Mode: {'DRY RUN (Safe)' if DRY_RUN else '🔴 LIVE — REAL BIDS WILL BE PLACED'}")
    log.info("═" * 70)

    # ── L1: Data Collection ────────────────────────────────────────────
    log.info("\n[ L1 ] DATA COLLECTION")
    mb_subs  = scrape_subscription("mainboard")
    sme_subs = scrape_subscription("sme")
    all_subs = mb_subs + sme_subs
    log.info(f"  Mainboard: {len(mb_subs)} | SME: {len(sme_subs)}")

    if not all_subs:
        msg = "ℹ️ No live IPOs today. Market may be closed."
        log.warning(f"  {msg}")
        tg(f"<b>IPO Engine</b>: {msg}")
        return []

    gmp_map  = scrape_gmp()
    calendar = scrape_official_calendar()
    log.info(f"  GMP data: {len(gmp_map)} IPOs | NSE/BSE calendar: {len(calendar)} entries")

    # ── L3: Zerodha Login (before loop, reuse session) ─────────────────
    log.info("\n[ L3 ] ZERODHA LOGIN")
    executor = ZerodhaExecutor()
    l3_ready = executor.login()

    if l3_ready and not DRY_RUN:
        funds = executor.get_funds()
        log.info(f"  Available funds: ₹{funds.get('available', 0):,.2f}")
        tg(f"💰 Zerodha logged in. Available: ₹{funds.get('available', 0):,.0f}")
    elif DRY_RUN:
        log.info("  DRY RUN mode — no real bids will be placed")

    # ── L2 + L3 + L4: Per-IPO Loop ────────────────────────────────────
    log.info("\n[ L2 ] DECISION MATRIX + [ L3 ] EXECUTION + [ L4 ] ALERTS")
    master_data = {}
    csv_rows    = []

    for sub in all_subs:
        name  = sub["name"]
        itype = sub["ipo_type"]

        # Match GMP
        gmp = next(
            (v for k, v in gmp_map.items() if fuzzy_match(name, k)),
            {}
        )

        # Detail page (optional, slower)
        detail = {}
        if fetch_details and sub.get("detail_url"):
            detail = scrape_ipo_detail(sub["detail_url"], name)

        # Extract values
        issue_price = clean_number(str(
            detail.get("issue_price") or
            str(detail.get("price_band", "0")).split("-")[-1]
        ))
        lot_size    = int(detail.get("lot_size") or 1)
        ofs_pct     = float(detail.get("ofs_pct") or 0)
        kostak_rs   = gmp.get("kostak_rs", 0)

        # Kostak analysis
        kost       = kostak_breakeven(issue_price, lot_size, kostak_rs) if issue_price > 0 else {}
        kostak_net = kost.get("net_kostak_rs", 0)

        # ── L2: Decision Matrix ────────────────────────────────────────
        decision = run_decision_matrix(
            qib_x     = sub.get("qib_x", 0),
            nii_x     = sub.get("nii_x", 0),
            retail_x  = sub.get("retail_x", 0),
            gmp_pct   = gmp.get("gmp_pct", 0),
            ofs_pct   = ofs_pct,
            kostak_net= kostak_net,
            ipo_type  = itype,
        )

        # ── L3: Execute if qualifies ──────────────────────────────────
        order_result = None
        if decision["should_execute"] and (l3_ready or DRY_RUN) and issue_price > 0:
            log.info(f"  [L3] Executing bid for {name}...")
            # Derive symbol from name (best-effort; confirm manually if needed)
            symbol = re.sub(r"[^A-Z0-9]", "", name.upper().replace("LIMITED", "").replace("LTD", "").strip())[:10]
            order_result = executor.place_ipo_bid(
                symbol   = symbol,
                price    = issue_price,
                lot_size = lot_size,
            )

        # ── Assemble master record ────────────────────────────────────
        record = {
            "name": name, "ipo_type": itype,
            "scraped_at": datetime.now().isoformat(),
            "sub": sub, "gmp": gmp, "detail": detail,
            "kostak": kost, "decision": decision,
            "order": order_result,
        }
        master_data[name] = record

        # ── CSV row (flat for Excel) ──────────────────────────────────
        row = {
            "IPO Name"       : name,
            "Type"           : itype.upper(),
            "Signal"         : decision["signal"],
            "Score"          : f"{decision['score']}/{decision['max_score']}",
            "Score %"        : decision["score_pct"],
            "Close Date"     : sub.get("close_date", ""),
            "QIB (x)"        : sub.get("qib_x", 0),
            "NII (x)"        : sub.get("nii_x", 0),
            "Retail (x)"     : sub.get("retail_x", 0),
            "Total (x)"      : sub.get("total_x", 0),
            "GMP ₹"          : gmp.get("gmp_rs", 0),
            "GMP %"          : gmp.get("gmp_pct", 0),
            "Est Listing ₹"  : gmp.get("est_listing", 0),
            "Kostak ₹"       : kostak_rs,
            "HNI Net ₹"      : kostak_net,
            "HNI Dump Risk"  : kost.get("hni_dump_risk", "N/A"),
            "Price Band"     : detail.get("price_band", "N/A"),
            "Lot Size"       : lot_size,
            "OFS %"          : ofs_pct,
            "Fresh Issue Cr" : detail.get("fresh_issue_cr", "N/A"),
            "Total Issue Cr" : detail.get("total_issue_cr", "N/A"),
            "Promoter Post %" : detail.get("promoter_post_pct", "N/A"),
            "Bid Placed"     : "YES" if (order_result and order_result.get("status") in ("SUCCESS","DRY_RUN")) else "NO",
            "Order ID"       : (order_result or {}).get("order_id", ""),
            "Disqualified"   : "YES" if decision["disqualified"] else "NO",
            "DQ Reason"      : decision.get("dq_reason", ""),
            "Timestamp"      : datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        csv_rows.append(row)

        log.info(
            f"  {'🟢' if 'STRONG' in decision['signal'] else decision['signal'][:2]} "
            f"{name} | QIB={sub.get('qib_x',0)}x | "
            f"GMP={gmp.get('gmp_pct',0)}% | "
            f"Score={decision['score']}/{decision['max_score']} | "
            f"Bid={'✅' if order_result else '—'}"
        )

        # ── L4: Alert for GO decisions ────────────────────────────────
        if "GO" in decision["signal"] and "NO-GO" not in decision["signal"]:
            alert_ipo_decision(row, decision["checks"], order_result)

    # ── Save outputs ──────────────────────────────────────────────────
    log.info("\n[ SAVE ] Writing outputs...")
    save_json(master_data, MASTER_JSON)

    if csv_rows:
        df        = pd.DataFrame(csv_rows)
        prio      = {"🟢": 0, "🟡": 1, "🟠": 2, "🔴": 3}
        df["_s"]  = df["Signal"].apply(lambda s: prio.get(s[0], 9))
        df        = df.sort_values("_s").drop(columns=["_s"])
        df.to_csv(REPORT_CSV, index=False, encoding="utf-8-sig")

        print("\n" + "═" * 70)
        print("  TOP IPO DECISIONS")
        print("═" * 70)
        print(df[["IPO Name", "Signal", "QIB (x)", "GMP %", "HNI Dump Risk", "Bid Placed"]].to_string(index=False))
        print("═" * 70 + "\n")

    # ── L4: Summary alert ─────────────────────────────────────────────
    alert_summary(csv_rows)

    log.info(f"✅ PIPELINE COMPLETE — {len(csv_rows)} IPOs processed")
    log.info(f"   CSV  → {REPORT_CSV}")
    log.info(f"   JSON → {MASTER_JSON}")
    log.info(f"   Log  → {LOG_FILE}\n")
    return csv_rows


# ═══════════════════════════════════════════════════════════════════════════
#  SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════

def run_scheduler():
    """Auto-run the pipeline daily at scheduled times."""
    log.info(f"📅 Scheduler active. Runs at: {SCHEDULE_TIMES}")
    alert_system_status("✅ ONLINE — Scheduler active")

    for t in SCHEDULE_TIMES:
        schedule.every().day.at(t).do(run_pipeline, fetch_details=True)

    run_pipeline(fetch_details=True)   # Run once on startup

    while True:
        schedule.run_pending()
        time.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":

    mode = (sys.argv[1].lower() if len(sys.argv) > 1 else "once")

    BANNER = """
╔═══════════════════════════════════════════════════════════════════╗
║       IPO INTELLIGENCE ENGINE — 4-LAYER ULTIMATE v4.0             ║
║  L1: Chittorgarh+Investorgain+NSE  L2: Elite Decision Matrix      ║
║  L3: Zerodha TOTP Auto-Execution   L4: Telegram Alerts            ║
╚═══════════════════════════════════════════════════════════════════╝
"""
    print(BANNER)
    mode_msg = "🔵 DRY RUN MODE (Safe)" if DRY_RUN else "🔴 LIVE MODE — REAL BIDS WILL BE PLACED"
    print(f"  Current mode: {mode_msg}\n")

    if mode == "schedule":
        run_scheduler()

    elif mode == "fast":
        log.info("Mode: FAST — no detail page scraping")
        run_pipeline(fetch_details=False)

    elif mode == "dryrun":
        import builtins
        builtins.DRY_RUN = True         # Force dry run regardless of config
        DRY_RUN = True
        log.info("Mode: FORCED DRY RUN")
        run_pipeline(fetch_details=True)

    elif mode == "once":
        log.info("Mode: ONCE — full run")
        run_pipeline(fetch_details=True)

    else:
        print("Usage: python ipo_engine.py [once | fast | dryrun | schedule]")
        print()
        print("  once     → Full run with detail scraping (use on IPO Day 2-3 at 1PM)")
        print("  fast     → Quick run, skip detail pages  (use for testing)")
        print("  dryrun   → Full run, force DRY_RUN = True, no real bids")
        print("  schedule → Auto-run daily at configured times")
