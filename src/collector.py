"""
collector.py
------------
Global Daily News — 39개 진출국 데이터 수집기 (공식 출처 연동 버전).

이 버전에서 추가/변경된 핵심 사항
  1) 한국타이어 공식 거점 명칭 (1회성 크롤링)
       - https://www.hankooktire.com/global/ko/company/global-network.html 을 BeautifulSoup 으로
         파싱해 국가별 공식 법인/공장명과 도시를 가져온다.
       - data/hankook_offices_cache.json 이 있으면 그 파일만 읽고 절대 다시 크롤링하지 않는다.
         (크롤링이 실패해 'fallback' 으로 저장된 경우에 한해 30일 뒤 1회 재시도)
       - 페이지가 JS 렌더링이라 파싱 결과가 부족하면 data/countries_metadata.json 의 offices 로 대체.
  2) 거시경제 지표 (주 1회 갱신)
       - 우선순위 1: Trading Economics API (환경변수 TE_API_KEY, 키 없음/403/한도 초과 시 즉시 다음 단계)
       - 우선순위 2: IMF WEO (NGDPD, PCPIPCH)
       - 우선순위 3: World Bank API (SP.POP.TOTL / NY.GDP.MKTP.CD / FP.CPI.TOTL.ZG / SL.UEM.TOTL.ZS)
       - 수도/인구 보조: REST Countries
       - 모든 지표에 source 와 기준연도(year) 를 함께 저장하고, data/economic_cache.json 에 7일간 캐시.
       - 우선순위 1~3 이 모두 실패한 지표만 countries_metadata.json 의 정적 값을 사용.
  3) 최저임금: {display, note, source, year}. 법정 최저임금이 없는 국가는 display = "-".
  4) 안전성: 모든 외부 호출은 7초 타임아웃 + try/except, 국가 순회 시 0.2~0.3초 지연,
     연속 실패하는 출처는 해당 실행 동안 자동으로 건너뛴다. 화면에 'Error' 문자열은 생성하지 않는다.

뉴스 수집 / 환율 수집 / 국가 정렬 / 아카이브 관리 로직은 기존과 동일하다.
"""

import os
import re
import copy
import json
import time
import random
import logging
from datetime import datetime, timedelta
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import feedparser
import pandas as pd
import pytz

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("collector")

KST = pytz.timezone("Asia/Seoul")
BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data")
METADATA_PATH = os.path.join(DATA_DIR, "countries_metadata.json")
CACHE_PATH = os.path.join(DATA_DIR, "countries_data.json")
HANKOOK_CACHE_PATH = os.path.join(DATA_DIR, "hankook_offices_cache.json")
ECONOMIC_CACHE_PATH = os.path.join(DATA_DIR, "economic_cache.json")
ARCHIVE_DIR = os.path.join(BASE_DIR, "docs", "archive")
ARCHIVE_INDEX_PATH = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_RETENTION_DAYS = 180

HANKOOK_NETWORK_URL = "https://www.hankooktire.com/global/ko/company/global-network.html"
HANKOOK_RETRY_AFTER_DAYS = 30
HANKOOK_MIN_COUNTRIES = 5

ECONOMIC_CACHE_TTL_DAYS = 7
API_TIMEOUT = 7
API_DELAY_MIN = 0.2
API_DELAY_MAX = 0.3
SOURCE_MAX_CONSECUTIVE_FAILS = 5
TE_API_KEY = os.environ.get("TE_API_KEY", "").strip()

MONTH_KR = ["1월", "2월", "3월", "4월", "5월", "6월",
            "7월", "8월", "9월", "10월", "11월", "12월"]

MAX_WORKERS = 8  # 국가 단위 병렬 수집 스레드 수

SMALL_UNIT_CURRENCIES = {"VND": 100, "JPY": 100, "IDR": 100}
WON_DECIMALS = 1
FALLBACK_TEXT = "현지 산업 및 정책 모니터링 중"
STATIC_SOURCE = "내부 메타데이터(정적 값)"

HTTP_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 GlobalDailyNews/1.0"),
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

REGIONS = [
    {"key": "ALL", "label": "전체"},
    {"key": "KR", "label": "한국"},
    {"key": "CN", "label": "중국"},
    {"key": "AMER", "label": "미주"},
    {"key": "EU", "label": "유럽"},
    {"key": "LATAM", "label": "중남미"},
    {"key": "APAC", "label": "아태"},
    {"key": "MEA", "label": "중아"},
]

REFERENCE_RATE_PER_UNIT = {
    "USD": 1380.0, "EUR": 1500.0, "GBP": 1750.0, "JPY": 9.2, "CNY": 190.0,
    "CAD": 1010.0, "HUF": 3.8, "PLN": 345.0, "CZK": 61.0, "RON": 300.0,
    "RUB": 14.0, "UAH": 33.0, "TRY": 40.0, "RSD": 12.8, "MAD": 138.0,
    "MXN": 68.0, "BRL": 250.0, "CLP": 1.4, "COP": 0.32, "PAB": 1380.0,
    "IDR": 0.085, "AUD": 900.0, "SGD": 1020.0, "MYR": 290.0, "THB": 38.0,
    "VND": 0.054, "TWD": 43.0, "AED": 375.0, "SAR": 368.0, "EGP": 28.0,
    "KZT": 2.6, "SEK": 130.0,
}

# 메타데이터 code(내부 코드) → 외부 API 표준 ISO 코드
ISO2_OVERRIDES = {"UK": "GB"}
ISO3_MAP = {
    "KR": "KOR", "CN": "CHN", "US": "USA", "CA": "CAN", "DE": "DEU", "HU": "HUN",
    "UK": "GBR", "GB": "GBR", "FR": "FRA", "IT": "ITA", "ES": "ESP", "PL": "POL",
    "CZ": "CZE", "NL": "NLD", "SE": "SWE", "AT": "AUT", "RO": "ROU", "RU": "RUS",
    "UA": "UKR", "TR": "TUR", "RS": "SRB", "HR": "HRV", "MA": "MAR", "MX": "MEX",
    "BR": "BRA", "CL": "CHL", "CO": "COL", "PA": "PAN", "ID": "IDN", "AU": "AUS",
    "JP": "JPN", "SG": "SGP", "MY": "MYS", "TH": "THA", "VN": "VNM", "TW": "TWN",
    "AE": "ARE", "SA": "SAU", "EG": "EGY", "KZ": "KAZ",
}
TE_COUNTRY_NAMES = {
    "KR": "south korea", "US": "united states", "UK": "united kingdom", "GB": "united kingdom",
    "CZ": "czech republic", "TR": "turkey", "RU": "russia", "AE": "united arab emirates",
    "VN": "vietnam", "TW": "taiwan", "SA": "saudi arabia", "NL": "netherlands",
    "KZ": "kazakhstan", "HR": "croatia", "RS": "serbia",
}

# 법정 최저임금 제도가 없는 국가 (display = "-")
NO_STATUTORY_MIN_WAGE = {
    "SE": "법정 최저임금 없음 (산별 단체협약으로 결정)",
    "AT": "법정 최저임금 없음 (산별 단체협약으로 결정)",
    "IT": "법정 최저임금 없음 (단체협약 중심)",
    "SG": "법정 최저임금 없음 (점진적 임금모델 PWM 적용)",
    "AE": "법정 일반 최저임금 없음",
}
EUROSTAT_MIN_WAGE_CODES = {"DE", "FR", "ES", "PL", "CZ", "NL", "RO", "HR", "HU", "TR", "RS"}
MIN_WAGE_SOURCE_BY_CODE = {
    "KR": "고용노동부 고시",
    "US": "U.S. Department of Labor",
    "UK": "GOV.UK (Low Pay Commission)",
    "JP": "일본 후생노동성",
}


def _is_bad_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"error", "err", "undefined", "null", "none", "nan", "n/a", ""}:
        return True
    return False


def load_countries_metadata() -> list:
    if not os.path.exists(METADATA_PATH):
        raise FileNotFoundError(f"{METADATA_PATH} 파일이 없습니다. 39개국 메타데이터 파일을 먼저 준비하세요.")

    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        raw = f.read()
    if not raw.strip():
        raise ValueError(f"{METADATA_PATH} 파일이 비어 있습니다.")

    payload = json.loads(raw)
    rows = payload.get("countries") if isinstance(payload, dict) else payload
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{METADATA_PATH} 안에 유효한 countries 리스트가 없습니다.")

    countries = []
    for row in rows:
        if not isinstance(row, dict) or _is_bad_value(row.get("code")):
            log.warning(f"[metadata] 잘못된 국가 항목을 건너뜁니다: {row}")
            continue
        code = str(row["code"]).strip().upper()
        offices = row.get("offices") if isinstance(row.get("offices"), list) else []
        countries.append({
            "code": code,
            "region": row.get("region", "ALL"),
            "name_kr": row.get("name_ko", code),
            "name_en": row.get("name_en", code),
            "currency": row.get("currency", "USD"),
            "flag": code.lower(),
            "offices": offices,
            "hubs": offices,
            "meta_profile": {
                "capital": row.get("capital", "-"),
                "population": row.get("population", "-"),
                "gdp": row.get("gdp", "-"),
                "inflation": row.get("inflation", "-"),
                "unemployment": row.get("unemployment", "-"),
                "min_wage": row.get("min_wage", "-"),
                "curr_unit": row.get("curr_unit", ""),
            },
        })

    if not countries:
        raise ValueError(f"{METADATA_PATH} 에서 유효한 국가를 하나도 읽지 못했습니다.")

    return countries


COUNTRIES = load_countries_metadata()
METADATA_ASOF = None
try:
    with open(METADATA_PATH, "r", encoding="utf-8") as _f:
        METADATA_ASOF = json.load(_f).get("generated_at", "-")
except Exception:
    METADATA_ASOF = "-"


def _strip_html(text: str) -> str:
    try:
        return re.sub(r"<[^>]+>", "", text or "").strip()
    except Exception:
        return ""


def _shorten(text: str, max_len: int) -> str:
    try:
        text = " ".join((text or "").split())
        return text if len(text) <= max_len else text[:max_len].rstrip() + "…"
    except Exception:
        return text or ""


def _round_won(value: float) -> float:
    try:
        return round(float(value), WON_DECIMALS)
    except Exception:
        return 0.0


def _yahoo_finance_url(currency: str) -> str:
    if not currency or currency == "KRW":
        return ""
    return f"https://finance.yahoo.com/quote/{currency}KRW=X"


def _read_json_file(path: str):
    try:
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        if not raw.strip():
            return None
        return json.loads(raw)
    except Exception as e:
        log.warning(f"[json] {os.path.basename(path)} 읽기 실패: {e}")
        return None


def _write_json_file(path: str, payload) -> bool:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        return True
    except Exception as e:
        log.warning(f"[json] {os.path.basename(path)} 저장 실패: {e}")
        return False


def _parse_iso_datetime(value):
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            dt = KST.localize(dt)
        return dt
    except Exception:
        return None


def _polite_sleep() -> None:
    try:
        time.sleep(random.uniform(API_DELAY_MIN, API_DELAY_MAX))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 외부 API 호출 공통 래퍼 (7초 타임아웃 / 예외 흡수 / 연속 실패 차단)
# ---------------------------------------------------------------------------
_SOURCE_STATE = {}


def _source_disabled(name: str) -> bool:
    return bool(_SOURCE_STATE.get(name, {}).get("disabled"))


def _disable_source(name: str, reason: str) -> None:
    state = _SOURCE_STATE.setdefault(name, {"fails": 0, "disabled": False})
    if not state["disabled"]:
        log.warning(f"[{name}] 이번 실행에서 사용을 중단하고 다음 출처로 전환합니다: {reason}")
    state["disabled"] = True


def _register_failure(name: str, reason: str) -> None:
    state = _SOURCE_STATE.setdefault(name, {"fails": 0, "disabled": False})
    state["fails"] += 1
    if state["fails"] >= SOURCE_MAX_CONSECUTIVE_FAILS:
        _disable_source(name, f"연속 {state['fails']}회 실패 ({reason})")


def _register_success(name: str) -> None:
    state = _SOURCE_STATE.setdefault(name, {"fails": 0, "disabled": False})
    state["fails"] = 0


def _api_get_json(source: str, url: str, params: dict = None):
    """
    반환: (status_code, parsed_json_or_None, raw_text).
    네트워크/파싱 예외는 모두 흡수하여 (None, None, "") 로 돌려준다.
    호출 뒤에는 항상 0.2~0.3초 대기한다.
    """
    if _source_disabled(source):
        return None, None, ""

    status, payload, text = None, None, ""
    try:
        resp = requests.get(url, params=params, headers=HTTP_HEADERS, timeout=API_TIMEOUT)
        status = resp.status_code
        text = resp.text or ""
        try:
            payload = resp.json()
        except Exception:
            payload = None
        _register_success(source)
    except Exception as e:
        _register_failure(source, str(e)[:80])
        log.warning(f"[{source}] 요청 실패: {str(e)[:120]}")
    finally:
        _polite_sleep()
    return status, payload, text


def _year_from_value(value):
    try:
        m = re.search(r"(19|20)\d{2}", str(value))
        return int(m.group(0)) if m else None
    except Exception:
        return None


def _to_float(value):
    try:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return None
        f = float(value)
        if f != f:
            return None
        return f
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 표시 문자열 포맷터
# ---------------------------------------------------------------------------
def format_population(people: float) -> str:
    try:
        man = int(round(float(people) / 10000))
        if man <= 0:
            return f"약 {int(people):,}명"
        eok, rem = divmod(man, 10000)
        if eok:
            return f"약 {eok}억 {rem:,}만 명" if rem else f"약 {eok}억 명"
        return f"약 {man:,}만 명"
    except Exception:
        return "-"


def format_gdp(billions_usd: float) -> str:
    try:
        b = float(billions_usd)
        if b >= 1000:
            return f"약 {b / 1000:.2f}조 달러"
        return f"약 {b * 10:,.0f}억 달러"
    except Exception:
        return "-"


def format_percent(value: float) -> str:
    try:
        return f"{float(value):.1f}%"
    except Exception:
        return "-"


def _make_entry(key: str, value, source: str, year):
    value = _to_float(value) if key != "capital" else value
    if value is None or (key == "capital" and _is_bad_value(value)):
        return None
    if key == "population":
        display = format_population(value)
    elif key == "gdp":
        display = format_gdp(value)
    elif key in ("inflation", "unemployment"):
        display = format_percent(value)
    else:
        display = str(value)
    return {"value": value, "display": display, "source": source, "year": year}


def _iso2_for(code: str) -> str:
    return ISO2_OVERRIDES.get(code, code)


# ---------------------------------------------------------------------------
# 1) 한국타이어 공식 거점 명칭 (1회성 크롤링)
# ---------------------------------------------------------------------------
COUNTRY_ALIASES = {
    "KR": ["korea", "south korea", "republic of korea", "한국", "대한민국"],
    "US": ["united states", "usa", "u.s.a", "u.s.", "america", "미국"],
    "UK": ["united kingdom", "uk", "great britain", "england", "영국"],
    "CZ": ["czech republic", "czechia", "체코"],
    "TR": ["turkey", "türkiye", "turkiye", "튀르키예", "터키"],
    "AE": ["united arab emirates", "uae", "아랍에미리트", "두바이"],
    "VN": ["vietnam", "viet nam", "베트남"],
    "RU": ["russia", "russian federation", "러시아"],
    "SA": ["saudi arabia", "saudi", "사우디아라비아", "사우디"],
    "TW": ["taiwan", "대만"],
    "CN": ["china", "중국"],
    "DE": ["germany", "독일"],
    "NL": ["netherlands", "the netherlands", "네덜란드"],
}
REGION_HEADINGS = {
    "asia", "asia pacific", "europe", "americas", "north america", "south america",
    "middle east", "africa", "middle east & africa", "cis", "global",
    "아시아", "아시아태평양", "유럽", "미주", "북미", "중남미", "중동", "아프리카", "중아", "글로벌",
}
OFFICE_KEYWORD_RE = re.compile(
    r"(plant|factory|corp|co\.|ltd|gmbh|inc\b|llc|s\.a\.|s\.r\.l|kft|sp\. ?z|a\.s\.|b\.v\.|\bab\b|pt\.|sdn|bhd|pty|"
    r"limited|headquarter|\bhq\b|technology center|tech center|technical center|research|r&d|proving ground|"
    r"technoring|technodome|sales|office|branch|center|centre|법인|공장|본부|연구소|센터|지사|주행시험장|사무소|판매|영업|테크노)",
    re.IGNORECASE,
)
NOISE_LINE_RE = re.compile(
    r"(cookie|privacy|copyright|©|all rights reserved|로그인|검색|개인정보|이용약관|사이트맵|sitemap|login|search|"
    r"javascript|skip to|menu|뉴스룸|채용|고객센터)",
    re.IGNORECASE,
)


def _normalize_name(text: str) -> str:
    try:
        text = re.sub(r"[\(\[].*?[\)\]]", "", text or "")
        text = re.sub(r"[^\w\s\.&\-가-힣]", " ", text)
        return " ".join(text.lower().split())
    except Exception:
        return ""


def _build_country_name_index(countries: list) -> dict:
    index = {}
    for c in countries:
        code = c["code"]
        names = [c.get("name_en", ""), c.get("name_kr", "")] + COUNTRY_ALIASES.get(code, [])
        for n in names:
            key = _normalize_name(n)
            if key and key not in index:
                index[key] = code
    return index


def _detect_country_in_line(line: str, name_index: dict):
    low = _normalize_name(line)
    best_code, best_len = None, 0
    for name, code in name_index.items():
        if len(name) < 4 and not re.search(r"[가-힣]", name):
            continue
        try:
            if re.search(r"(?<![a-z0-9])" + re.escape(name) + r"(?![a-z0-9])", low) and len(name) > best_len:
                best_code, best_len = code, len(name)
        except Exception:
            continue
    return best_code


def parse_hankook_global_network(html: str, countries: list) -> dict:
    """공식 글로벌 네트워크 페이지 HTML → {country_code: [공식 거점명, ...]}"""
    result = {}
    try:
        from bs4 import BeautifulSoup
    except Exception as e:
        log.warning(f"[hankook] BeautifulSoup 불러오기 실패: {e}")
        return result

    try:
        soup = BeautifulSoup(html or "", "html.parser")
        for tag in soup(["script", "style", "noscript", "nav", "header", "footer"]):
            tag.decompose()
        lines = [" ".join(l.split()) for l in soup.get_text("\n").split("\n")]
        lines = [l for l in lines if l]
    except Exception as e:
        log.warning(f"[hankook] HTML 파싱 실패: {e}")
        return result

    name_index = _build_country_name_index(countries)
    current = None

    for line in lines:
        try:
            if len(line) > 110 or NOISE_LINE_RE.search(line):
                continue

            norm = _normalize_name(line)
            if norm in REGION_HEADINGS:
                current = None
                continue
            if norm in name_index:
                current = name_index[norm]
                continue

            if not OFFICE_KEYWORD_RE.search(line) or len(line) < 4:
                continue

            explicit = _detect_country_in_line(line, name_index)
            target = explicit or current
            if not target:
                continue

            bucket = result.setdefault(target, [])
            if line not in bucket and len(bucket) < 12:
                bucket.append(line)
        except Exception:
            continue

    return {code: names for code, names in result.items() if names}


def _hankook_retry_due(cached: dict) -> bool:
    fetched = _parse_iso_datetime(cached.get("fetched_at"))
    if fetched is None:
        return True
    return datetime.now(KST) - fetched > timedelta(days=HANKOOK_RETRY_AFTER_DAYS)


def fetch_hankook_offices(countries: list) -> dict:
    """
    반환: {country_code: [공식 거점명, ...]} (확보한 국가만 포함, 없으면 빈 dict).
    캐시 파일이 있으면 읽기만 하고 크롤링하지 않는다.
    """
    cached = _read_json_file(HANKOOK_CACHE_PATH)
    if isinstance(cached, dict) and isinstance(cached.get("offices"), dict):
        if cached.get("status") == "ok" or not _hankook_retry_due(cached):
            log.info("[hankook] data/hankook_offices_cache.json 사용 (재크롤링 없음)")
            return {k: v for k, v in cached["offices"].items() if isinstance(v, list) and v}

    log.info("[hankook] 공식 글로벌 네트워크 페이지 1회 크롤링 시작")
    offices, status, reason = {}, "fallback", ""
    try:
        resp = requests.get(HANKOOK_NETWORK_URL, headers=HTTP_HEADERS, timeout=API_TIMEOUT)
        if resp.status_code != 200:
            reason = f"HTTP {resp.status_code}"
        else:
            resp.encoding = resp.apparent_encoding or "utf-8"
            offices = parse_hankook_global_network(resp.text, countries)
            if len(offices) >= HANKOOK_MIN_COUNTRIES:
                status = "ok"
            else:
                reason = f"파싱 결과 부족({len(offices)}개국, JS 렌더링 페이지로 추정)"
    except Exception as e:
        reason = str(e)[:120]

    if status != "ok":
        log.warning(f"[hankook] 공식 페이지 사용 불가 → countries_metadata.json 의 거점 정보로 대체 ({reason})")
        offices = {}

    _write_json_file(HANKOOK_CACHE_PATH, {
        "source_url": HANKOOK_NETWORK_URL,
        "fetched_at": datetime.now(KST).isoformat(),
        "status": status,
        "reason": reason,
        "offices": offices,
    })
    return offices


# ---------------------------------------------------------------------------
# 2) 거시경제 지표: Trading Economics → IMF WEO → World Bank → 메타데이터
# ---------------------------------------------------------------------------
def _fetch_trading_economics(code: str, name_en: str) -> dict:
    out = {}
    source = "Trading Economics"
    if not TE_API_KEY or _source_disabled(source):
        return out

    try:
        te_name = TE_COUNTRY_NAMES.get(code) or str(name_en or code).lower()
        url = f"https://api.tradingeconomics.com/country/{requests.utils.quote(te_name)}"
        status, payload, text = _api_get_json(source, url, params={"c": TE_API_KEY, "f": "json"})

        if status in (401, 402, 403, 429):
            _disable_source(source, f"HTTP {status} (키 오류/권한/한도 초과)")
            return out
        if status is not None and status != 200:
            lowered = (text or "").lower()
            if "limit" in lowered or "quota" in lowered or "exceed" in lowered:
                _disable_source(source, "호출 한도 초과")
            return out
        if status == 200 and not isinstance(payload, list):
            lowered = (text or "").lower()
            if "limit" in lowered or "quota" in lowered or "exceed" in lowered:
                _disable_source(source, "호출 한도 초과")
            return out
        if not isinstance(payload, list):
            return out

        wanted = {"gdp": "gdp", "inflation rate": "inflation", "unemployment rate": "unemployment"}
        for row in payload:
            if not isinstance(row, dict):
                continue
            category = str(row.get("Category", "")).strip().lower()
            key = wanted.get(category)
            if not key or key in out:
                continue
            value = _to_float(row.get("LatestValue"))
            if value is None:
                continue
            year = _year_from_value(row.get("LatestValueDate"))

            if key == "gdp":
                unit = str(row.get("Unit", "")).lower()
                if "usd" not in unit:
                    continue
                if "trillion" in unit:
                    value = value * 1000.0
                elif "million" in unit:
                    value = value / 1000.0
                elif "thousand" in unit:
                    value = value / 1_000_000.0
            entry = _make_entry(key, value, source, year)
            if entry:
                out[key] = entry
    except Exception as e:
        log.warning(f"[{code}] Trading Economics 처리 실패: {e}")
    return out


def _fetch_imf_weo(code: str, need: list) -> dict:
    out = {}
    source = "IMF WEO"
    if _source_disabled(source):
        return out

    iso3 = ISO3_MAP.get(code)
    if not iso3:
        return out

    indicators = {"gdp": "NGDPD", "inflation": "PCPIPCH"}
    this_year = datetime.now(KST).year
    for key in need:
        ind = indicators.get(key)
        if not ind:
            continue
        try:
            url = f"https://www.imf.org/external/datamapper/api/v1/{ind}/{iso3}"
            status, payload, _ = _api_get_json(source, url)
            if status != 200 or not isinstance(payload, dict):
                continue
            series = (((payload.get("values") or {}).get(ind) or {}).get(iso3)) or {}
            best_year, best_value = None, None
            for y, v in series.items():
                yi = _year_from_value(y)
                fv = _to_float(v)
                if yi is None or fv is None or yi > this_year:
                    continue
                if best_year is None or yi > best_year:
                    best_year, best_value = yi, fv
            if best_year is not None:
                entry = _make_entry(key, best_value, source, best_year)
                if entry:
                    out[key] = entry
        except Exception as e:
            log.warning(f"[{code}] IMF WEO {ind} 처리 실패: {e}")
    return out


def _fetch_world_bank(code: str, need: list) -> dict:
    out = {}
    source = "World Bank"
    if _source_disabled(source):
        return out

    indicators = {
        "population": "SP.POP.TOTL",
        "gdp": "NY.GDP.MKTP.CD",
        "inflation": "FP.CPI.TOTL.ZG",
        "unemployment": "SL.UEM.TOTL.ZS",
    }
    iso2 = _iso2_for(code)
    for key in need:
        ind = indicators.get(key)
        if not ind:
            continue
        try:
            url = f"https://api.worldbank.org/v2/country/{iso2}/indicator/{ind}?format=json&per_page=5&mrnev=1"
            status, payload, _ = _api_get_json(source, url)
            if status != 200 or not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
                continue
            for row in payload[1]:
                if not isinstance(row, dict):
                    continue
                value = _to_float(row.get("value"))
                if value is None:
                    continue
                if key == "gdp":
                    value = value / 1e9
                entry = _make_entry(key, value, source, _year_from_value(row.get("date")))
                if entry:
                    out[key] = entry
                    break
        except Exception as e:
            log.warning(f"[{code}] World Bank {ind} 처리 실패: {e}")
    return out


def _fetch_rest_countries(code: str) -> dict:
    out = {}
    source = "REST Countries"
    if _source_disabled(source):
        return out
    try:
        iso2 = _iso2_for(code)
        url = f"https://restcountries.com/v3.1/alpha/{iso2}?fields=capital,population"
        status, payload, _ = _api_get_json(source, url)
        if status != 200 or payload is None:
            return out
        row = payload[0] if isinstance(payload, list) and payload else payload
        if not isinstance(row, dict):
            return out
        year = datetime.now(KST).year

        capitals = row.get("capital")
        if isinstance(capitals, list) and capitals and not _is_bad_value(capitals[0]):
            out["capital"] = {"value": str(capitals[0]).strip(), "display": str(capitals[0]).strip(),
                              "source": source, "year": year}
        pop = _to_float(row.get("population"))
        if pop is not None and pop > 0:
            entry = _make_entry("population", pop, source, year)
            if entry:
                out["population"] = entry
    except Exception as e:
        log.warning(f"[{code}] REST Countries 처리 실패: {e}")
    return out


def collect_macro_for_country(country: dict) -> dict:
    """한 국가의 거시지표를 우선순위대로 수집. 확보하지 못한 지표는 결과에 포함하지 않는다."""
    code = country["code"]
    found = {}
    try:
        found.update(_fetch_trading_economics(code, country.get("name_en", "")))
    except Exception as e:
        log.warning(f"[{code}] 우선순위1 처리 예외, 다음 출처로 전환: {e}")

    try:
        need = [k for k in ("gdp", "inflation") if k not in found]
        if need:
            for k, v in _fetch_imf_weo(code, need).items():
                found.setdefault(k, v)
    except Exception as e:
        log.warning(f"[{code}] 우선순위2 처리 예외, 다음 출처로 전환: {e}")

    try:
        need = [k for k in ("population", "gdp", "inflation", "unemployment") if k not in found]
        if need:
            for k, v in _fetch_world_bank(code, need).items():
                found.setdefault(k, v)
    except Exception as e:
        log.warning(f"[{code}] 우선순위3 처리 예외, 다음 출처로 전환: {e}")

    try:
        rest = _fetch_rest_countries(code)
        if "capital" in rest:
            found["capital"] = rest["capital"]
        if "population" not in found and "population" in rest:
            found["population"] = rest["population"]
    except Exception as e:
        log.warning(f"[{code}] REST Countries 처리 예외: {e}")

    return found


def _economic_cache_is_fresh(cache) -> bool:
    try:
        if not isinstance(cache, dict) or not cache.get("complete"):
            return False
        fetched = _parse_iso_datetime(cache.get("fetched_at"))
        if fetched is None:
            return False
        if datetime.now(KST) - fetched >= timedelta(days=ECONOMIC_CACHE_TTL_DAYS):
            return False
        stored = cache.get("countries")
        if not isinstance(stored, dict):
            return False
        return all(c["code"] in stored for c in COUNTRIES)
    except Exception:
        return False


def load_economic_data(countries: list) -> tuple:
    """
    반환: ({code: {indicator: entry}}, 기준일 문자열)
    캐시가 7일 이내(그리고 완전)이면 API 호출 없이 재사용한다.
    """
    cache = _read_json_file(ECONOMIC_CACHE_PATH)
    if _economic_cache_is_fresh(cache):
        log.info("[economic] data/economic_cache.json 이 7일 이내 → API 호출 없이 재사용")
        return cache["countries"], str(cache.get("fetched_at", ""))[:10]

    log.info("[economic] 거시지표 갱신 시작 (TE → IMF WEO → World Bank → REST Countries)")
    if not TE_API_KEY:
        log.info("[economic] TE_API_KEY 없음 → Trading Economics 건너뛰고 IMF WEO 로 시작")

    collected = {}
    api_count = 0
    for country in countries:
        try:
            data = collect_macro_for_country(country)
        except Exception as e:
            log.warning(f"[{country.get('code')}] 거시지표 수집 예외, 메타데이터로 대체: {e}")
            data = {}
        collected[country["code"]] = data
        api_count += sum(1 for k in ("population", "gdp", "inflation", "unemployment") if k in data)

    expected = max(1, len(countries) * 4)
    complete = api_count >= expected * 0.5
    now_kst = datetime.now(KST)

    # 직전 캐시(오래된 것)가 있으면 이번에 못 받은 지표를 보충한다.
    if isinstance(cache, dict) and isinstance(cache.get("countries"), dict):
        for code, old in cache["countries"].items():
            if code in collected and isinstance(old, dict):
                for k, v in old.items():
                    collected[code].setdefault(k, v)

    _write_json_file(ECONOMIC_CACHE_PATH, {
        "fetched_at": now_kst.isoformat(),
        "complete": complete,
        "api_indicator_count": api_count,
        "countries": collected,
    })
    log.info(f"[economic] 갱신 완료: API 지표 {api_count}/{expected}건 (complete={complete})")
    return collected, now_kst.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# 3) 최저임금 {display, note, source, year}
# ---------------------------------------------------------------------------
def _metadata_year() -> int:
    y = _year_from_value(METADATA_ASOF)
    return y if y else datetime.now(KST).year


def build_min_wage(code: str, raw) -> dict:
    source = MIN_WAGE_SOURCE_BY_CODE.get(code) or ("Eurostat" if code in EUROSTAT_MIN_WAGE_CODES else "ILO")
    year = _metadata_year()
    display, note = "-", ""

    try:
        if isinstance(raw, dict):
            display = str(raw.get("display", "-")).strip() or "-"
            note = str(raw.get("note", "") or "").strip()
            if not _is_bad_value(raw.get("source")):
                source = str(raw.get("source")).strip()
            parsed_year = _year_from_value(raw.get("year"))
            if parsed_year:
                year = parsed_year
            else:
                parsed_year = _year_from_value(note) or _year_from_value(display)
                if parsed_year:
                    year = parsed_year
        elif isinstance(raw, str) and not _is_bad_value(raw):
            text = raw.strip()
            m = re.match(r"^(.*?)\s*\((.*)\)\s*$", text)
            if m:
                display, note = m.group(1).strip(), m.group(2).strip()
            else:
                display = text
            parsed_year = _year_from_value(note) or _year_from_value(text)
            if parsed_year:
                year = parsed_year
    except Exception as e:
        log.warning(f"[{code}] 최저임금 파싱 실패, '-' 처리: {e}")
        display, note = "-", ""

    if _is_bad_value(display) or display == "-" or re.search(r"(없음|해당 없음|n/a)", display, re.IGNORECASE):
        display = "-"

    if code in NO_STATUTORY_MIN_WAGE:
        display = "-"
        note = NO_STATUTORY_MIN_WAGE[code]
    elif display == "-" and not note:
        note = "법정 최저임금 정보 없음"

    return {"display": display, "note": note, "source": source, "year": year}


# ---------------------------------------------------------------------------
# 프로필 구성
# ---------------------------------------------------------------------------
def _static_entry(key: str, value, year):
    if _is_bad_value(value) or str(value).strip() == "-":
        return None
    return {"value": value, "display": str(value), "source": STATIC_SOURCE, "year": year}


def build_profile(country: dict, cached: dict, shared: dict = None) -> dict:
    shared = shared or {}
    code = country.get("code", "")
    meta = country.get("meta_profile", {}) or {}
    cached_profile = (cached or {}).get("profile", {}) or {}
    cached_indicators = cached_profile.get("indicators", {}) if isinstance(cached_profile.get("indicators"), dict) else {}
    econ = (shared.get("economic") or {}).get(code, {}) or {}
    meta_year = _metadata_year()

    indicators = {}
    for key in ("capital", "population", "gdp", "inflation", "unemployment"):
        entry = None
        try:
            api_entry = econ.get(key)
            if isinstance(api_entry, dict) and not _is_bad_value(api_entry.get("display")):
                entry = dict(api_entry)
                if key == "capital":
                    meta_capital = meta.get("capital")
                    if (not _is_bad_value(meta_capital) and re.search(r"[가-힣]", str(meta_capital))):
                        entry["display"] = str(meta_capital)
            if entry is None:
                entry = _static_entry(key, meta.get(key), meta_year)
            if entry is None and isinstance(cached_indicators.get(key), dict):
                entry = dict(cached_indicators[key])
            if entry is None and not _is_bad_value(cached_profile.get(key)) and cached_profile.get(key) != "-":
                entry = {"value": cached_profile.get(key), "display": str(cached_profile.get(key)),
                         "source": cached_profile.get("stats_source", STATIC_SOURCE), "year": meta_year}
        except Exception as e:
            log.warning(f"[{code}] '{key}' 지표 구성 실패: {e}")
            entry = None
        if entry is None:
            entry = {"value": None, "display": "-", "source": "-", "year": "-"}
        indicators[key] = entry

    raw_min_wage = meta.get("min_wage")
    if _is_bad_value(raw_min_wage):
        raw_min_wage = cached_profile.get("min_wage")
    min_wage = build_min_wage(code, raw_min_wage)

    sources = []
    years = []
    for entry in indicators.values():
        src = entry.get("source")
        if src and src != "-" and src not in sources:
            sources.append(src)
        y = _year_from_value(entry.get("year"))
        if y:
            years.append(y)

    asof = shared.get("economic_asof") or METADATA_ASOF or "-"
    return {
        "capital": indicators["capital"]["display"],
        "population": indicators["population"]["display"],
        "gdp": indicators["gdp"]["display"],
        "inflation": indicators["inflation"]["display"],
        "unemployment": indicators["unemployment"]["display"],
        "min_wage": min_wage,
        "min_wage_note": min_wage.get("note", ""),
        "indicators": indicators,
        "stats_source": " · ".join(sources) if sources else "-",
        "stats_year": max(years) if years else "-",
        "stats_asof": asof,
    }


# ---------------------------------------------------------------------------
# 환율 (기존 로직 유지)
# ---------------------------------------------------------------------------
MIN_RELIABLE_TRADING_DAYS = 30


def _yf_daily_close(ticker: str, days: int = 400) -> pd.Series:
    try:
        import yfinance as yf
        try:
            hist = yf.Ticker(ticker).history(period=f"{days}d", interval="1d", timeout=API_TIMEOUT)
        except TypeError:
            hist = yf.Ticker(ticker).history(period=f"{days}d", interval="1d")
        if hist.empty:
            return pd.Series(dtype="float64")
        return hist["Close"].dropna()
    except Exception as e:
        log.warning(f"[yfinance] {ticker} 조회 실패: {e}")
        return pd.Series(dtype="float64")


def _monthly_from_daily(daily: pd.Series, months: int = 12):
    if daily.empty:
        return [], []
    try:
        grouped = daily.groupby(daily.index.to_period("M")).last().tail(months)
        labels = [str(MONTH_KR[int(p.month) - 1]) for p in grouped.index]
        values = [float(v) for v in grouped]
        return labels, values
    except Exception as e:
        log.warning(f"월별 리샘플 실패: {e}")
        return [], []


def _direct_pair_series(currency: str) -> pd.Series:
    return _yf_daily_close(f"{currency}KRW=X")


def _cross_pair_series(currency: str) -> pd.Series:
    cur_usd = _yf_daily_close(f"{currency}USD=X")
    usd_krw = _yf_daily_close("USDKRW=X")
    if cur_usd.empty or usd_krw.empty:
        return pd.Series(dtype="float64")
    try:
        combined = pd.DataFrame({"cur_usd": cur_usd, "usd_krw": usd_krw}).sort_index().ffill().dropna()
        if combined.empty:
            return pd.Series(dtype="float64")
        return combined["cur_usd"] * combined["usd_krw"]
    except Exception as e:
        log.warning(f"교차 환산 실패: {e}")
        return pd.Series(dtype="float64")


def _recent_month_labels(months: int = 12) -> list:
    now = datetime.now(KST)
    labels = []
    y, m = now.year, now.month
    for _ in range(months):
        labels.append(MONTH_KR[m - 1])
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(labels))


def _synthetic_monthly_series(currency: str, unit_base: int, months: int = 12) -> tuple:
    base_rate = REFERENCE_RATE_PER_UNIT.get(currency, 1000.0) * unit_base
    seed_key = f"{currency}-{datetime.now(KST).strftime('%Y-%m-%d')}"
    rng = random.Random(seed_key)

    values = [0.0] * months
    values[-1] = base_rate
    for i in range(months - 2, -1, -1):
        change_pct = rng.uniform(-1.5, 1.5) / 100.0
        values[i] = values[i + 1] / (1 + change_pct)

    labels = _recent_month_labels(months)
    rounded_values = [_round_won(v) for v in values]
    return labels, rounded_values


def get_exchange_rate(country: dict, cached: dict) -> dict:
    currency = country["currency"]
    cached_fx = (cached or {}).get("exchange_rate", {})
    unit_base = SMALL_UNIT_CURRENCIES.get(currency, 1)
    yahoo_url = _yahoo_finance_url(currency)

    if currency == "KRW":
        return {"is_base": True, "unit_base": 1, "current_rate": 1.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "기준통화", "url": ""}

    try:
        daily = _direct_pair_series(currency)
        source = "Yahoo Finance"
        if len(daily) < MIN_RELIABLE_TRADING_DAYS:
            log.info(f"[{country['code']}] 직접 페어 데이터 부실({len(daily)}일) → 교차 환산으로 전환")
            cross = _cross_pair_series(currency)
            if len(cross) >= len(daily):
                daily, source = cross, "Yahoo Finance (USD 교차 환산)"

        if daily.empty:
            raise ValueError("일별 환율 데이터 없음(직접/교차 모두 실패)")

        current_rate_raw = float(daily.iloc[-1])
        change_pct = (
            round((current_rate_raw - float(daily.iloc[-2])) / float(daily.iloc[-2]) * 100, 2)
            if len(daily) >= 2 else 0.0
        )
        history_labels, history_values_raw = _monthly_from_daily(daily, months=12)
        if not history_labels:
            raise ValueError("월별 히스토리 생성 실패")

        return {
            "is_base": False,
            "unit_base": unit_base,
            "current_rate": _round_won(current_rate_raw * unit_base),
            "change_pct": change_pct,
            "history_labels": history_labels,
            "history_values": [_round_won(v * unit_base) for v in history_values_raw],
            "source": source,
            "url": yahoo_url,
        }
    except Exception as e:
        log.warning(f"[{country['code']}] 환율 수집 실패 → 캐시/합성 시계열로 대체: {e}")
        if cached_fx and cached_fx.get("history_values"):
            fx = dict(cached_fx)
            fx["url"] = yahoo_url
            fx.setdefault("unit_base", unit_base)
            fx.setdefault("is_base", False)
            return fx

        history_labels, history_values = _synthetic_monthly_series(currency, unit_base, months=12)
        current_rate = history_values[-1] if history_values else 0.0
        prev_rate = history_values[-2] if len(history_values) >= 2 else current_rate
        change_pct = round((current_rate - prev_rate) / prev_rate * 100, 2) if prev_rate else 0.0
        return {
            "is_base": False,
            "unit_base": unit_base,
            "current_rate": current_rate,
            "change_pct": change_pct,
            "history_labels": history_labels,
            "history_values": history_values,
            "source": "수집 실패 (참고용 추정치)",
            "url": yahoo_url,
        }


# ---------------------------------------------------------------------------
# 뉴스 (기존 로직 유지)
# ---------------------------------------------------------------------------
@lru_cache(maxsize=2048)
def translate_to_ko(text: str) -> str:
    original = text if isinstance(text, str) else ("" if text is None else str(text))
    if not original.strip():
        return original
    try:
        from deep_translator import GoogleTranslator
        translated = GoogleTranslator(source="auto", target="ko").translate(original)
        if not translated or not str(translated).strip():
            return original
        return translated
    except Exception as e:
        log.warning(f"번역 실패, 원문(영문) 그대로 사용: {e}")
        return original


def _google_news_url_ko(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"


def _google_news_top_url_ko() -> str:
    return "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko"


def _google_news_url_en(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"


def _google_news_search_link(query_en: str) -> str:
    try:
        return f"https://news.google.com/search?q={requests.utils.quote(query_en)}&hl=en-US&gl=US&ceid=US:en"
    except Exception:
        return "https://news.google.com/"


def _parse_entry(entry) -> dict:
    title_raw = getattr(entry, "title", "") or ""
    link = getattr(entry, "link", "") or ""
    if not title_raw or not link:
        raise ValueError("title 또는 link 누락")

    source = ""
    try:
        if hasattr(entry, "source"):
            source = getattr(entry.source, "title", "") or ""
        elif " - " in title_raw:
            source = title_raw.split(" - ")[-1]
    except Exception:
        source = ""

    summary = _strip_html(getattr(entry, "summary", ""))
    title = title_raw.split(" - ")[0] if " - " in title_raw else title_raw

    return {
        "title": title.strip(),
        "source": (source or "Google News").strip(),
        "link": link.strip(),
        "summary": _shorten(summary, 90) if summary else "",
    }


def _fetch_feed(url: str, limit: int) -> list:
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=API_TIMEOUT)
        if resp.status_code != 200:
            log.warning(f"피드 HTTP {resp.status_code}")
            return []
        feed = feedparser.parse(resp.content)
    except Exception as e:
        log.warning(f"피드 요청/파싱 실패: {e}")
        return []

    items = []
    try:
        entries = getattr(feed, "entries", []) or []
    except Exception as e:
        log.warning(f"피드 엔트리 접근 실패: {e}")
        return []

    for entry in entries[: max(limit * 4, limit)]:
        try:
            items.append(_parse_entry(entry))
        except Exception as e:
            log.warning(f"개별 기사 파싱 실패, 건너뜀: {e}")
            continue
        if len(items) >= limit:
            break
    return items


def _dedup_key(item: dict) -> str:
    try:
        link = (item.get("link") or "").strip().lower()
        if link:
            return link
        return (item.get("title") or "").strip().lower()
    except Exception:
        return ""


def _fallback_headline_item(country: dict) -> dict:
    try:
        query_en = country.get("name_en", "")
        link = _google_news_search_link(query_en)
        name_kr = country.get("name_kr", "")
    except Exception:
        link = "https://news.google.com/"
        name_kr = ""
    return {
        "title": FALLBACK_TEXT,
        "source": "Google News 검색",
        "link": link,
        "summary": f"{name_kr} 관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다." if name_kr else "관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다.",
        "lang": "ko",
        "translated": False,
        "original_title": "",
    }


def _get_headlines_kr_native(country: dict, cached: dict, seen_titles: set) -> list:
    limit = 3
    selected = []
    try:
        candidates = _fetch_feed(_google_news_top_url_ko(), limit=limit * 8)
        for item in candidates:
            try:
                key = _dedup_key(item)
                if not key or key in seen_titles:
                    continue
                item["lang"] = "ko"
                item["translated"] = False
                item["original_title"] = ""
                selected.append(item)
                seen_titles.add(key)
                if len(selected) >= limit:
                    break
            except Exception as e:
                log.warning(f"[KR] 개별 기사 처리 실패, 건너뜀: {e}")
                continue
    except Exception as e:
        log.warning(f"[KR] 국내 뉴스 수집 실패: {e}")

    if selected:
        return selected
    cached_headlines = (cached or {}).get("headlines", [])
    if cached_headlines:
        return cached_headlines
    return [_fallback_headline_item(country)]


def _get_headlines_global_translated(country: dict, cached: dict, seen_titles: set) -> list:
    limit = 3
    selected = []
    country_name_en = country["name_en"]
    country_code = country["code"]
    try:
        candidates = _fetch_feed(_google_news_url_en(f"{country_name_en} when:3d"), limit=limit * 8)
        for item in candidates:
            try:
                key = _dedup_key(item)
                if not key or key in seen_titles:
                    continue
                original_title = item.get("title", "")
                original_summary = item.get("summary", "")
                try:
                    item["title"] = translate_to_ko(original_title)
                    item["summary"] = translate_to_ko(original_summary) if original_summary else ""
                    item["lang"] = "en"
                    item["translated"] = (item["title"] != original_title)
                    item["original_title"] = original_title
                except Exception as e:
                    log.warning(f"[{country_code}] 헤드라인 번역 실패, 원문 유지: {e}")
                    item["title"] = original_title
                    item["summary"] = original_summary
                    item["lang"] = "en"
                    item["translated"] = False
                    item["original_title"] = original_title
                selected.append(item)
                seen_titles.add(key)
                if len(selected) >= limit:
                    break
            except Exception as e:
                log.warning(f"[{country_code}] 개별 기사 처리 실패, 건너뜀: {e}")
                continue
    except Exception as e:
        log.warning(f"[{country_code}] 현지 뉴스 수집 실패: {e}")

    if selected:
        return selected
    cached_headlines = (cached or {}).get("headlines", [])
    if cached_headlines:
        return cached_headlines
    return [_fallback_headline_item(country)]


def get_headlines(country: dict, cached: dict, seen_titles: set) -> list:
    try:
        if country.get("code") == "KR":
            return _get_headlines_kr_native(country, cached, seen_titles)
        return _get_headlines_global_translated(country, cached, seen_titles)
    except Exception as e:
        log.error(f"[{country.get('code')}] get_headlines 최상위 예외, 안전 Fallback 사용: {e}")
        cached_headlines = (cached or {}).get("headlines", [])
        return cached_headlines if cached_headlines else [_fallback_headline_item(country)]


# ---------------------------------------------------------------------------
# 비즈니스/HR 동향 (2개 카테고리, 카테고리당 최대 4건)
# ---------------------------------------------------------------------------
INDUSTRY_TOPICS = [
    {
        "category": "AUTO MARKET", "tag": "완성차·타이어 시장", "tag_class": "bg-rose-100 text-rose-700",
        "query_en_tpl": "{name_en} automotive tire market",
        "query_kr_tpl": "{name_kr} 완성차 타이어 시장",
        "max_items": 4,
    },
    {
        "category": "HR & LABOR", "tag": "노동법·인력", "tag_class": "bg-indigo-100 text-indigo-700",
        "query_en_tpl": "{name_en} labor law HR wage",
        "query_kr_tpl": "{name_kr} 노동법 최저임금 HR",
        "max_items": 4,
    },
]


def _fallback_trend_item(spec: dict, country: dict) -> dict:
    try:
        query_en = spec["query_en_tpl"].format(name_en=country.get("name_en", ""))
        link = _google_news_search_link(query_en)
    except Exception:
        link = "https://news.google.com/"
    return {
        "title": FALLBACK_TEXT,
        "desc": f"{spec.get('tag', '')} 관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다. 다음 갱신 시 자동으로 업데이트됩니다.",
        "source": "Google News 검색",
        "link": link,
        "lang": "ko",
        "translated": False,
        "original_title": "",
    }


def _fallback_trend_category(spec: dict, country: dict) -> dict:
    return {
        "category": spec.get("category", ""),
        "tag": spec.get("tag", ""),
        "tag_class": spec.get("tag_class", "bg-slate-100 text-slate-700"),
        "items": [_fallback_trend_item(spec, country)],
    }


def get_localized_items(query_kr: str, query_en: str, limit: int, when_filter: str,
                        seen_titles: set, kr_only: bool = False) -> list:
    results = []

    try:
        kr_candidates = _fetch_feed(_google_news_url_ko(f"{query_kr} {when_filter}"), limit=limit * 8)
        for it in kr_candidates:
            try:
                key = _dedup_key(it)
                if not key or key in seen_titles:
                    continue
                it["lang"] = "ko"
                it["translated"] = False
                it["original_title"] = ""
                results.append(it)
                seen_titles.add(key)
                if len(results) >= limit:
                    break
            except Exception as e:
                log.warning(f"[KO 검색] 개별 기사 처리 실패, 건너뜀: {e}")
                continue
    except Exception as e:
        log.warning(f"[KO 검색] 실패 ({query_kr}): {e}")

    remaining = limit - len(results)
    if remaining > 0 and not kr_only:
        try:
            en_candidates = _fetch_feed(_google_news_url_en(f"{query_en} {when_filter}"), limit=remaining * 8)
            for it in en_candidates:
                try:
                    key = _dedup_key(it)
                    if not key or key in seen_titles:
                        continue
                    original_title = it.get("title", "")
                    original_summary = it.get("summary", "")
                    try:
                        it["title"] = translate_to_ko(original_title)
                        it["summary"] = translate_to_ko(original_summary) if original_summary else ""
                        it["lang"] = "en"
                        it["translated"] = (it["title"] != original_title)
                        it["original_title"] = original_title
                    except Exception as e:
                        log.warning(f"번역 단계 실패, 원문(영문) 그대로 사용: {e}")
                        it["title"] = original_title
                        it["summary"] = original_summary
                        it["lang"] = "en"
                        it["translated"] = False
                        it["original_title"] = original_title
                    results.append(it)
                    seen_titles.add(key)
                    if len(results) >= limit:
                        break
                except Exception as e:
                    log.warning(f"[EN 대체 검색] 개별 기사 처리 실패, 건너뜀: {e}")
                    continue
        except Exception as e:
            log.warning(f"[EN 대체 검색] 실패 ({query_en}): {e}")

    return results


def _cached_trend_items(cached_cat) -> list:
    try:
        if not isinstance(cached_cat, dict):
            return []
        items = cached_cat.get("items")
        if isinstance(items, list) and items:
            return items
        if not _is_bad_value(cached_cat.get("title")) and not _is_bad_value(cached_cat.get("link")):
            return [{
                "title": str(cached_cat.get("title")),
                "desc": "" if _is_bad_value(cached_cat.get("desc")) else str(cached_cat.get("desc")),
                "source": "Google News" if _is_bad_value(cached_cat.get("source")) else str(cached_cat.get("source")),
                "link": str(cached_cat.get("link")),
                "lang": "ko" if _is_bad_value(cached_cat.get("lang")) else str(cached_cat.get("lang")),
                "translated": bool(cached_cat.get("translated")) if isinstance(cached_cat.get("translated"), bool) else False,
                "original_title": "" if _is_bad_value(cached_cat.get("original_title")) else str(cached_cat.get("original_title")),
            }]
    except Exception:
        pass
    return []


def get_industry_trends(country: dict, cached: dict, seen_titles: set) -> list:
    country_name_kr = country.get("name_kr", "")
    country_name_en = country.get("name_en", "")
    country_code = country.get("code")

    cached_by_category = {}
    try:
        cached_by_category = {t.get("category"): t for t in (cached or {}).get("hr_trends", []) if isinstance(t, dict)}
    except Exception:
        cached_by_category = {}

    trends = []
    for spec in INDUSTRY_TOPICS:
        items = []
        try:
            query_kr = spec["query_kr_tpl"].format(name_kr=country_name_kr)
            query_en = spec["query_en_tpl"].format(name_en=country_name_en)
            picked = get_localized_items(
                query_kr, query_en, limit=spec.get("max_items", 4), when_filter="when:14d",
                seen_titles=seen_titles, kr_only=(country_code == "KR"),
            )
            for it in picked:
                try:
                    items.append({
                        "title": _shorten(it.get("title", ""), 60),
                        "desc": it.get("summary") or _shorten(it.get("title", ""), 70),
                        "source": it.get("source", "Google News"),
                        "link": it.get("link", ""),
                        "lang": it.get("lang", "ko"),
                        "translated": it.get("translated", False),
                        "original_title": it.get("original_title", ""),
                    })
                except Exception as e:
                    log.warning(f"[{country_code}] '{spec.get('category')}' 항목 가공 실패, 건너뜀: {e}")
        except Exception as e:
            log.warning(f"[{country_code}] '{spec.get('category')}' 산업 동향 수집 예외: {e}")
            items = []

        if not items:
            items = _cached_trend_items(cached_by_category.get(spec["category"]))
        if not items:
            items = [_fallback_trend_item(spec, country)]

        trends.append({
            "category": spec["category"],
            "tag": spec["tag"],
            "tag_class": spec["tag_class"],
            "items": items,
        })

    return trends


# ---------------------------------------------------------------------------
# 캐시 / 아카이브 (기존 로직 유지)
# ---------------------------------------------------------------------------
def load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return {c["code"]: c for c in payload.get("countries", [])}
        except Exception as e:
            log.warning(f"캐시 로드 실패: {e}")
    return {}


def save_cache(data: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def clean_old_archives(archive_dir: str, max_days: int = 180) -> None:
    if not os.path.isdir(archive_dir):
        log.warning(f"[clean_old_archives] {archive_dir} 디렉토리가 없어 정리를 건너뜁니다.")
        return

    cutoff = datetime.now(KST).date() - timedelta(days=max_days)
    valid_dates = []

    try:
        filenames = os.listdir(archive_dir)
    except Exception as e:
        log.warning(f"[clean_old_archives] {archive_dir} 조회 실패: {e}")
        return

    for fname in filenames:
        if fname == "index.json" or not fname.endswith(".json"):
            continue
        date_part = fname[:-5]
        try:
            d = datetime.strptime(date_part, "%Y-%m-%d").date()
        except ValueError:
            continue

        if d < cutoff:
            try:
                os.remove(os.path.join(archive_dir, fname))
                log.info(f"[clean_old_archives] {fname} 삭제 ({max_days}일 경과)")
            except Exception as e:
                log.warning(f"[clean_old_archives] {fname} 삭제 실패: {e}")
            continue

        valid_dates.append(date_part)

    valid_dates.sort()
    index_payload = {
        "dates": valid_dates,
        "available_dates": valid_dates,
        "min_date": valid_dates[0] if valid_dates else None,
        "latest": valid_dates[-1] if valid_dates else None,
    }
    try:
        index_path = os.path.join(archive_dir, "index.json")
        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index_payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        log.warning(f"[clean_old_archives] index.json 저장 실패: {e}")


def save_archive(data: dict) -> None:
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    date_str = data["generated_at"][:10]

    archive_path = os.path.join(ARCHIVE_DIR, f"{date_str}.json")
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)

    clean_old_archives(ARCHIVE_DIR, max_days=ARCHIVE_RETENTION_DAYS)


def _is_legacy_archive(data: dict) -> bool:
    if not isinstance(data, dict):
        return True
    countries = data.get("countries")
    if not isinstance(countries, list) or len(countries) != len(COUNTRIES):
        return True
    for c in countries:
        if not isinstance(c, dict):
            return True
        profile = c.get("profile")
        if not isinstance(profile, dict) or not isinstance(profile.get("min_wage"), dict):
            return True
        if "source" not in profile["min_wage"] or "year" not in profile["min_wage"]:
            return True
        if not isinstance(profile.get("indicators"), dict):
            return True
        fx = c.get("exchange_rate")
        if not isinstance(fx, dict) or "unit_base" not in fx or "url" not in fx:
            return True
        hr_trends = c.get("hr_trends")
        if not isinstance(hr_trends, list) or len(hr_trends) != len(INDUSTRY_TOPICS):
            return True
        for t in hr_trends:
            if not isinstance(t, dict) or not isinstance(t.get("items"), list) or not t.get("items"):
                return True
        if "offices" not in c and "hubs" not in c:
            return True
    return False


def _migrate_exchange_rate_legacy(old_fx: dict, meta: dict) -> dict:
    currency = meta["currency"]
    unit_base_default = SMALL_UNIT_CURRENCIES.get(currency, 1)
    yahoo_url = _yahoo_finance_url(currency)

    if currency == "KRW":
        return {"is_base": True, "unit_base": 1, "current_rate": 1.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "기준통화", "url": ""}

    old_fx = old_fx if isinstance(old_fx, dict) else {}
    labels = old_fx.get("history_labels") if isinstance(old_fx.get("history_labels"), list) else []
    values = old_fx.get("history_values") if isinstance(old_fx.get("history_values"), list) else []
    if len(labels) != len(values):
        labels, values = [], []

    try:
        current_rate = float(old_fx.get("current_rate")) if not _is_bad_value(old_fx.get("current_rate")) else 0.0
    except Exception:
        current_rate = 0.0

    if current_rate <= 0 and not values:
        labels, values = _synthetic_monthly_series(currency, unit_base_default, months=12)
        current_rate = values[-1] if values else 0.0

    try:
        change_pct = float(old_fx.get("change_pct")) if not _is_bad_value(old_fx.get("change_pct")) else 0.0
    except Exception:
        change_pct = 0.0

    unit_base = old_fx.get("unit_base") if isinstance(old_fx.get("unit_base"), int) and old_fx.get("unit_base") > 0 else unit_base_default

    return {
        "is_base": False,
        "unit_base": unit_base,
        "current_rate": _round_won(current_rate),
        "change_pct": round(change_pct, 2),
        "history_labels": [str(x) for x in labels],
        "history_values": [_round_won(v) for v in values],
        "source": old_fx.get("source") if not _is_bad_value(old_fx.get("source")) else "이전 데이터(마이그레이션)",
        "url": yahoo_url,
    }


def _migrate_profile_legacy(old_profile: dict, meta: dict) -> dict:
    old_profile = old_profile if isinstance(old_profile, dict) else {}
    code = meta.get("code", "")
    meta_profile = meta.get("meta_profile", {}) or {}
    old_indicators = old_profile.get("indicators") if isinstance(old_profile.get("indicators"), dict) else {}
    old_source = old_profile.get("stats_source")
    old_source = STATIC_SOURCE if _is_bad_value(old_source) else str(old_source)
    meta_year = _metadata_year()

    indicators = {}
    for key in ("capital", "population", "gdp", "inflation", "unemployment"):
        entry = old_indicators.get(key) if isinstance(old_indicators.get(key), dict) else None
        if entry is None:
            old_value = old_profile.get(key)
            if not _is_bad_value(old_value) and str(old_value) != "-":
                entry = {"value": old_value, "display": str(old_value), "source": old_source, "year": meta_year}
        if entry is None:
            entry = _static_entry(key, meta_profile.get(key), meta_year)
        if entry is None:
            entry = {"value": None, "display": "-", "source": "-", "year": "-"}
        indicators[key] = entry

    raw_min_wage = old_profile.get("min_wage")
    if _is_bad_value(raw_min_wage) or (isinstance(raw_min_wage, str) and raw_min_wage.strip() == "-"):
        raw_min_wage = meta_profile.get("min_wage")
    min_wage = build_min_wage(code, raw_min_wage)

    sources = []
    for entry in indicators.values():
        src = entry.get("source")
        if src and src != "-" and src not in sources:
            sources.append(src)

    return {
        "capital": indicators["capital"]["display"],
        "population": indicators["population"]["display"],
        "gdp": indicators["gdp"]["display"],
        "inflation": indicators["inflation"]["display"],
        "unemployment": indicators["unemployment"]["display"],
        "min_wage": min_wage,
        "min_wage_note": min_wage.get("note", ""),
        "indicators": indicators,
        "stats_source": " · ".join(sources) if sources else "-",
        "stats_year": old_profile.get("stats_year", "-"),
        "stats_asof": old_profile.get("stats_asof") if not _is_bad_value(old_profile.get("stats_asof")) else (METADATA_ASOF or "-"),
    }


def _migrate_headline_item_legacy(old_item: dict) -> dict:
    title = old_item.get("title")
    title = FALLBACK_TEXT if _is_bad_value(title) else str(title)
    link = old_item.get("link")
    link = "" if _is_bad_value(link) else str(link)
    return {
        "title": title,
        "source": "Google News" if _is_bad_value(old_item.get("source")) else str(old_item.get("source")),
        "link": link,
        "summary": "" if _is_bad_value(old_item.get("summary")) else str(old_item.get("summary")),
        "lang": "ko" if _is_bad_value(old_item.get("lang")) else str(old_item.get("lang")),
        "translated": bool(old_item.get("translated")) if isinstance(old_item.get("translated"), bool) else False,
        "original_title": "" if _is_bad_value(old_item.get("original_title")) else str(old_item.get("original_title")),
    }


def _migrate_headlines_legacy(old_headlines, meta: dict) -> list:
    items = []
    if isinstance(old_headlines, list):
        for h in old_headlines:
            if isinstance(h, dict) and not _is_bad_value(h.get("title")) and not _is_bad_value(h.get("link")):
                items.append(_migrate_headline_item_legacy(h))
    if not items:
        items = [_fallback_headline_item(meta)]
    return items


def _migrate_hr_trends_legacy(old_trends, meta: dict) -> list:
    old_by_category = {}
    if isinstance(old_trends, list):
        for t in old_trends:
            if isinstance(t, dict) and t.get("category"):
                old_by_category[t["category"]] = t

    result = []
    for spec in INDUSTRY_TOPICS:
        items = _cached_trend_items(old_by_category.get(spec["category"]))
        if items:
            result.append({
                "category": spec["category"],
                "tag": spec["tag"],
                "tag_class": spec["tag_class"],
                "items": items[: spec.get("max_items", 4)],
            })
        else:
            result.append(_fallback_trend_category(spec, meta))
    return result


def migrate_legacy_archive_file(path: str, date_str: str) -> None:
    old_data = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        if raw.strip():
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                old_data = parsed
    except Exception as e:
        log.warning(f"[migrate] {path} 로드 실패, 전체를 Fallback으로 재구성: {e}")

    old_countries = old_data.get("countries", []) if isinstance(old_data.get("countries"), list) else []
    old_by_code = {c.get("code"): c for c in old_countries if isinstance(c, dict) and c.get("code")}

    new_countries = []
    for meta in COUNTRIES:
        meta_copy = copy.deepcopy(meta)
        old_country = old_by_code.get(meta_copy["code"]) or {}
        old_offices = old_country.get("offices") if isinstance(old_country.get("offices"), list) and old_country.get("offices") else None
        offices = old_offices if old_offices else meta_copy.get("offices", [])
        new_countries.append({
            "code": meta_copy["code"],
            "region": meta_copy["region"],
            "name_kr": meta_copy["name_kr"],
            "name_en": meta_copy["name_en"],
            "currency": meta_copy["currency"],
            "flag": meta_copy["flag"],
            "offices": offices,
            "hubs": offices,
            "profile": _migrate_profile_legacy(old_country.get("profile", {}), meta_copy),
            "exchange_rate": _migrate_exchange_rate_legacy(old_country.get("exchange_rate", {}), meta_copy),
            "headlines": _migrate_headlines_legacy(old_country.get("headlines", []), meta_copy),
            "hr_trends": _migrate_hr_trends_legacy(old_country.get("hr_trends", []), meta_copy),
        })

    new_data = {
        "generated_at": f"{date_str}T06:00:00+09:00",
        "generated_at_display": f"{date_str} 06:00 KST",
        "as_of_display": f"{date_str} 06:00 KST 기준",
        "regions": REGIONS,
        "countries": new_countries,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(new_data, f, ensure_ascii=False, default=str)

    log.info(f"[migrate] {os.path.basename(path)} 를 최신 스키마로 마이그레이션 완료 ({len(new_countries)}개국)")


def migrate_all_legacy_archives() -> None:
    if not os.path.isdir(ARCHIVE_DIR):
        return

    try:
        filenames = os.listdir(ARCHIVE_DIR)
    except Exception as e:
        log.warning(f"[migrate] {ARCHIVE_DIR} 조회 실패, 마이그레이션을 건너뜁니다: {e}")
        return

    date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    migrated_count = 0

    for fname in filenames:
        if fname == "index.json" or not fname.endswith(".json"):
            continue
        date_str = fname[:-5]
        if not date_pattern.match(date_str):
            continue
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue

        path = os.path.join(ARCHIVE_DIR, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = f.read()
            data = json.loads(raw) if raw.strip() else {}
        except Exception as e:
            log.warning(f"[migrate] {fname} 로드 실패(구버전으로 간주하고 재구성): {e}")
            data = {}

        try:
            if _is_legacy_archive(data):
                migrate_legacy_archive_file(path, date_str)
                migrated_count += 1
        except Exception as e:
            log.error(f"[migrate] {fname} 마이그레이션 실패, 다음 파일로 계속 진행: {e}")
            continue

    if migrated_count:
        log.info(f"[migrate] 구버전 아카이브 {migrated_count}건을 최신 스키마로 자동 마이그레이션했습니다.")
        try:
            clean_old_archives(ARCHIVE_DIR, max_days=ARCHIVE_RETENTION_DAYS)
        except Exception as e:
            log.warning(f"[migrate] 마이그레이션 후 index.json 재생성 실패: {e}")


# ---------------------------------------------------------------------------
# 국가 단위 수집 / 정렬 / 메인
# ---------------------------------------------------------------------------
def _resolve_offices(country: dict, shared: dict) -> tuple:
    official = (shared or {}).get("offices", {}) or {}
    names = official.get(country["code"])
    if isinstance(names, list) and names:
        cleaned = [str(n).strip() for n in names if not _is_bad_value(n)]
        if cleaned:
            return cleaned, "Hankook Tire 공식 홈페이지"
    return copy.deepcopy(country.get("offices", [])), "내부 메타데이터"


def collect_country(country: dict, cache_by_code: dict, shared: dict = None) -> dict:
    shared = shared or {}
    country_code = country["code"]
    country_currency = country["currency"]
    cached = cache_by_code.get(country_code, {})
    log.info(f"[{country_code}] 수집 시작 — {country['name_kr']} (통화: {country_currency})")

    seen_titles: set = set()

    offices, offices_source = _resolve_offices(country, shared)
    result = {
        "code": country["code"],
        "region": country["region"],
        "name_kr": country["name_kr"],
        "name_en": country["name_en"],
        "currency": country["currency"],
        "flag": country["flag"],
        "offices": offices,
        "hubs": offices,
        "offices_source": offices_source,
    }

    try:
        result["profile"] = build_profile(country, cached, shared)
    except Exception as e:
        log.error(f"[{country_code}] 프로필 구성 실패, 캐시/빈 값으로 대체 후 계속 진행: {e}")
        result["profile"] = (cached or {}).get("profile", {})

    try:
        result["exchange_rate"] = get_exchange_rate(country, cached)
    except Exception as e:
        log.error(f"[{country_code}] 환율 수집 실패, 캐시/기본값으로 대체 후 계속 진행: {e}")
        result["exchange_rate"] = (cached or {}).get(
            "exchange_rate",
            {"is_base": False, "unit_base": 1, "current_rate": 0.0, "change_pct": 0.0,
             "history_labels": [], "history_values": [], "source": "수집 실패", "url": _yahoo_finance_url(country_currency)},
        )

    try:
        result["headlines"] = get_headlines(country, cached, seen_titles)
    except Exception as e:
        log.error(f"[{country_code}] 헤드라인 수집 실패, Fallback 문구로 대체 후 계속 진행: {e}")
        cached_headlines = (cached or {}).get("headlines", [])
        result["headlines"] = cached_headlines if cached_headlines else [_fallback_headline_item(country)]

    try:
        result["hr_trends"] = get_industry_trends(country, cached, seen_titles)
    except Exception as e:
        log.error(f"[{country_code}] 산업 동향 수집 실패, Fallback 문구로 대체 후 계속 진행: {e}")
        cached_trends = (cached or {}).get("hr_trends", [])
        valid_cached = [t for t in cached_trends if isinstance(t, dict) and isinstance(t.get("items"), list) and t.get("items")]
        result["hr_trends"] = valid_cached if len(valid_cached) == len(INDUSTRY_TOPICS) else [
            _fallback_trend_category(spec, country) for spec in INDUSTRY_TOPICS
        ]

    log.info(f"[{country_code}] 수집 완료")
    return result


def _build_country_fallback(country_code: str, shared: dict = None) -> dict:
    meta = next((copy.deepcopy(c) for c in COUNTRIES if c["code"] == country_code), None)
    if meta is None:
        meta = {"code": country_code, "region": "ALL", "name_kr": country_code,
                "name_en": country_code, "currency": "USD", "flag": "un", "offices": [],
                "meta_profile": {}}
    offices, offices_source = _resolve_offices(meta, shared or {})
    try:
        profile = build_profile(meta, {}, shared or {})
    except Exception as e:
        log.error(f"[{country_code}] 안전망 프로필 구성 실패: {e}")
        profile = {"capital": "-", "population": "-", "gdp": "-", "inflation": "-", "unemployment": "-",
                   "min_wage": build_min_wage(country_code, None), "min_wage_note": "",
                   "indicators": {}, "stats_source": "-", "stats_year": "-", "stats_asof": "-"}
    return {
        "code": meta["code"], "region": meta["region"], "name_kr": meta["name_kr"],
        "name_en": meta["name_en"], "currency": meta["currency"], "flag": meta["flag"],
        "offices": offices, "hubs": offices, "offices_source": offices_source,
        "profile": profile,
        "exchange_rate": {"is_base": False, "unit_base": 1, "current_rate": 0.0, "change_pct": 0.0,
                          "history_labels": [], "history_values": [], "source": "수집 실패",
                          "url": _yahoo_finance_url(meta.get("currency", ""))},
        "headlines": [_fallback_headline_item(meta)],
        "hr_trends": [_fallback_trend_category(spec, meta) for spec in INDUSTRY_TOPICS],
    }


def _prepare_shared_sources() -> dict:
    shared = {"offices": {}, "economic": {}, "economic_asof": None}

    try:
        shared["offices"] = fetch_hankook_offices(COUNTRIES) or {}
    except Exception as e:
        log.error(f"한국타이어 공식 거점 정보 처리 실패, 메타데이터 거점으로 대체: {e}")
        shared["offices"] = {}

    try:
        economic, asof = load_economic_data(COUNTRIES)
        shared["economic"] = economic or {}
        shared["economic_asof"] = asof or None
    except Exception as e:
        log.error(f"거시경제 지표 처리 실패, 메타데이터 정적 값으로 대체: {e}")
        shared["economic"] = {}
        shared["economic_asof"] = None

    return shared


def main() -> dict:
    try:
        cache_by_code = load_cache()
    except Exception as e:
        log.error(f"캐시 로드 중 예기치 못한 오류, 빈 캐시로 계속 진행: {e}")
        cache_by_code = {}

    shared = _prepare_shared_sources()

    collected = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_country, copy.deepcopy(c), cache_by_code, shared): c["code"] for c in COUNTRIES}
        for future in as_completed(futures):
            code = futures[future]
            try:
                collected[code] = future.result()
            except Exception as e:
                log.error(f"[{code}] 국가 단위 수집이 스레드에서 완전히 실패, 캐시/안전망으로 대체: {e}")
                try:
                    cached_fallback = cache_by_code.get(code)
                    collected[code] = cached_fallback if cached_fallback else _build_country_fallback(code, shared)
                except Exception as inner_e:
                    log.error(f"[{code}] 안전망 구성 중에도 오류 발생, 최소 골격으로 대체: {inner_e}")
                    collected[code] = _build_country_fallback(code, shared)

    countries_result = [collected.get(c["code"]) or _build_country_fallback(c["code"], shared) for c in COUNTRIES]

    now_kst = datetime.now(KST)
    data = {
        "generated_at": now_kst.isoformat(),
        "generated_at_display": now_kst.strftime("%Y-%m-%d %H:%M KST"),
        "as_of_display": f"{now_kst.strftime('%Y-%m-%d')} 06:00 KST 기준",
        "regions": REGIONS,
        "countries": countries_result,
    }

    cache_saved = False
    try:
        save_cache(data)
        cache_saved = True
        log.info(f"data/countries_data.json 저장 완료 ({len(countries_result)}개국)")
    except Exception as e:
        log.error(f"data/countries_data.json 저장 실패: {e}")

    try:
        save_archive(data)
        log.info(f"docs/archive/{now_kst.strftime('%Y-%m-%d')}.json 저장 및 보존정책 적용 완료")
    except Exception as e:
        log.error(f"아카이브 저장/보존정책 처리 실패 (사이트 자체는 정상 생성됩니다): {e}")

    try:
        migrate_all_legacy_archives()
    except Exception as e:
        log.error(f"구버전 아카이브 자동 마이그레이션 중 오류(사이트 자체 생성에는 영향 없음): {e}")

    if not cache_saved:
        raise RuntimeError(
            "data/countries_data.json 저장에 실패해 사이트를 생성할 수 없습니다. "
            "위 로그의 'data/countries_data.json 저장 실패' 항목을 확인하세요."
        )

    log.info(f"전체 {len(countries_result)}개국 수집 완료 → {CACHE_PATH} / {ARCHIVE_DIR}")
    return data


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("collector.py 실행 중 처리되지 않은 예외가 발생했습니다 (원인은 위 스택 트레이스 참고)")
        raise
