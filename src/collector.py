"""
collector.py
------------
Global Daily News — 한국타이어 진출 39개국 일일 데이터 수집기.

수집 항목과 출처
  1) 수도
     data/countries_metadata.json 의 capital 고정값을 사용한다. 외부 API는 호출하지 않는다.
  2) 총 인구 / 연간 GDP
     - 인구: World Bank SP.POP.TOTL
     - GDP: IMF WEO NGDPD(1순위) → World Bank NY.GDP.MKTP.CD
     - IMF 전망치(당해·미래 연도)는 제외하고 직전 연도까지의 실측/추정치만 사용한다.
  3) 인플레이션 / 실업률
     - 인플레이션: IMF WEO PCPIPCH → World Bank FP.CPI.TOTL.ZG
     - 실업률: ILOSTAT → World Bank SL.UEM.TOTL.ZS
     - 1순위 값의 기준연도가 오래되면 2순위도 조회해 더 최신 연도를 채택한다.
     - 모두 실패하면 Trading Economics 링크로 대체한다
       (display "최신 지표 확인", link, source "Trading Economics").
  4) 최저임금
     - OECD(MW_CURP)와 data/minwage_manual.json 중 적용연도가 더 최신인 값을 채택한다.
     - 법정 최저임금이 없는 국가는 display "-", note "단체협약(CBA)으로 결정".
  5) 한국타이어 거점
     - 공식 Global Network 페이지를 파싱해 "공식 영문 조직명 (한글 도시명)" 형식으로 저장한다.
     - 검증을 통과한 결과만 data/hankook_offices_cache.json(schema_version 2)에 캐싱한다.
     - 구버전·불량 캐시는 무시하고 다시 크롤링한다.
     - 실패 시 "공식 거점 정보 확인 중".
  6) 현지 뉴스
     - 3대 카테고리로 분류해 카테고리당 최대 4건을 수집한다:
       HR & LABOR / SOCIETY & CULTURE / POLITICS & ECONOMY.
     - AUTO MARKET(완성차·타이어) 동향은 hr_trends 에 별도로 유지한다.
     - 국가 단위 seen_titles 로 중복을 차단한다.
     - headlines 는 3대 카테고리에서 파생한 상위 3건이다(구 템플릿 호환용).
  7) 번역
     - 모든 기사에 대해 langdetect 로 원문 언어를 감지한다.
     - 원문이 한국어가 아니면 deep-translator 로 번역한다.
     - 번역은 병렬 수집이 끝난 뒤 메인 스레드에서 순차·지연 처리한다.
     - 연속 실패가 누적되면 이번 실행에서 번역을 중단하고 원문을 유지한다.
     - 기사에는 title, original_title, original_lang, original_lang_upper,
       is_translated, translate_label("EN > KR 번역") 필드를 저장한다.
     - link 는 항상 원문 기사 URL이다.
  8) 갱신 주기
     - 뉴스·환율은 매일 갱신한다.
     - 경제지표는 data/cache_economic.json(cache_version 2)에서 항목별 30일 경과분만 재호출한다.
     - 모든 아카이브는 schema_version 3 으로 통일하고 구버전은 자동 마이그레이션한다.
     - 180일 보존 후 삭제하며 docs/archive/index.json 을 갱신한다.
  9) 안전 처리
     - 모든 외부 호출은 7초 타임아웃과 try/except 로 감싼다.
     - 호출 간 0.2~0.3초 지연을 둔다.
     - 실패 시 안전한 안내 문구로 대체한다.
"""

import os
import re
import copy
import json
import time
import html
import random
import logging
import unicodedata
from datetime import datetime, timedelta
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
ECONOMIC_CACHE_PATH = os.path.join(DATA_DIR, "cache_economic.json")
MINWAGE_MANUAL_PATH = os.path.join(DATA_DIR, "minwage_manual.json")
ARCHIVE_DIR = os.path.join(BASE_DIR, "docs", "archive")
ARCHIVE_INDEX_PATH = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_RETENTION_DAYS = 180

SCHEMA_VERSION = 3
ECONOMIC_CACHE_VERSION = 2
HANKOOK_CACHE_VERSION = 2

HANKOOK_NETWORK_URL = "https://www.hankooktire.com/global/ko/company/global-network.html"
HANKOOK_NETWORK_URLS = [
    HANKOOK_NETWORK_URL,
    "https://www.hankooktire.com/global/en/company/global-network.html",
]
HANKOOK_MIN_COUNTRIES = 5

ECONOMIC_REFRESH_DAYS = 30
ECONOMIC_ITEMS = ("population", "gdp", "inflation", "unemployment", "min_wage")
API_TIMEOUT = 7
API_DELAY_MIN = 0.2
API_DELAY_MAX = 0.3
SOURCE_MAX_CONSECUTIVE_FAILS = 5

TRANSLATE_DELAY = 0.3
TRANSLATE_RETRY_BACKOFF = 1.5
TRANSLATE_MAX_CONSECUTIVE_FAILS = 8

MONTH_KR = ["1월", "2월", "3월", "4월", "5월", "6월",
            "7월", "8월", "9월", "10월", "11월", "12월"]

MAX_WORKERS = 8

SMALL_UNIT_CURRENCIES = {"VND": 100, "JPY": 100, "IDR": 100, "COP": 100, "CLP": 100, "KZT": 100, "HUF": 100}
WON_DECIMALS = 1
FALLBACK_TEXT = "현지 산업 및 정책 모니터링 중"
PLACEHOLDER_TEXT = "데이터 확인 중"
OFFICE_PLACEHOLDER = "공식 거점 정보 확인 중"
TE_DISPLAY = "최신 지표 확인"
TE_SOURCE = "Trading Economics"
CAPITAL_SOURCE = "기본 정보"
API_SOURCES = {"World Bank", "IMF WEO", "ILOSTAT", "OECD", "수동 보완"}

NEWS_WHEN = "when:7d"
AUTO_WHEN = "when:14d"

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

TE_SLUG_OVERRIDES = {
    "KR": "south-korea",
    "US": "united-states",
    "GB": "united-kingdom",
    "CZ": "czech-republic",
    "AE": "united-arab-emirates",
    "SA": "saudi-arabia",
}

MIN_WAGE_MANUAL_DEFAULT = {
    "KR": {"display": "10,320원 / 시간", "note": "2026년 최저임금", "year": 2026, "basis": "고용노동부 고시"},
    "CN": {"display": "2,740위안 / 월", "note": "상하이시 기준 (지역별 상이)", "year": 2025, "basis": "상하이시 인력자원사회보장국 고시"},
    "US": {"display": "7.25달러 / 시간", "note": "연방 기준 (주별 상이)", "year": 2026, "basis": "U.S. Department of Labor"},
    "CA": {"display": "17.20캐나다달러 / 시간", "note": "온타리오주 기준", "year": 2025, "basis": "Ontario Ministry of Labour"},
    "DE": {"display": "13.90유로 / 시간", "note": "법정 최저임금", "year": 2026, "basis": "Eurostat"},
    "HU": {"display": "322,800포린트 / 월", "note": "월 총액 기준", "year": 2026, "basis": "Eurostat"},
    "GB": {"display": "12.71파운드 / 시간", "note": "21세 이상 National Living Wage", "year": 2026, "basis": "GOV.UK (Low Pay Commission)"},
    "FR": {"display": "12.02유로 / 시간", "note": "SMIC", "year": 2026, "basis": "Eurostat"},
    "ES": {"display": "1,184유로 / 월", "note": "14회 지급 기준 (SMI)", "year": 2025, "basis": "Eurostat"},
    "PL": {"display": "4,806즈워티 / 월", "note": "월 총액 기준", "year": 2026, "basis": "Eurostat"},
    "CZ": {"display": "22,400코루나 / 월", "note": "월 총액 기준", "year": 2026, "basis": "Eurostat"},
    "NL": {"display": "14.71유로 / 시간", "note": "21세 이상", "year": 2026, "basis": "Eurostat"},
    "RO": {"display": "4,050레이 / 월", "note": "월 총액 기준", "year": 2025, "basis": "Eurostat"},
    "HR": {"display": "1,050유로 / 월", "note": "월 총액 기준", "year": 2026, "basis": "Eurostat"},
    "TR": {"display": "28,075.5리라 / 월", "note": "순액 기준", "year": 2026, "basis": "Eurostat"},
    "RU": {"display": "27,093루블 / 월", "note": "법정 최저임금(MROT)", "year": 2026, "basis": "ILO"},
    "UA": {"display": "8,647흐리브냐 / 월", "note": "월 기준", "year": 2026, "basis": "ILO"},
    "EG": {"display": "7,000이집트파운드 / 월", "note": "민간 부문", "year": 2025, "basis": "ILO"},
    "KZ": {"display": "85,000텡게 / 월", "note": "월 기준", "year": 2025, "basis": "ILO"},
    "MX": {"display": "315.04페소 / 일", "note": "일반 최저임금", "year": 2026, "basis": "ILO"},
    "BR": {"display": "1,621헤알 / 월", "note": "연방 최저임금", "year": 2026, "basis": "ILO"},
    "CL": {"display": "529,000페소 / 월", "note": "2025년 7월 적용분", "year": 2025, "basis": "ILO"},
    "CO": {"display": "1,750,905페소 / 월", "note": "교통보조금 별도", "year": 2026, "basis": "ILO"},
    "ID": {"display": "5,396,761루피아 / 월", "note": "자카르타 주 최저임금(UMP) 기준", "year": 2025, "basis": "ILO"},
    "AU": {"display": "24.95호주달러 / 시간", "note": "전국 최저임금", "year": 2025, "basis": "Fair Work Commission"},
    "JP": {"display": "1,121엔 / 시간", "note": "전국 가중평균 (도쿄 1,226엔)", "year": 2025, "basis": "일본 후생노동성"},
    "MY": {"display": "1,700링깃 / 월", "note": "월 기준", "year": 2025, "basis": "ILO"},
    "TH": {"display": "400바트 / 일", "note": "방콕 기준 (지역별 337~400바트)", "year": 2025, "basis": "ILO"},
    "VN": {"display": "5,310,000동 / 월", "note": "1지역 기준", "year": 2026, "basis": "ILO"},
    "TW": {"display": "29,500대만달러 / 월", "note": "시급 196대만달러", "year": 2026, "basis": "ILO"},
    "MA": {"display": "17.07디르함 / 시간", "note": "SMIG 참고치 · 공식 고시 확인 필요", "year": 2025, "basis": "수동 보완"},
    "RS": {"display": "308디나르 / 시간", "note": "참고치 · 공식 고시 확인 필요", "year": 2025, "basis": "수동 보완"},
    "PA": {"display": "업종·지역별 상이", "note": "업종·지역별 차등 고시", "year": 2026, "basis": "수동 보완"},
    "SE": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2026, "basis": "수동 보완"},
    "AT": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2026, "basis": "수동 보완"},
    "IT": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2026, "basis": "수동 보완"},
    "SG": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2026, "basis": "수동 보완"},
    "AE": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2026, "basis": "수동 보완"},
    "SA": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2026, "basis": "수동 보완"},
}

CURRENCY_KO = {
    "KRW": "원", "USD": "달러", "EUR": "유로", "GBP": "파운드", "JPY": "엔", "CNY": "위안",
    "CAD": "캐나다달러", "AUD": "호주달러", "HUF": "포린트", "PLN": "즈워티", "CZK": "코루나",
    "RON": "레이", "TRY": "리라", "RUB": "루블", "UAH": "흐리브냐", "MXN": "페소", "BRL": "헤알",
    "CLP": "페소", "COP": "페소", "IDR": "루피아", "MYR": "링깃", "THB": "바트", "VND": "동",
    "TWD": "대만달러", "SEK": "크로나", "SGD": "싱가포르달러", "KZT": "텡게", "EGP": "이집트파운드",
    "RSD": "디나르", "MAD": "디르함", "PAB": "발보아", "AED": "디르함", "SAR": "리얄",
}
PAY_PERIOD_KO = {"H": "시간", "D": "일", "W": "주", "M": "월", "A": "연"}


# ---------------------------------------------------------------------------
# 공통 유틸
# ---------------------------------------------------------------------------
def _is_bad_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"error", "err", "undefined", "null", "none", "nan", "n/a", "", "[object object]"}:
        return True
    return False


def _now_iso() -> str:
    return datetime.now(KST).isoformat()


def _clean_text(text) -> str:
    try:
        t = html.unescape(str(text or ""))
        t = re.sub(r"<[^>]+>", " ", t)
        t = t.replace("\xa0", " ")
        return " ".join(t.split()).strip()
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


def _iso2_for(code: str) -> str:
    return ISO2_OVERRIDES.get(code, code)


def _normalize_code(code) -> str:
    c = str(code or "").strip().upper()
    return "GB" if c == "UK" else c


# ---------------------------------------------------------------------------
# 메타데이터 (식별 정보 + 수도 고정값만 사용)
# ---------------------------------------------------------------------------
def load_countries_metadata() -> list:
    if not os.path.exists(METADATA_PATH):
        raise FileNotFoundError(f"{METADATA_PATH} 파일이 없습니다. 39개국 식별 메타데이터 파일을 먼저 준비하세요.")

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
        code = _normalize_code(row["code"])
        capital = row.get("capital")
        countries.append({
            "code": code,
            "region": row.get("region", "ALL"),
            "name_kr": row.get("name_ko", code),
            "name_en": row.get("name_en", code),
            "currency": row.get("currency", "USD"),
            "flag": code.lower(),
            "curr_unit": row.get("curr_unit", ""),
            "capital": "" if _is_bad_value(capital) else str(capital).strip(),
        })

    if not countries:
        raise ValueError(f"{METADATA_PATH} 에서 유효한 국가를 하나도 읽지 못했습니다.")

    return countries


COUNTRIES = load_countries_metadata()
COUNTRY_CODES = {c["code"] for c in COUNTRIES}


# ---------------------------------------------------------------------------
# 외부 API 공통 래퍼 (7초 타임아웃 / 예외 흡수 / 연속 실패 차단)
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
        if status == 200:
            _register_success(source)
        else:
            _register_failure(source, f"HTTP {status}")
            log.warning(f"[{source}] HTTP {status}: {url[:140]}")
    except Exception as e:
        _register_failure(source, str(e)[:80])
        log.warning(f"[{source}] 요청 실패: {str(e)[:120]}")
    finally:
        _polite_sleep()
    return status, payload, text


# ---------------------------------------------------------------------------
# 표시 문자열 포맷터
# ---------------------------------------------------------------------------
def format_population(people: float) -> str:
    try:
        p = float(people)
        man = int(round(p / 10000))
        if man <= 0:
            return f"{int(p):,}명"
        eok, rem = divmod(man, 10000)
        if eok:
            return f"{eok:,}억 {rem:,}만 명" if rem else f"{eok:,}억 명"
        return f"{man:,}만 명"
    except Exception:
        return PLACEHOLDER_TEXT


def format_gdp(billions_usd: float) -> str:
    try:
        eok_total = int(round(float(billions_usd) * 10))
        jo, rem = divmod(eok_total, 10000)
        if jo and rem:
            return f"{jo:,}조 {rem:,}억 USD"
        if jo:
            return f"{jo:,}조 USD"
        return f"{eok_total:,}억 USD"
    except Exception:
        return PLACEHOLDER_TEXT


def format_percent(value: float) -> str:
    try:
        return f"{float(value):.1f}%"
    except Exception:
        return PLACEHOLDER_TEXT


def _make_entry(key: str, value, source: str, year):
    value = _to_float(value)
    if value is None:
        return None
    if key == "population":
        if value <= 0:
            return None
        display = format_population(value)
    elif key == "gdp":
        if value <= 0:
            return None
        display = format_gdp(value)
    elif key in ("inflation", "unemployment"):
        display = format_percent(value)
    else:
        display = str(value)
    return {"value": value, "display": display, "source": source, "year": year}


def _placeholder_entry() -> dict:
    return {"value": None, "display": PLACEHOLDER_TEXT, "source": "-", "year": "-"}


def _te_slug(country: dict) -> str:
    code = country.get("code", "")
    if code in TE_SLUG_OVERRIDES:
        return TE_SLUG_OVERRIDES[code]
    try:
        name = unicodedata.normalize("NFKD", str(country.get("name_en", "")))
        name = "".join(ch for ch in name if not unicodedata.combining(ch)).lower()
        return re.sub(r"[^a-z0-9]+", "-", name).strip("-") or code.lower()
    except Exception:
        return code.lower()


def _te_entry(country: dict, key: str) -> dict:
    path = "inflation-cpi" if key == "inflation" else "unemployment-rate"
    return {
        "value": None,
        "display": TE_DISPLAY,
        "link": f"https://tradingeconomics.com/{_te_slug(country)}/{path}",
        "source": TE_SOURCE,
        "year": "-",
    }


def _capital_entry(country: dict) -> dict:
    capital = country.get("capital", "")
    if _is_bad_value(capital):
        return _placeholder_entry()
    return {"value": capital, "display": capital, "source": CAPITAL_SOURCE, "year": "-"}


def _entry_valid(entry) -> bool:
    return (isinstance(entry, dict)
            and not _is_bad_value(entry.get("display"))
            and entry.get("display") not in (PLACEHOLDER_TEXT, TE_DISPLAY)
            and entry.get("source") != TE_SOURCE)


def _entry_fresh(entry, now) -> bool:
    if not _entry_valid(entry):
        return False
    updated = _parse_iso_datetime(entry.get("last_updated"))
    if updated is None:
        return False
    return (now - updated) < timedelta(days=ECONOMIC_REFRESH_DAYS)


def _stamp(entry):
    if not isinstance(entry, dict):
        return None
    stamped = dict(entry)
    stamped["last_updated"] = _now_iso()
    return stamped


def _safe_call(label: str, func, *args):
    try:
        return func(*args)
    except Exception as e:
        log.warning(f"{label} 처리 예외, 다음 출처로 전환: {str(e)[:120]}")
        return None


def _pick_newer(first, second):
    first = first if _entry_valid(first) else None
    second = second if _entry_valid(second) else None
    if not first and not second:
        return None
    if first and not second:
        return first
    if second and not first:
        return second
    year_first = _year_from_value(first.get("year")) or 0
    year_second = _year_from_value(second.get("year")) or 0
    return second if year_second > year_first else first


# ---------------------------------------------------------------------------
# 1) 한국타이어 공식 거점 (검증 통과 시에만 캐싱)
# ---------------------------------------------------------------------------
COUNTRY_ALIASES = {
    "KR": ["korea", "south korea", "republic of korea", "한국", "대한민국"],
    "US": ["united states", "usa", "u.s.a", "america", "미국"],
    "GB": ["united kingdom", "uk", "u.k.", "great britain", "england", "영국"],
    "CZ": ["czech republic", "czechia", "체코"],
    "TR": ["turkey", "türkiye", "turkiye", "튀르키예", "터키"],
    "AE": ["united arab emirates", "uae", "아랍에미리트"],
    "VN": ["vietnam", "viet nam", "베트남"],
    "RU": ["russia", "russian federation", "러시아"],
    "SA": ["saudi arabia", "사우디아라비아", "사우디"],
    "TW": ["taiwan", "대만"],
    "CN": ["china", "중국"],
    "DE": ["germany", "독일"],
    "NL": ["netherlands", "the netherlands", "네덜란드"],
    "UA": ["ukraine", "우크라이나"],
    "KZ": ["kazakhstan", "카자흐스탄"],
    "EG": ["egypt", "이집트"],
}
REGION_HEADINGS = {
    "asia", "asia pacific", "europe", "americas", "north america", "south america", "latin america",
    "middle east", "africa", "middle east africa", "cis", "global", "headquarters",
    "아시아", "아시아태평양", "유럽", "미주", "북미", "중남미", "중동", "아프리카", "중아", "글로벌", "본사",
}
OFFICE_KEYWORD_RE = re.compile(
    r"(plant|factory|corp|co\.|ltd|gmbh|inc\b|llc|s\.a\.|s\.a\.r\.l|s\.r\.l|kft|sp\. ?z|a\.s\.|b\.v\.|\bab\b|pt\.|sdn|bhd|pty|"
    r"limited|headquarter|\bhq\b|technology center|tech center|technical center|liaison|research|r&d|proving ground|"
    r"technoring|technodome|sales|office|branch|agencia|법인|공장|본부|연구소|센터|지사|주행시험장|사무소|판매|영업|테크노)",
    re.IGNORECASE,
)
NOISE_LINE_RE = re.compile(
    r"(cookie|privacy|copyright|©|all rights reserved|로그인|검색|개인정보|이용약관|사이트맵|sitemap|login|search|"
    r"javascript|skip to|menu|뉴스룸|채용|고객센터|tel\b|fax\b|e-?mail|전화|팩스)",
    re.IGNORECASE,
)
ADDRESS_WORD_RE = re.compile(
    r"\b(street|st\.|ave|avenue|blvd|boulevard|road|rd\.|floor|fl\.|suite|bld|building|prospect|viale|via|calle|"
    r"strasse|straße|ul\.|business center|quay|drive|lane|plaza|tower|edificio|planta|oficina)\b",
    re.IGNORECASE,
)

ORG_COUNTRY_HINTS = [
    (r"technodome|테크노돔|technoring|테크노링|daejeon|대전|geumsan|금산|pangyo|판교|global headquarters|글로벌 본사|hankook & company", "KR"),
    (r"europe headquarters|european headquarters|europe technical center|european technical center|deutschland|hannover|neu-isenburg", "DE"),
    (r"tyre u\.?k|tire u\.?k|u\.k\. ltd|uk ltd", "GB"),
    (r"italia", "IT"),
    (r"espa[ñn]a", "ES"),
    (r"polska", "PL"),
    (r"\brus\b", "RU"),
    (r"lastik", "TR"),
    (r"hungary|magyar|budapest|dunaujvaros|dunaújváros", "HU"),
    (r"france", "FR"),
    (r"netherlands|benelux", "NL"),
    (r"sweden|sverige", "SE"),
    (r"austria", "AT"),
    (r"romania", "RO"),
    (r"ukraine", "UA"),
    (r"czech|ceska|česká", "CZ"),
    (r"croatia|hrvatska", "HR"),
    (r"serbia|srbija", "RS"),
    (r"morocco|maroc", "MA"),
    (r"america corp|akron|clarksville|tennessee plant|nashville", "US"),
    (r"canada", "CA"),
    (r"mexico|méxico", "MX"),
    (r"brasil|brazil", "BR"),
    (r"chile", "CL"),
    (r"colombia", "CO"),
    (r"panama|panamá", "PA"),
    (r"japan", "JP"),
    (r"indonesia", "ID"),
    (r"australia", "AU"),
    (r"malaysia", "MY"),
    (r"thailand", "TH"),
    (r"singapore", "SG"),
    (r"vietnam", "VN"),
    (r"taiwan", "TW"),
    (r"china|jiaxing|jiangsu|chongqing|shanghai|중국", "CN"),
    (r"dubai|middle east", "AE"),
    (r"jeddah|saudi", "SA"),
    (r"egypt", "EG"),
    (r"kazakhstan|almaty", "KZ"),
]
ORG_COUNTRY_HINTS = [(re.compile(p, re.IGNORECASE), c) for p, c in ORG_COUNTRY_HINTS]

CITY_KO = [
    ("seoul", "서울"), ("seongnam", "판교"), ("pangyo", "판교"), ("daejeon", "대전"), ("geumsan", "금산"), ("taean", "태안"),
    ("nashville", "내슈빌"), ("clarksville", "클락스빌"), ("uniontown", "유니언타운"), ("akron", "애크런"),
    ("neu-isenburg", "노이이젠부르크"), ("frankfurt", "프랑크푸르트"), ("hannover", "하노버"), ("hanover", "하노버"),
    ("daventry", "다번트리"), ("milton keynes", "밀턴킨스"), ("london", "런던"),
    ("sesto san giovanni", "밀라노"), ("milano", "밀라노"), ("milan", "밀라노"), ("rome", "로마"),
    ("san sebastian de los reyes", "마드리드"), ("madrid", "마드리드"), ("barcelona", "바르셀로나"),
    ("warszawa", "바르샤바"), ("warsaw", "바르샤바"), ("moscow", "모스크바"), ("kyiv", "키이우"), ("kiev", "키이우"),
    ("istanbul", "이스탄불"), ("budapest", "부다페스트"), ("dunaujvaros", "두나우이바로시"), ("racalmas", "라찰마시"),
    ("paris", "파리"), ("lyon", "리옹"), ("amsterdam", "암스테르담"), ("rotterdam", "로테르담"),
    ("stockholm", "스톡홀름"), ("vienna", "비엔나"), ("wien", "비엔나"), ("bucharest", "부쿠레슈티"), ("bucuresti", "부쿠레슈티"),
    ("prague", "프라하"), ("praha", "프라하"), ("belgrade", "베오그라드"), ("beograd", "베오그라드"), ("zagreb", "자그레브"),
    ("casablanca", "카사블랑카"), ("toronto", "토론토"), ("mississauga", "미시소거"), ("markham", "마컴"),
    ("mexico city", "멕시코시티"), ("ciudad de mexico", "멕시코시티"), ("sao paulo", "상파울루"),
    ("santiago", "산티아고"), ("bogota", "보고타"), ("panama city", "파나마시티"),
    ("jakarta", "자카르타"), ("cikarang", "찌카랑"), ("bekasi", "브카시"), ("sydney", "시드니"), ("melbourne", "멜버른"),
    ("tokyo", "도쿄"), ("osaka", "오사카"), ("singapore", "싱가포르"), ("kuala lumpur", "쿠알라룸푸르"),
    ("petaling jaya", "페탈링자야"), ("bangkok", "방콕"), ("ho chi minh", "호치민"), ("hanoi", "하노이"),
    ("taipei", "타이베이"), ("dubai", "두바이"), ("jeddah", "제다"), ("riyadh", "리야드"), ("cairo", "카이로"),
    ("almaty", "알마티"), ("shanghai", "상하이"), ("jiaxing", "자싱"), ("huai'an", "화이안"), ("huaian", "화이안"),
    ("chongqing", "충칭"), ("beijing", "베이징"), ("kunshan", "쿤산"),
]
KO_CITIES = ["서울", "판교", "대전", "금산", "태안", "성남"]


def _fold(text: str) -> str:
    try:
        t = unicodedata.normalize("NFKD", str(text or ""))
        t = "".join(ch for ch in t if not unicodedata.combining(ch))
        return t.lower()
    except Exception:
        return str(text or "").lower()


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
    low = _normalize_name(_fold(line))
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


def _hint_country(line: str):
    for rx, code in ORG_COUNTRY_HINTS:
        try:
            if rx.search(line or "") and code in COUNTRY_CODES:
                return code
        except Exception:
            continue
    return None


def _is_address_like(line: str) -> bool:
    try:
        t = line or ""
        digits = len(re.findall(r"\d", t))
        if digits >= 2 and ("," in t or len(t) > 30):
            return True
        if digits >= 1 and ADDRESS_WORD_RE.search(t):
            return True
        return False
    except Exception:
        return False


def _extract_city(text: str):
    try:
        folded = _fold(text)
        best, best_pos = None, -1
        for en, ko in CITY_KO:
            for m in re.finditer(r"(?<![a-z])" + re.escape(en) + r"(?![a-z])", folded):
                if m.start() > best_pos:
                    best_pos, best = m.start(), ko
        if best:
            return best
        for ko in KO_CITIES:
            if ko in str(text or ""):
                return "판교" if ko == "성남" else ko
    except Exception:
        pass
    return None


def _is_city_candidate(text: str, name_index: dict) -> bool:
    try:
        t = " ".join((text or "").split())
        if not t or len(t) > 40:
            return False
        if NOISE_LINE_RE.search(t) or OFFICE_KEYWORD_RE.search(t):
            return False
        if re.search(r"\d", t):
            return False
        norm = _normalize_name(t)
        if norm in REGION_HEADINGS or norm in name_index:
            return False
        return True
    except Exception:
        return False


def parse_hankook_global_network(html_text: str, countries: list) -> dict:
    result = {}
    try:
        from bs4 import BeautifulSoup
    except Exception as e:
        log.warning(f"[hankook] BeautifulSoup 불러오기 실패: {e}")
        return result

    try:
        soup = BeautifulSoup(html_text or "", "html.parser")
        for tag in soup(["script", "style", "noscript", "nav", "header", "footer"]):
            tag.decompose()
        lines = [" ".join(l.split()) for l in soup.get_text("\n").split("\n")]
        lines = [l for l in lines if l]
    except Exception as e:
        log.warning(f"[hankook] HTML 파싱 실패: {e}")
        return result

    name_index = _build_country_name_index(countries)
    current = None
    skip = set()

    for i, line in enumerate(lines):
        try:
            if i in skip:
                continue
            if len(line) > 160 or NOISE_LINE_RE.search(line):
                continue

            norm = _normalize_name(line)
            if norm in REGION_HEADINGS:
                current = None
                continue
            if norm in name_index:
                current = name_index[norm]
                continue
            if _is_address_like(line):
                continue
            if not OFFICE_KEYWORD_RE.search(line) or len(line) < 4:
                continue

            org = line
            city = None
            trailing = re.search(r"\(([^()]+)\)\s*$", org)
            if trailing:
                city = _extract_city(trailing.group(1)) or trailing.group(1).strip()
                org = org[:trailing.start()].strip()

            addr_country = None
            for j in range(i + 1, min(i + 4, len(lines))):
                nxt = lines[j]
                if _is_address_like(nxt):
                    if not city:
                        city = _extract_city(nxt)
                    if not addr_country:
                        addr_country = _detect_country_in_line(nxt, name_index)
                    skip.add(j)
                    continue
                if OFFICE_KEYWORD_RE.search(nxt) or _normalize_name(nxt) in name_index or _normalize_name(nxt) in REGION_HEADINGS:
                    break
                if not city and _is_city_candidate(nxt, name_index):
                    city = _extract_city(nxt) or nxt
                    skip.add(j)
                    continue
                break

            if not city:
                city = _extract_city(org)

            target = _hint_country(org) or _detect_country_in_line(org, name_index) or addr_country or current
            if not target or target not in COUNTRY_CODES:
                continue

            entry = f"{org} ({city})" if city else org
            bucket = result.setdefault(target, [])
            if entry not in bucket and len(bucket) < 12:
                bucket.append(entry)
        except Exception:
            continue

    return {code: names for code, names in result.items() if names}


def _validate_offices(offices) -> tuple:
    try:
        if not isinstance(offices, dict) or not offices:
            return False, "결과 없음"
        if len(offices) < HANKOOK_MIN_COUNTRIES:
            return False, f"국가 수 부족({len(offices)}개국)"
        if "KR" not in offices:
            return False, "한국(KR) 거점 누락"
        for code, names in offices.items():
            if code not in COUNTRY_CODES:
                return False, f"알 수 없는 국가 코드({code})"
            if not isinstance(names, list) or not names:
                return False, f"{code} 거점 목록이 비어 있음"
            for n in names:
                if _is_bad_value(n):
                    return False, f"{code} 거점에 빈 항목"
                base = re.sub(r"\([^()]*\)\s*$", "", str(n)).strip()
                if _is_address_like(base):
                    return False, f"{code} 거점에 주소 형식 항목 포함"
        return True, ""
    except Exception as e:
        return False, str(e)[:80]


def fetch_hankook_offices(countries: list) -> dict:
    cached = _read_json_file(HANKOOK_CACHE_PATH)
    if isinstance(cached, dict) and cached.get("schema_version") == HANKOOK_CACHE_VERSION:
        offices = cached.get("offices")
        ok, reason = _validate_offices(offices)
        if ok:
            log.info("[hankook] data/hankook_offices_cache.json 사용 (재크롤링 없음)")
            return {k: v for k, v in offices.items() if isinstance(v, list) and v}
        log.warning(f"[hankook] 캐시 검증 실패 → 재크롤링 ({reason})")
    elif cached is not None:
        log.warning("[hankook] 구버전 캐시 감지 → 재크롤링")

    log.info("[hankook] 공식 글로벌 네트워크 페이지 크롤링 시작")
    reason = ""
    for url in HANKOOK_NETWORK_URLS:
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=API_TIMEOUT)
            _polite_sleep()
            if resp.status_code != 200:
                reason = f"HTTP {resp.status_code}"
                continue
            resp.encoding = resp.apparent_encoding or "utf-8"
            parsed = parse_hankook_global_network(resp.text, countries)
            ok, why = _validate_offices(parsed)
            if ok:
                _write_json_file(HANKOOK_CACHE_PATH, {
                    "schema_version": HANKOOK_CACHE_VERSION,
                    "source_url": url,
                    "fetched_at": _now_iso(),
                    "status": "ok",
                    "offices": parsed,
                })
                log.info(f"[hankook] 거점 {len(parsed)}개국 수집·캐싱 완료 ({url})")
                return parsed
            reason = f"검증 실패: {why}"
        except Exception as e:
            reason = str(e)[:120]

    log.warning(f"[hankook] 공식 페이지 파싱 실패 → '{OFFICE_PLACEHOLDER}' 문구 사용, 캐시 저장 안 함 ({reason})")
    return {}


# ---------------------------------------------------------------------------
# 2) 경제지표 수집기
# ---------------------------------------------------------------------------
def _fetch_imf_weo(code: str, key: str):
    source = "IMF WEO"
    if _source_disabled(source):
        return None
    iso3 = ISO3_MAP.get(code)
    indicator = {"gdp": "NGDPD", "inflation": "PCPIPCH"}.get(key)
    if not iso3 or not indicator:
        return None
    this_year = datetime.now(KST).year
    try:
        url = f"https://www.imf.org/external/datamapper/api/v1/{indicator}/{iso3}"
        status, payload, _ = _api_get_json(source, url)
        if status != 200 or not isinstance(payload, dict):
            return None
        series = (((payload.get("values") or {}).get(indicator) or {}).get(iso3)) or {}
        best_year, best_value = None, None
        for y, v in series.items():
            yi = _year_from_value(y)
            fv = _to_float(v)
            if yi is None or fv is None or yi >= this_year:
                continue
            if best_year is None or yi > best_year:
                best_year, best_value = yi, fv
        if best_year is None:
            return None
        return _make_entry(key, best_value, source, best_year)
    except Exception as e:
        log.warning(f"[{code}] IMF WEO {indicator} 처리 실패: {e}")
        return None


def _fetch_world_bank(code: str, keys: list) -> dict:
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
    for key in keys:
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


def _parse_sdmx_json(payload) -> list:
    rows = []
    try:
        if not isinstance(payload, dict):
            return rows
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        datasets = data.get("dataSets")
        structure = data.get("structure")
        if structure is None:
            structures = data.get("structures")
            structure = structures[0] if isinstance(structures, list) and structures else None
        if not isinstance(datasets, list) or not datasets or not isinstance(structure, dict):
            return rows

        dims_struct = structure.get("dimensions") or {}
        series_dims = dims_struct.get("series") or []
        obs_dims = dims_struct.get("observation") or []
        dataset = datasets[0] if isinstance(datasets[0], dict) else {}

        items = []
        if isinstance(dataset.get("observations"), dict) and dataset.get("observations"):
            all_dims = list(obs_dims)
            items = list(dataset["observations"].items())
        elif isinstance(dataset.get("series"), dict):
            all_dims = list(series_dims) + list(obs_dims)
            for skey, sval in dataset["series"].items():
                for okey, arr in ((sval or {}).get("observations") or {}).items():
                    items.append((f"{skey}:{okey}", arr))
        else:
            return rows

        for key, arr in items:
            idxs = str(key).split(":")
            if len(idxs) != len(all_dims):
                continue
            row, ok = {}, True
            for dim, ix in zip(all_dims, idxs):
                values = dim.get("values") or []
                i = int(ix)
                if i < 0 or i >= len(values):
                    ok = False
                    break
                row[str(dim.get("id"))] = str(values[i].get("id"))
            if not ok:
                continue
            value = _to_float(arr[0] if isinstance(arr, list) and arr else None)
            if value is None:
                continue
            row["value"] = value
            rows.append(row)
    except Exception as e:
        log.warning(f"[sdmx] 응답 파싱 실패: {str(e)[:120]}")
    return rows


def _period_key(period: str) -> tuple:
    try:
        p = str(period or "")
        year = _year_from_value(p) or 0
        m = re.search(r"Q([1-4])", p)
        if m:
            return year, int(m.group(1)) * 3
        m = re.search(r"M(\d{1,2})", p)
        if m:
            return year, int(m.group(1))
        m = re.search(r"-(\d{2})$", p)
        if m:
            return year, int(m.group(1))
        return year, 12
    except Exception:
        return 0, 0


def _fmt_amount(value: float) -> str:
    try:
        v = float(value)
        return f"{v:,.0f}" if v >= 1000 else f"{v:,.2f}"
    except Exception:
        return str(value)


_ILO_STATE = {"dots": None, "empty_streak": 0}


def _ilo_row_ok(row: dict) -> bool:
    if "SEX" in row and row["SEX"] != "SEX_T":
        return False
    ages = [v for k, v in row.items() if k in ("CLASSIF1", "AGE")]
    if ages and not any("YGE15" in a for a in ages):
        return False
    return 0 <= row.get("value", -1) <= 100


def _fetch_ilostat_unemployment(code: str):
    source = "ILOSTAT"
    if _source_disabled(source):
        return None
    iso3 = ISO3_MAP.get(code)
    if not iso3:
        return None

    candidates = [_ILO_STATE["dots"]] if _ILO_STATE["dots"] is not None else [4, 5, 3]
    for dots in candidates:
        url = (f"https://sdmx.ilo.org/rest/data/ILO,DF_UNE_DEAP_SEX_AGE_RT/{iso3}{'.' * dots}"
               f"?format=jsondata&lastNObservations=1&dimensionAtObservation=AllDimensions")
        status, payload, _ = _api_get_json(source, url)
        if status != 200 or payload is None:
            continue
        rows = [r for r in _parse_sdmx_json(payload) if _ilo_row_ok(r)]
        if not rows:
            continue
        best = max(rows, key=lambda r: _period_key(r.get("TIME_PERIOD")))
        entry = _make_entry("unemployment", best["value"], source, _year_from_value(best.get("TIME_PERIOD")))
        if entry:
            _ILO_STATE["dots"] = dots
            _ILO_STATE["empty_streak"] = 0
            return entry

    _ILO_STATE["empty_streak"] += 1
    if _ILO_STATE["empty_streak"] >= 6 and _ILO_STATE["dots"] is None:
        _disable_source(source, "응답 형식을 확인하지 못해 World Bank 로 대체")
    return None


# ---- 최저임금: OECD vs 수동 보완 중 최신 적용연도 채택 -------------------------
_MINWAGE_MANUAL = None


def load_minwage_manual() -> dict:
    global _MINWAGE_MANUAL
    if _MINWAGE_MANUAL is not None:
        return _MINWAGE_MANUAL

    table = {}
    stored = _read_json_file(MINWAGE_MANUAL_PATH)
    stored_rows = stored.get("countries") if isinstance(stored, dict) and isinstance(stored.get("countries"), dict) else {}
    changed = not stored_rows

    for code, row in stored_rows.items():
        norm = _normalize_code(code)
        if norm != str(code).upper():
            changed = True
        if isinstance(row, dict) and not _is_bad_value(row.get("display")):
            table[norm] = row

    for code, row in MIN_WAGE_MANUAL_DEFAULT.items():
        if code not in table:
            table[code] = dict(row)
            changed = True

    if changed:
        _write_json_file(MINWAGE_MANUAL_PATH, {
            "_comment": "OECD 에 없거나 OECD 값이 오래된 국가의 최저임금 수동 보완 테이블. display/note/year/basis 를 직접 수정할 수 있습니다. 국가 코드는 ISO alpha-2(영국=GB)를 사용합니다.",
            "countries": table,
        })

    _MINWAGE_MANUAL = table
    return _MINWAGE_MANUAL


def _manual_minwage_entry(code: str):
    row = load_minwage_manual().get(code)
    if not isinstance(row, dict) or _is_bad_value(row.get("display")):
        return None
    note = "" if _is_bad_value(row.get("note")) else str(row.get("note"))
    basis = row.get("basis")
    if row.get("display") != "-" and not _is_bad_value(basis) and basis != "수동 보완":
        note = f"{note} · 기준: {basis}" if note else f"기준: {basis}"
    year = row.get("year")
    return {"display": str(row["display"]), "note": note, "source": "수동 보완",
            "year": year if year not in (None, "") else "-", "last_updated": _now_iso()}


def _fetch_oecd_min_wage(code: str):
    source = "OECD"
    if _source_disabled(source):
        return None
    iso3 = ISO3_MAP.get(code)
    if not iso3:
        return None

    url = ("https://sdmx.oecd.org/public/rest/data/OECD.ELS.SAE,DSD_EARNINGS@MW_CURP,1.0/"
           f"{iso3}......?format=jsondata&lastNObservations=1&dimensionAtObservation=AllDimensions")
    status, payload, _ = _api_get_json(source, url)
    if status != 200 or payload is None:
        return None

    rows = [r for r in _parse_sdmx_json(payload) if r.get("PRICE_BASE", "V") == "V" and r.get("value", 0) > 0]
    if not rows:
        return None

    pay_rank = {"H": 0, "M": 1, "D": 2, "W": 3, "A": 4}
    best = sorted(
        rows,
        key=lambda r: (-_period_key(r.get("TIME_PERIOD"))[0], pay_rank.get(r.get("PAY_PERIOD"), 9)),
    )[0]
    pay = best.get("PAY_PERIOD", "M")
    currency = best.get("UNIT_MEASURE", "")
    label = CURRENCY_KO.get(currency, currency)
    pay_label = PAY_PERIOD_KO.get(pay, "월")
    return {
        "display": f"{_fmt_amount(best['value'])}{label} / {pay_label}",
        "note": f"법정 최저임금 ({pay_label} 기준, 현지통화)",
        "source": source,
        "year": _year_from_value(best.get("TIME_PERIOD")),
        "last_updated": _now_iso(),
    }


def select_min_wage(code: str, oecd_entry):
    manual = _manual_minwage_entry(code)
    if manual and manual["display"] == "-":
        return manual

    candidates = [e for e in (oecd_entry, manual) if _entry_valid(e)]
    if not candidates:
        return None

    chosen = dict(max(
        candidates,
        key=lambda e: (_year_from_value(e.get("year")) or 0, 1 if e.get("source") == "OECD" else 0),
    ))
    this_year = datetime.now(KST).year
    year = _year_from_value(chosen.get("year"))
    if year and year < this_year and chosen.get("display") not in ("-", "업종·지역별 상이"):
        suffix = f"{year}년 적용분 (당해년도 고시 미반영)"
        note = chosen.get("note") or ""
        if suffix not in note:
            chosen["note"] = f"{note} · {suffix}" if note else suffix
    return chosen


def build_min_wage(code: str) -> dict:
    entry = _safe_call(f"[{code}] 수동 최저임금", select_min_wage, code, None)
    if entry:
        return entry
    return {"display": PLACEHOLDER_TEXT, "note": "", "source": "-", "year": "-", "last_updated": ""}


# ---- 항목별 수집 -------------------------------------------------------------
def collect_economic_items(country: dict, need: list) -> dict:
    code = country["code"]
    found = {}
    this_year = datetime.now(KST).year
    recent_year = this_year - 1

    wb_cache = {}

    def wb(key):
        if key not in wb_cache:
            wb_cache[key] = (_safe_call(f"[{code}] World Bank {key}", _fetch_world_bank, code, [key]) or {}).get(key)
        return wb_cache[key]

    if "population" in need:
        entry = wb("population")
        if entry:
            found["population"] = _stamp(entry)

    if "gdp" in need:
        entry = _safe_call(f"[{code}] IMF WEO GDP", _fetch_imf_weo, code, "gdp")
        if not _entry_valid(entry):
            entry = wb("gdp")
        if _entry_valid(entry):
            found["gdp"] = _stamp(entry)

    if "inflation" in need:
        imf = _safe_call(f"[{code}] IMF WEO 물가", _fetch_imf_weo, code, "inflation")
        entry = imf if _entry_valid(imf) else None
        if not entry or (_year_from_value(entry.get("year")) or 0) < recent_year:
            entry = _pick_newer(entry, wb("inflation"))
        if entry:
            found["inflation"] = _stamp(entry)

    if "unemployment" in need:
        ilo = _safe_call(f"[{code}] ILOSTAT 실업률", _fetch_ilostat_unemployment, code)
        entry = ilo if _entry_valid(ilo) else None
        if not entry or (_year_from_value(entry.get("year")) or 0) < recent_year:
            entry = _pick_newer(entry, wb("unemployment"))
        if entry:
            found["unemployment"] = _stamp(entry)

    if "min_wage" in need:
        oecd = _safe_call(f"[{code}] OECD 최저임금", _fetch_oecd_min_wage, code)
        entry = _safe_call(f"[{code}] 최저임금 선택", select_min_wage, code, oecd)
        if entry:
            found["min_wage"] = entry

    return found


def load_economic_data(countries: list) -> tuple:
    now = datetime.now(KST)

    economic_cache = _read_json_file(ECONOMIC_CACHE_PATH)
    raw_stored = economic_cache.get("countries") if isinstance(economic_cache, dict) and isinstance(economic_cache.get("countries"), dict) else {}
    version_ok = isinstance(economic_cache, dict) and economic_cache.get("cache_version") == ECONOMIC_CACHE_VERSION
    if economic_cache is not None and not version_ok:
        log.info("[economic] 구버전 경제지표 캐시 감지 → 전 항목 재수집 (실패 항목은 이전 값 유지)")

    stored = raw_stored if version_ok else {}
    legacy = raw_stored if not version_ok else {}

    result = {}
    economic_changed = not version_ok
    api_countries = 0

    for country in countries:
        code = country["code"]
        items = {}
        try:
            old = dict(stored.get(code)) if isinstance(stored.get(code), dict) else {}
            legacy_old = legacy.get(code) if isinstance(legacy.get(code), dict) else {}
            need = []
            for key in ECONOMIC_ITEMS:
                entry = old.get(key)
                if _entry_fresh(entry, now):
                    if key == "min_wage":
                        oecd_part = entry if entry.get("source") == "OECD" else None
                        refreshed = select_min_wage(code, oecd_part) or entry
                        if (refreshed.get("display") != entry.get("display")
                                or refreshed.get("year") != entry.get("year")
                                or refreshed.get("note") != entry.get("note")):
                            if entry.get("source") == "OECD" and refreshed.get("source") != "OECD":
                                refreshed = dict(refreshed)
                            economic_changed = True
                        items[key] = refreshed
                    else:
                        items[key] = entry
                else:
                    need.append(key)

            if need:
                api_countries += 1
                fetched = collect_economic_items(country, need)
                for key in need:
                    if key in fetched and _entry_valid(fetched[key]):
                        items[key] = fetched[key]
                        old[key] = fetched[key]
                        economic_changed = True
                    elif _entry_valid(old.get(key)):
                        items[key] = old[key]
                    elif _entry_valid(legacy_old.get(key)):
                        items[key] = legacy_old[key]
            stored[code] = old
        except Exception as e:
            log.warning(f"[{code}] 경제지표 처리 예외: {e}")
        result[code] = items

    if economic_changed or not os.path.exists(ECONOMIC_CACHE_PATH):
        if any(stored.get(c["code"]) for c in countries):
            _write_json_file(ECONOMIC_CACHE_PATH, {
                "cache_version": ECONOMIC_CACHE_VERSION,
                "updated_at": now.isoformat(),
                "refresh_days": ECONOMIC_REFRESH_DAYS,
                "countries": stored,
            })

    latest_dates = []
    for items in result.values():
        for entry in items.values():
            dt = _parse_iso_datetime(entry.get("last_updated")) if isinstance(entry, dict) else None
            if dt:
                latest_dates.append(dt)
    asof = max(latest_dates).strftime("%Y-%m-%d") if latest_dates else now.strftime("%Y-%m-%d")

    log.info(f"[economic] 완료: 항목 갱신이 필요했던 국가 {api_countries}/{len(countries)}개 (나머지는 캐시 재사용)")
    return result, asof


# ---------------------------------------------------------------------------
# 프로필 구성
# ---------------------------------------------------------------------------
def _cached_indicator(cached_indicators: dict, key: str):
    old = cached_indicators.get(key) if isinstance(cached_indicators, dict) else None
    if isinstance(old, dict) and old.get("source") in API_SOURCES and _entry_valid(old):
        return dict(old)
    return None


def build_profile(country: dict, cached: dict, shared: dict = None) -> dict:
    shared = shared or {}
    code = country.get("code", "")
    cached_profile = (cached or {}).get("profile", {}) or {}
    cached_indicators = cached_profile.get("indicators", {}) if isinstance(cached_profile.get("indicators"), dict) else {}
    econ = (shared.get("economic") or {}).get(code, {}) or {}

    indicators = {"capital": _capital_entry(country)}
    for key in ("population", "gdp", "inflation", "unemployment"):
        entry = None
        try:
            api_entry = econ.get(key)
            if isinstance(api_entry, dict) and api_entry.get("source") in API_SOURCES and _entry_valid(api_entry):
                entry = dict(api_entry)
            if entry is None:
                entry = _cached_indicator(cached_indicators, key)
        except Exception as e:
            log.warning(f"[{code}] '{key}' 지표 구성 실패: {e}")
            entry = None
        if entry is None:
            entry = _te_entry(country, key) if key in ("inflation", "unemployment") else _placeholder_entry()
        indicators[key] = entry

    min_wage = econ.get("min_wage")
    if not (isinstance(min_wage, dict) and not _is_bad_value(min_wage.get("display"))):
        min_wage = build_min_wage(code)
    min_wage = {
        "display": min_wage.get("display", PLACEHOLDER_TEXT),
        "note": min_wage.get("note", ""),
        "source": min_wage.get("source", "-"),
        "year": min_wage.get("year", "-"),
        "last_updated": min_wage.get("last_updated", ""),
    }

    sources, years = [], []
    for entry in list(indicators.values()) + [min_wage]:
        src = entry.get("source")
        if src and src not in ("-", CAPITAL_SOURCE) and src not in sources:
            sources.append(src)
        y = _year_from_value(entry.get("year"))
        if y:
            years.append(y)

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
        "stats_asof": shared.get("economic_asof") or "-",
    }


# ---------------------------------------------------------------------------
# 환율
# ---------------------------------------------------------------------------
MIN_RELIABLE_TRADING_DAYS = 30


def _yf_daily_close(ticker: str, days: int = 400) -> pd.Series:
    try:
        import yfinance as yf
        try:
            hist = yf.Ticker(ticker).history(period=f"{days}d", interval="1d", timeout=API_TIMEOUT)
        except TypeError:
            hist = yf.Ticker(ticker).history(period=f"{days}d", interval="1d")
        if hist is None or hist.empty:
            return pd.Series(dtype="float64")
        return hist["Close"].dropna()
    except Exception as e:
        log.warning(f"[yfinance] {ticker} 조회 실패: {e}")
        return pd.Series(dtype="float64")


def _monthly_from_daily(daily: pd.Series, months: int = 12):
    if daily.empty:
        return [], []
    try:
        idx = daily.index
        try:
            if getattr(idx, "tz", None) is not None:
                daily = daily.copy()
                daily.index = idx.tz_localize(None)
        except Exception:
            pass
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
    m = now.month
    for _ in range(months):
        labels.append(MONTH_KR[m - 1])
        m -= 1
        if m == 0:
            m = 12
    return list(reversed(labels))


def _synthetic_monthly_series(currency: str, unit_base: int, months: int = 12) -> tuple:
    base_rate = REFERENCE_RATE_PER_UNIT.get(currency, 1000.0) * unit_base
    rng = random.Random(f"{currency}-{datetime.now(KST).strftime('%Y-%m-%d')}")
    values = [0.0] * months
    values[-1] = base_rate
    for i in range(months - 2, -1, -1):
        values[i] = values[i + 1] / (1 + rng.uniform(-1.5, 1.5) / 100.0)
    return _recent_month_labels(months), [_round_won(v) for v in values]


def _rescale_fx(fx: dict, new_unit_base: int) -> dict:
    out = dict(fx or {})
    try:
        old_ub = out.get("unit_base") if isinstance(out.get("unit_base"), int) and out.get("unit_base") > 0 else 1
        factor = new_unit_base / old_ub
        current = _to_float(out.get("current_rate")) or 0.0
        values = out.get("history_values") if isinstance(out.get("history_values"), list) else []
        out["current_rate"] = _round_won(current * factor)
        out["history_values"] = [_round_won((_to_float(v) or 0.0) * factor) for v in values]
        out["unit_base"] = new_unit_base
        if _to_float(out.get("rate_per_unit")) is None:
            out["rate_per_unit"] = round(current / old_ub, 6) if current else 0.0
    except Exception:
        out["unit_base"] = new_unit_base
        out.setdefault("rate_per_unit", 0.0)
    return out


def get_exchange_rate(country: dict, cached: dict) -> dict:
    currency = country["currency"]
    cached_fx = (cached or {}).get("exchange_rate", {})
    unit_base = SMALL_UNIT_CURRENCIES.get(currency, 1)
    yahoo_url = _yahoo_finance_url(currency)

    if currency == "KRW":
        return {"is_base": True, "unit_base": 1, "current_rate": 1.0, "rate_per_unit": 1.0, "change_pct": 0.0,
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

        current_raw = float(daily.iloc[-1])
        prev_raw = float(daily.iloc[-2]) if len(daily) >= 2 else current_raw
        change_pct = round((current_raw - prev_raw) / prev_raw * 100, 2) if prev_raw else 0.0
        history_labels, history_raw = _monthly_from_daily(daily, months=12)
        if not history_labels:
            raise ValueError("월별 히스토리 생성 실패")

        return {
            "is_base": False,
            "unit_base": unit_base,
            "current_rate": _round_won(current_raw * unit_base),
            "rate_per_unit": round(current_raw, 6),
            "change_pct": change_pct,
            "history_labels": history_labels,
            "history_values": [_round_won(v * unit_base) for v in history_raw],
            "source": source,
            "url": yahoo_url,
        }
    except Exception as e:
        log.warning(f"[{country['code']}] 환율 수집 실패 → 캐시/참고 시계열로 대체: {e}")
        if cached_fx and cached_fx.get("history_values"):
            fx = _rescale_fx(cached_fx, unit_base)
            fx["url"] = yahoo_url
            fx["is_base"] = False
            return fx

        history_labels, history_values = _synthetic_monthly_series(currency, unit_base, months=12)
        current_rate = history_values[-1] if history_values else 0.0
        prev_rate = history_values[-2] if len(history_values) >= 2 else current_rate
        change_pct = round((current_rate - prev_rate) / prev_rate * 100, 2) if prev_rate else 0.0
        return {
            "is_base": False,
            "unit_base": unit_base,
            "current_rate": current_rate,
            "rate_per_unit": round(current_rate / unit_base, 6) if unit_base else current_rate,
            "change_pct": change_pct,
            "history_labels": history_labels,
            "history_values": history_values,
            "source": "수집 실패 (참고용 추정치)",
            "url": yahoo_url,
        }


# ---------------------------------------------------------------------------
# 언어 감지 / 번역 (메인 스레드에서 순차 실행)
# ---------------------------------------------------------------------------
_TRANSLATION_CACHE = {}
_TRANSLATE_STATE = {"fails": 0, "disabled": False, "ok": 0, "failed": 0}


def detect_language(text: str) -> str:
    try:
        t = (text or "").strip()
        if not t:
            return "en"
        if len(re.findall(r"[가-힣]", t)) >= 2:
            return "ko"
        try:
            from langdetect import detect_langs, DetectorFactory
            DetectorFactory.seed = 0
            langs = detect_langs(t)
            if langs:
                top = langs[0]
                code = str(top.lang).lower()
                if code.startswith("zh"):
                    code = "zh"
                code = code.split("-")[0]
                if t.isascii() and code != "en" and float(top.prob) < 0.95:
                    return "en"
                return code or "en"
        except Exception:
            pass
        if re.search(r"[\u3040-\u30ff]", t):
            return "ja"
        if re.search(r"[\u4e00-\u9fff]", t):
            return "zh"
        if re.search(r"[\u0400-\u04ff]", t):
            return "ru"
        if re.search(r"[\u0600-\u06ff]", t):
            return "ar"
        if re.search(r"[\u0e00-\u0e7f]", t):
            return "th"
        return "en"
    except Exception:
        return "en"


def translate_to_ko(text: str):
    original = text if isinstance(text, str) else ("" if text is None else str(text))
    if not original.strip():
        return None
    if original in _TRANSLATION_CACHE:
        return _TRANSLATION_CACHE[original]
    if _TRANSLATE_STATE["disabled"]:
        return None

    try:
        from deep_translator import GoogleTranslator
    except Exception as e:
        _TRANSLATE_STATE["disabled"] = True
        log.warning(f"[translate] deep-translator 불러오기 실패, 번역 생략: {e}")
        return None

    for attempt in range(2):
        try:
            out = GoogleTranslator(source="auto", target="ko").translate(original[:4500])
            time.sleep(TRANSLATE_DELAY)
            if out and str(out).strip():
                result = str(out).strip()
                _TRANSLATION_CACHE[original] = result
                _TRANSLATE_STATE["fails"] = 0
                _TRANSLATE_STATE["ok"] += 1
                return result
        except Exception as e:
            if attempt == 0:
                log.warning(f"[translate] 번역 실패, 재시도: {str(e)[:100]}")
            time.sleep(TRANSLATE_RETRY_BACKOFF)

    _TRANSLATE_STATE["fails"] += 1
    _TRANSLATE_STATE["failed"] += 1
    if _TRANSLATE_STATE["fails"] >= TRANSLATE_MAX_CONSECUTIVE_FAILS:
        _TRANSLATE_STATE["disabled"] = True
        log.warning(f"[translate] 연속 {_TRANSLATE_STATE['fails']}회 실패 → 이번 실행의 남은 기사는 원문 유지")
    return None


def _translate_label(lang: str, is_translated: bool) -> str:
    if not is_translated:
        return ""
    return f"{str(lang or 'en').split('-')[0].upper()} > KR 번역"


def _localize_item(item: dict) -> dict:
    original_title = item.get("original_title") or item.get("title", "") or ""
    original_summary = item.get("summary", "") or ""
    title, summary = original_title, original_summary
    lang, is_translated = "en", False
    try:
        lang = detect_language(original_title)
        if lang != "ko":
            translated_title = translate_to_ko(original_title)
            if translated_title and translated_title != original_title:
                title = translated_title
                is_translated = True
                if original_summary:
                    translated_summary = translate_to_ko(original_summary)
                    if translated_summary:
                        summary = translated_summary
    except Exception as e:
        log.warning(f"기사 언어 처리 실패, 원문 그대로 사용: {e}")
        title, summary, is_translated = original_title, original_summary, False

    item["title"] = title
    item["summary"] = summary
    item["desc"] = summary
    item["original_title"] = original_title
    item["original_lang"] = lang
    item["original_lang_upper"] = lang.upper()
    item["is_translated"] = is_translated
    item["translate_label"] = _translate_label(lang, is_translated)
    item["lang"] = lang
    item["translated"] = is_translated
    return item


# ---------------------------------------------------------------------------
# 뉴스 / 동향
# ---------------------------------------------------------------------------
NEWS_TOPICS = [
    {
        "category": "HR & LABOR", "tag": "HR·노동", "tag_class": "bg-indigo-100 text-indigo-700",
        "query_kr_tpl": "{name_kr} 노동법 임금 고용",
        "query_en_tpl": "{name_en} labor law employment wage",
        "max_items": 4,
    },
    {
        "category": "SOCIETY & CULTURE", "tag": "사회·문화", "tag_class": "bg-emerald-100 text-emerald-700",
        "query_kr_tpl": "{name_kr} 사회 문화 이슈",
        "query_en_tpl": "{name_en} society culture social issue",
        "max_items": 4,
    },
    {
        "category": "POLITICS & ECONOMY", "tag": "정치·경제", "tag_class": "bg-sky-100 text-sky-700",
        "query_kr_tpl": "{name_kr} 정치 경제 정책",
        "query_en_tpl": "{name_en} politics economy policy",
        "max_items": 4,
    },
]
AUTO_TOPICS = [
    {
        "category": "AUTO MARKET", "tag": "완성차·타이어 시장", "tag_class": "bg-rose-100 text-rose-700",
        "query_kr_tpl": "{name_kr} 완성차 타이어 시장",
        "query_en_tpl": "{name_en} automotive tire market",
        "max_items": 4,
    },
]


def _google_news_url_ko(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"


def _google_news_url_en(query: str) -> str:
    return f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"


def _google_news_search_link(query: str, korean: bool = False) -> str:
    try:
        if korean:
            return f"https://news.google.com/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
        return f"https://news.google.com/search?q={requests.utils.quote(query)}&hl=en-US&gl=US&ceid=US:en"
    except Exception:
        return "https://news.google.com/"


def _article(title: str, source: str, link: str, summary: str = "", lang: str = "ko",
             is_translated: bool = False, original_title: str = "") -> dict:
    return {
        "title": title,
        "desc": summary,
        "summary": summary,
        "source": source,
        "link": link,
        "original_title": original_title,
        "original_lang": lang,
        "original_lang_upper": str(lang).upper(),
        "is_translated": is_translated,
        "translate_label": _translate_label(lang, is_translated),
        "lang": lang,
        "translated": is_translated,
    }


def _clean_summary(raw_summary: str, title: str, source: str) -> str:
    try:
        s = _clean_text(raw_summary)
        if title and s.startswith(title):
            s = s[len(title):]
        if source:
            s = s.replace(source, " ")
        s = " ".join(s.split()).strip(" -·|")
        if len(s) < 15 or s == title:
            return ""
        return _shorten(s, 140)
    except Exception:
        return ""


def _parse_entry(entry) -> dict:
    title_raw = _clean_text(getattr(entry, "title", "") or "")
    link = (getattr(entry, "link", "") or "").strip()
    if not title_raw or not link:
        raise ValueError("title 또는 link 누락")

    source = ""
    try:
        if hasattr(entry, "source"):
            source = _clean_text(getattr(entry.source, "title", "") or "")
    except Exception:
        source = ""
    if not source and " - " in title_raw:
        source = title_raw.rsplit(" - ", 1)[-1].strip()

    title = title_raw.rsplit(" - ", 1)[0].strip() if " - " in title_raw else title_raw
    summary = _clean_summary(getattr(entry, "summary", ""), title, source)
    item = _article(title, source or "Google News", link, summary, lang="", original_title=title)
    item["_raw"] = True
    return item


def _fetch_feed(url: str, limit: int) -> list:
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=API_TIMEOUT)
        if resp.status_code != 200:
            log.warning(f"피드 HTTP {resp.status_code}")
            return []
        feed = feedparser.parse(resp.content)
    except Exception as e:
        log.warning(f"피드 요청/파싱 실패: {str(e)[:120]}")
        return []
    finally:
        _polite_sleep()

    items = []
    try:
        entries = getattr(feed, "entries", []) or []
    except Exception:
        return []

    for entry in entries[: max(limit * 4, limit)]:
        try:
            items.append(_parse_entry(entry))
        except Exception:
            continue
        if len(items) >= limit:
            break
    return items


def _dedup_key(item: dict) -> str:
    try:
        title = re.sub(r"\s+", " ", (item.get("original_title") or item.get("title") or "").strip().lower())
        if title:
            return title
        return (item.get("link") or "").strip().lower()
    except Exception:
        return ""


def _fallback_item(spec: dict, country: dict) -> dict:
    try:
        is_kr = country.get("code") == "KR"
        tpl = spec["query_kr_tpl"] if is_kr else spec["query_en_tpl"]
        query = tpl.format(name_kr=country.get("name_kr", ""), name_en=country.get("name_en", ""))
        link = _google_news_search_link(query, korean=is_kr)
    except Exception:
        link = "https://news.google.com/"
    item = _article(
        FALLBACK_TEXT, "Google News 검색", link,
        f"{spec.get('tag', '')} 관련 최신 기사를 찾지 못해 Google 뉴스 검색 결과로 연결됩니다. 다음 갱신 시 자동으로 업데이트됩니다.",
    )
    item["is_fallback"] = True
    return item


def _fallback_headline_item(country: dict) -> dict:
    name_kr = country.get("name_kr", "")
    item = _article(
        FALLBACK_TEXT, "Google News 검색", _google_news_search_link(country.get("name_en", "")),
        f"{name_kr} 관련 최신 기사를 찾지 못해 Google 뉴스 검색 결과로 연결됩니다." if name_kr else "관련 최신 기사를 찾지 못해 Google 뉴스 검색 결과로 연결됩니다.",
    )
    item["is_fallback"] = True
    return item


def _normalize_existing_item(old: dict):
    if not isinstance(old, dict):
        return None
    title = old.get("title")
    link = old.get("link")
    if _is_bad_value(title) or _is_bad_value(link):
        return None
    is_translated = old.get("is_translated", old.get("translated"))
    is_translated = is_translated if isinstance(is_translated, bool) else False
    lang = old.get("original_lang", old.get("lang"))
    lang = "ko" if _is_bad_value(lang) else str(lang).lower().split("-")[0]
    summary = old.get("summary", old.get("desc"))
    summary = "" if _is_bad_value(summary) else _clean_text(summary)
    if summary == str(title).strip() or summary.startswith(str(title).strip() + " "):
        summary = ""
    original_title = old.get("original_title")
    original_title = "" if _is_bad_value(original_title) else str(original_title)
    item = _article(
        str(title), "Google News" if _is_bad_value(old.get("source")) else str(old.get("source")),
        str(link), summary, lang=lang, is_translated=is_translated, original_title=original_title,
    )
    if str(title) == FALLBACK_TEXT:
        item["is_fallback"] = True
    return item


def _find_cached_category(cached: dict, category: str) -> list:
    try:
        for group in ("news_categories", "hr_trends"):
            for t in (cached or {}).get(group, []) or []:
                if not isinstance(t, dict) or t.get("category") != category:
                    continue
                items = t.get("items") if isinstance(t.get("items"), list) else []
                if not items and not _is_bad_value(t.get("title")):
                    items = [t]
                normalized = [n for n in (_normalize_existing_item(i) for i in items) if n and not n.get("is_fallback")]
                if normalized:
                    return normalized
        if category == "POLITICS & ECONOMY":
            normalized = [n for n in (_normalize_existing_item(i) for i in (cached or {}).get("headlines", []) or [])
                          if n and not n.get("is_fallback")]
            if normalized:
                return normalized
    except Exception:
        pass
    return []


def _collect_topic(country: dict, spec: dict, seen_titles: set, when_filter: str) -> list:
    code = country.get("code")
    limit = spec.get("max_items", 4)
    query_kr = spec["query_kr_tpl"].format(name_kr=country.get("name_kr", ""))
    query_en = spec["query_en_tpl"].format(name_en=country.get("name_en", ""))
    if code == "KR":
        plans = [_google_news_url_ko(f"{query_kr} {when_filter}")]
    else:
        plans = [_google_news_url_en(f"{query_en} {when_filter}"), _google_news_url_ko(f"{query_kr} {when_filter}")]

    results = []
    for url in plans:
        if len(results) >= limit:
            break
        try:
            candidates = _fetch_feed(url, limit=(limit - len(results)) * 6)
            for it in candidates:
                key = _dedup_key(it)
                if not key or key in seen_titles:
                    continue
                seen_titles.add(key)
                results.append(it)
                if len(results) >= limit:
                    break
        except Exception as e:
            log.warning(f"[{code}] '{spec.get('category')}' 수집 실패: {str(e)[:100]}")
    return results


def get_topic_categories(country: dict, cached: dict, seen_titles: set, specs: list, when_filter: str) -> list:
    categories = []
    for spec in specs:
        items = []
        try:
            items = _collect_topic(country, spec, seen_titles, when_filter)
        except Exception as e:
            log.warning(f"[{country.get('code')}] '{spec.get('category')}' 예외: {e}")
            items = []
        if not items:
            items = _find_cached_category(cached, spec["category"])[: spec.get("max_items", 4)]
        if not items:
            items = [_fallback_item(spec, country)]
        categories.append({
            "category": spec["category"],
            "tag": spec["tag"],
            "tag_class": spec["tag_class"],
            "items": items,
        })
    return categories


def _derive_headlines(news_categories: list, country: dict, limit: int = 3) -> list:
    picked, used = [], set()
    try:
        cats = [c for c in (news_categories or []) if isinstance(c, dict)]
        max_len = max((len(c.get("items") or []) for c in cats), default=0)
        for idx in range(max_len):
            for cat in cats:
                items = cat.get("items") or []
                if idx >= len(items):
                    continue
                it = items[idx]
                if not isinstance(it, dict) or it.get("is_fallback") or it.get("title") == FALLBACK_TEXT:
                    continue
                key = _dedup_key(it)
                if key in used:
                    continue
                used.add(key)
                h = {k: v for k, v in it.items() if not k.startswith("_")}
                h["category"] = cat.get("category", "")
                picked.append(h)
                if len(picked) >= limit:
                    return picked
    except Exception as e:
        log.warning(f"[{country.get('code')}] 헤드라인 파생 실패: {e}")
    return picked if picked else [_fallback_headline_item(country)]


def localize_all(countries: list) -> None:
    total = 0
    for c in countries:
        for group in ("news_categories", "hr_trends"):
            for cat in c.get(group, []) or []:
                for it in cat.get("items", []) or []:
                    try:
                        if isinstance(it, dict) and it.pop("_raw", False):
                            _localize_item(it)
                            total += 1
                    except Exception as e:
                        log.warning(f"[{c.get('code')}] 기사 현지화 실패, 원문 유지: {e}")
    log.info(f"[translate] 처리 기사 {total}건 / 번역 성공 {_TRANSLATE_STATE['ok']}건 / 실패 {_TRANSLATE_STATE['failed']}건"
             + (" / 중간에 번역 중단됨" if _TRANSLATE_STATE["disabled"] else ""))


# ---------------------------------------------------------------------------
# 캐시 / 아카이브
# ---------------------------------------------------------------------------
def load_cache() -> dict:
    payload = _read_json_file(CACHE_PATH)
    if isinstance(payload, dict) and isinstance(payload.get("countries"), list):
        out = {}
        for c in payload["countries"]:
            if isinstance(c, dict) and c.get("code"):
                out[_normalize_code(c["code"])] = c
        return out
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
    _write_json_file(os.path.join(archive_dir, "index.json"), {
        "dates": valid_dates,
        "available_dates": valid_dates,
        "min_date": valid_dates[0] if valid_dates else None,
        "latest": valid_dates[-1] if valid_dates else None,
    })


def save_archive(data: dict) -> None:
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    date_str = data["generated_at"][:10]
    with open(os.path.join(ARCHIVE_DIR, f"{date_str}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)
    clean_old_archives(ARCHIVE_DIR, max_days=ARCHIVE_RETENTION_DAYS)


def _is_legacy_archive(data: dict) -> bool:
    if not isinstance(data, dict):
        return True
    if data.get("schema_version") != SCHEMA_VERSION:
        return True
    countries = data.get("countries")
    if not isinstance(countries, list) or len(countries) != len(COUNTRIES):
        return True
    return False


def _migrate_exchange_rate_legacy(old_fx: dict, meta: dict) -> dict:
    currency = meta["currency"]
    unit_base = SMALL_UNIT_CURRENCIES.get(currency, 1)
    yahoo_url = _yahoo_finance_url(currency)

    if currency == "KRW":
        return {"is_base": True, "unit_base": 1, "current_rate": 1.0, "rate_per_unit": 1.0, "change_pct": 0.0,
                "history_labels": [], "history_values": [], "source": "기준통화", "url": ""}

    old_fx = old_fx if isinstance(old_fx, dict) else {}
    labels = old_fx.get("history_labels") if isinstance(old_fx.get("history_labels"), list) else []
    values = old_fx.get("history_values") if isinstance(old_fx.get("history_values"), list) else []
    if len(labels) != len(values):
        labels, values = [], []

    current_rate = _to_float(old_fx.get("current_rate")) or 0.0
    if current_rate <= 0 and not values:
        labels, values = _synthetic_monthly_series(currency, unit_base, months=12)
        current_rate = values[-1] if values else 0.0
        base = {"unit_base": unit_base, "current_rate": current_rate, "history_values": values,
                "rate_per_unit": round(current_rate / unit_base, 6) if unit_base else 0.0}
    else:
        base = _rescale_fx({
            "unit_base": old_fx.get("unit_base") if isinstance(old_fx.get("unit_base"), int) and old_fx.get("unit_base") > 0 else 1,
            "current_rate": current_rate,
            "history_values": values,
            "rate_per_unit": old_fx.get("rate_per_unit"),
        }, unit_base)

    change_pct = _to_float(old_fx.get("change_pct")) or 0.0
    return {
        "is_base": False,
        "unit_base": unit_base,
        "current_rate": base.get("current_rate", 0.0),
        "rate_per_unit": base.get("rate_per_unit", 0.0),
        "change_pct": round(change_pct, 2),
        "history_labels": [str(x) for x in labels],
        "history_values": base.get("history_values", []),
        "source": old_fx.get("source") if not _is_bad_value(old_fx.get("source")) else "이전 데이터(마이그레이션)",
        "url": yahoo_url,
    }


def _migrate_profile_legacy(old_profile: dict, meta: dict) -> dict:
    old_profile = old_profile if isinstance(old_profile, dict) else {}
    code = meta.get("code", "")
    old_indicators = old_profile.get("indicators") if isinstance(old_profile.get("indicators"), dict) else {}

    indicators = {"capital": _capital_entry(meta)}
    for key in ("population", "gdp", "inflation", "unemployment"):
        old = old_indicators.get(key)
        if isinstance(old, dict) and old.get("source") in API_SOURCES and _entry_valid(old):
            indicators[key] = dict(old)
        elif isinstance(old, dict) and old.get("source") == TE_SOURCE and old.get("link"):
            indicators[key] = dict(old)
        elif key in ("inflation", "unemployment"):
            indicators[key] = _te_entry(meta, key)
        else:
            indicators[key] = _placeholder_entry()

    old_mw = old_profile.get("min_wage")
    if isinstance(old_mw, dict) and not _is_bad_value(old_mw.get("display")) and old_mw.get("display") != PLACEHOLDER_TEXT:
        min_wage = {
            "display": str(old_mw.get("display")),
            "note": "" if _is_bad_value(old_mw.get("note")) else str(old_mw.get("note")),
            "source": "-" if _is_bad_value(old_mw.get("source")) else str(old_mw.get("source")),
            "year": old_mw.get("year", "-") if not _is_bad_value(old_mw.get("year")) else "-",
            "last_updated": old_mw.get("last_updated", "") or "",
        }
    else:
        mw = build_min_wage(code)
        min_wage = {"display": mw.get("display", PLACEHOLDER_TEXT), "note": mw.get("note", ""),
                    "source": mw.get("source", "-"), "year": mw.get("year", "-"),
                    "last_updated": mw.get("last_updated", "")}

    sources = []
    for entry in list(indicators.values()) + [min_wage]:
        src = entry.get("source")
        if src and src not in ("-", CAPITAL_SOURCE) and src not in sources:
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
        "stats_year": old_profile.get("stats_year", "-") if not _is_bad_value(old_profile.get("stats_year")) else "-",
        "stats_asof": old_profile.get("stats_asof") if not _is_bad_value(old_profile.get("stats_asof")) else "-",
    }


def _migrate_categories(old_country: dict, meta: dict, specs: list) -> list:
    result = []
    for spec in specs:
        items = _find_cached_category(old_country, spec["category"])[: spec.get("max_items", 4)]
        if not items:
            items = [_fallback_item(spec, meta)]
        result.append({
            "category": spec["category"],
            "tag": spec["tag"],
            "tag_class": spec["tag_class"],
            "items": items,
        })
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
    old_by_code = {}
    for c in old_countries:
        if isinstance(c, dict) and c.get("code"):
            old_by_code[_normalize_code(c.get("code"))] = c

    new_countries = []
    for meta in COUNTRIES:
        meta_copy = copy.deepcopy(meta)
        old_country = old_by_code.get(meta_copy["code"]) or {}
        old_offices = old_country.get("offices") if isinstance(old_country.get("offices"), list) else []
        old_offices = [str(o) for o in old_offices if not _is_bad_value(o)]
        offices = old_offices if old_offices else [OFFICE_PLACEHOLDER]
        offices_source = old_country.get("offices_source")
        if _is_bad_value(offices_source):
            offices_source = "Hankook Tire 공식 홈페이지" if offices != [OFFICE_PLACEHOLDER] else "확인 중"

        news_categories = _migrate_categories(old_country, meta_copy, NEWS_TOPICS)
        new_countries.append({
            "code": meta_copy["code"],
            "region": meta_copy["region"],
            "name_kr": meta_copy["name_kr"],
            "name_en": meta_copy["name_en"],
            "currency": meta_copy["currency"],
            "flag": meta_copy["flag"],
            "offices": offices,
            "hubs": offices,
            "offices_source": offices_source,
            "profile": _migrate_profile_legacy(old_country.get("profile", {}), meta_copy),
            "exchange_rate": _migrate_exchange_rate_legacy(old_country.get("exchange_rate", {}), meta_copy),
            "headlines": _derive_headlines(news_categories, meta_copy),
            "news_categories": news_categories,
            "hr_trends": _migrate_categories(old_country, meta_copy, AUTO_TOPICS),
        })

    new_data = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": old_data.get("generated_at") if not _is_bad_value(old_data.get("generated_at")) else f"{date_str}T06:00:00+09:00",
        "generated_at_display": old_data.get("generated_at_display") if not _is_bad_value(old_data.get("generated_at_display")) else f"{date_str} 06:00 KST",
        "as_of_display": f"{date_str} 06:00 KST 기준",
        "regions": REGIONS,
        "countries": new_countries,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(new_data, f, ensure_ascii=False, default=str)
    log.info(f"[migrate] {os.path.basename(path)} 를 schema_version {SCHEMA_VERSION} 으로 마이그레이션 완료 ({len(new_countries)}개국)")


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
        data = _read_json_file(path) or {}
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
# 국가 단위 수집 / 메인
# ---------------------------------------------------------------------------
def _resolve_offices(country: dict, shared: dict) -> tuple:
    official = (shared or {}).get("offices", {}) or {}
    names = official.get(country["code"])
    if isinstance(names, list) and names:
        cleaned = [str(n).strip() for n in names if not _is_bad_value(n)]
        if cleaned:
            return cleaned, "Hankook Tire 공식 홈페이지"
    return [OFFICE_PLACEHOLDER], "확인 중"


def collect_country(country: dict, cache_by_code: dict, shared: dict = None) -> dict:
    shared = shared or {}
    code = country["code"]
    cached = cache_by_code.get(code, {})
    log.info(f"[{code}] 수집 시작 — {country['name_kr']} (통화: {country['currency']})")

    seen_titles = set()
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
        log.error(f"[{code}] 프로필 구성 실패, 안전값으로 대체: {e}")
        result["profile"] = _migrate_profile_legacy((cached or {}).get("profile", {}), country)

    try:
        result["exchange_rate"] = get_exchange_rate(country, cached)
    except Exception as e:
        log.error(f"[{code}] 환율 수집 실패, 안전값으로 대체: {e}")
        result["exchange_rate"] = _migrate_exchange_rate_legacy((cached or {}).get("exchange_rate", {}), country)

    try:
        result["news_categories"] = get_topic_categories(country, cached, seen_titles, NEWS_TOPICS, NEWS_WHEN)
    except Exception as e:
        log.error(f"[{code}] 뉴스 카테고리 수집 실패, Fallback 대체: {e}")
        result["news_categories"] = _migrate_categories(cached or {}, country, NEWS_TOPICS)

    try:
        result["hr_trends"] = get_topic_categories(country, cached, seen_titles, AUTO_TOPICS, AUTO_WHEN)
    except Exception as e:
        log.error(f"[{code}] 완성차·타이어 동향 수집 실패, Fallback 대체: {e}")
        result["hr_trends"] = _migrate_categories(cached or {}, country, AUTO_TOPICS)

    result["headlines"] = []
    log.info(f"[{code}] 수집 완료")
    return result


def _build_country_fallback(country_code: str, shared: dict = None, cached: dict = None) -> dict:
    meta = next((copy.deepcopy(c) for c in COUNTRIES if c["code"] == country_code), None)
    if meta is None:
        meta = {"code": country_code, "region": "ALL", "name_kr": country_code, "name_en": country_code,
                "currency": "USD", "flag": "un", "capital": ""}
    cached = cached or {}
    offices, offices_source = _resolve_offices(meta, shared or {})
    try:
        profile = build_profile(meta, cached, shared or {})
    except Exception as e:
        log.error(f"[{country_code}] 안전망 프로필 구성 실패: {e}")
        profile = _migrate_profile_legacy({}, meta)
    news_categories = _migrate_categories(cached, meta, NEWS_TOPICS)
    return {
        "code": meta["code"], "region": meta["region"], "name_kr": meta["name_kr"],
        "name_en": meta["name_en"], "currency": meta["currency"], "flag": meta["flag"],
        "offices": offices, "hubs": offices, "offices_source": offices_source,
        "profile": profile,
        "exchange_rate": _migrate_exchange_rate_legacy(cached.get("exchange_rate", {}), meta),
        "headlines": [],
        "news_categories": news_categories,
        "hr_trends": _migrate_categories(cached, meta, AUTO_TOPICS),
    }


def _prepare_shared_sources() -> dict:
    shared = {"offices": {}, "economic": {}, "economic_asof": None}

    try:
        shared["offices"] = fetch_hankook_offices(COUNTRIES) or {}
    except Exception as e:
        log.error(f"한국타이어 공식 거점 정보 처리 실패, 안내 문구로 대체: {e}")
        shared["offices"] = {}

    try:
        economic, asof = load_economic_data(COUNTRIES)
        shared["economic"] = economic or {}
        shared["economic_asof"] = asof or None
    except Exception as e:
        log.error(f"거시경제 지표 처리 실패, 안내 문구로 대체: {e}")
        shared["economic"] = {}
        shared["economic_asof"] = None

    return shared


def main() -> dict:
    try:
        cache_by_code = load_cache()
    except Exception as e:
        log.error(f"캐시 로드 중 예기치 못한 오류, 빈 캐시로 계속 진행: {e}")
        cache_by_code = {}

    try:
        load_minwage_manual()
    except Exception as e:
        log.warning(f"최저임금 수동 테이블 준비 실패(기본값 사용): {e}")

    shared = _prepare_shared_sources()

    collected = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_country, copy.deepcopy(c), cache_by_code, shared): c["code"] for c in COUNTRIES}
        for future in as_completed(futures):
            code = futures[future]
            try:
                collected[code] = future.result()
            except Exception as e:
                log.error(f"[{code}] 국가 단위 수집 실패, 안전망으로 대체: {e}")
                try:
                    collected[code] = _build_country_fallback(code, shared, cache_by_code.get(code))
                except Exception as inner_e:
                    log.error(f"[{code}] 안전망 구성 중 오류, 최소 골격으로 대체: {inner_e}")
                    collected[code] = _build_country_fallback(code, {}, {})

    countries_result = []
    for c in COUNTRIES:
        entry = collected.get(c["code"])
        if not isinstance(entry, dict):
            entry = _build_country_fallback(c["code"], shared, cache_by_code.get(c["code"]))
        countries_result.append(entry)

    try:
        localize_all(countries_result)
    except Exception as e:
        log.error(f"기사 번역/언어 처리 단계 실패, 원문 유지: {e}")

    for c in countries_result:
        try:
            for group in ("news_categories", "hr_trends"):
                for cat in c.get(group, []) or []:
                    for it in cat.get("items", []) or []:
                        if isinstance(it, dict):
                            it.pop("_raw", None)
                            if not it.get("original_lang"):
                                _localize_item(it)
            c["headlines"] = _derive_headlines(c.get("news_categories", []), c)
        except Exception as e:
            log.warning(f"[{c.get('code')}] 헤드라인 구성 실패, 안내 문구로 대체: {e}")
            c["headlines"] = [_fallback_headline_item(c)]

    now_kst = datetime.now(KST)
    data = {
        "schema_version": SCHEMA_VERSION,
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
        log.error(f"구버전 아카이브 자동 마이그레이션 중 오류(사이트 생성에는 영향 없음): {e}")

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
