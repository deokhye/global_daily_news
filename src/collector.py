"""
collector.py
------------
Global Daily News — 39개 진출국 데이터 수집기 (공식 출처 전용 버전).

  1) 한국타이어 공식 거점명: 글로벌 네트워크 페이지 원문을 "{공식 조직명} ({도시})" 그대로 저장.
     data/hankook_offices_cache.json 이 있으면 재크롤링 없이 재사용(최초 1회만 수집).
     캐시가 없고 파싱도 실패할 때만 "공식 거점 정보 확인 중" 문구 사용 (수기 거점 리스트 없음).
  2) 국가 기본정보: 내부 메타데이터의 수기값을 읽지 않고 항목별 1순위 공식 API 로 수집한다.
       수도        REST Countries                        data/cache_capital.json   (최초 1회, 영구 재사용)
       총 인구     World Bank SP.POP.TOTL                data/cache_economic.json  (항목별 30일)
       GDP         World Bank NY.GDP.MKTP.CD + IMF WEO 교차확인 (더 최신 기준연도 채택)
       인플레이션  IMF WEO PCPIPCH → World Bank FP.CPI.TOTL.ZG
       실업률      ILOSTAT → World Bank SL.UEM.TOTL.ZS
       최저임금    OECD Data API → data/minwage_manual.json (수동 보완)
     모든 항목은 {display, source, year, last_updated} 구조이며, 항목마다 last_updated 를 따로 관리해
     30일이 지난 항목만 다시 호출한다. 전부 실패한 항목만 "데이터 확인 중" 으로 표시한다.
  3) 법정 최저임금이 없는 국가(스웨덴·오스트리아·이탈리아·싱가포르·UAE·사우디)는
     display = "-", note = "단체협약(CBA)으로 결정".
  4) 모든 외부 호출은 7초 타임아웃 + try/except, 국가 순회 시 호출 간 0.2~0.3초 지연.
  5) 뉴스 제목 언어 감지(langdetect) → 한국어가 아니면 deep-translator 로 번역하고
     original_lang / original_title / is_translated 를 함께 저장 (링크는 항상 원문 URL 유지).
     requirements.txt 에 langdetect 가 필요하다 (미설치 시 한글 포함 여부로 ko/en 만 판별).

뉴스 / 환율 / 국가 정렬 / 아카이브 마이그레이션 / 180일 보존 로직은 기존과 동일하다.
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
CAPITAL_CACHE_PATH = os.path.join(DATA_DIR, "cache_capital.json")
ECONOMIC_CACHE_PATH = os.path.join(DATA_DIR, "cache_economic.json")
MINWAGE_MANUAL_PATH = os.path.join(DATA_DIR, "minwage_manual.json")
ARCHIVE_DIR = os.path.join(BASE_DIR, "docs", "archive")
ARCHIVE_INDEX_PATH = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_RETENTION_DAYS = 180

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

MONTH_KR = ["1월", "2월", "3월", "4월", "5월", "6월",
            "7월", "8월", "9월", "10월", "11월", "12월"]

MAX_WORKERS = 8  # 국가 단위 병렬 수집 스레드 수

SMALL_UNIT_CURRENCIES = {"VND": 100, "JPY": 100, "IDR": 100}
WON_DECIMALS = 1
FALLBACK_TEXT = "현지 산업 및 정책 모니터링 중"
PLACEHOLDER_TEXT = "데이터 확인 중"
OFFICE_PLACEHOLDER = "공식 거점 정보 확인 중"
API_SOURCES = {"World Bank", "IMF WEO", "ILOSTAT", "OECD", "REST Countries", "수동 보완"}

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
# 최저임금 수동 보완 기본값. OECD Data API 에 없는 국가를 위한 것이며,
# 최초 실행 시 data/minwage_manual.json 으로 저장되어 이후에는 그 파일을 직접 수정해 관리한다.
# 법정 최저임금이 없는 국가는 display = "-" 이다.
MIN_WAGE_MANUAL_DEFAULT = {
    "KR": {"display": "10,320원 / 시간", "note": "2026년 최저임금", "year": 2026, "basis": "고용노동부 고시"},
    "CN": {"display": "2,740위안 / 월", "note": "상하이시 기준 (지역별 상이)", "year": 2025, "basis": "상하이시 인력자원사회보장국 고시"},
    "US": {"display": "7.25달러 / 시간", "note": "연방 기준 (주별 상이)", "year": 2025, "basis": "U.S. Department of Labor"},
    "CA": {"display": "17.20캐나다달러 / 시간", "note": "온타리오주 기준", "year": 2025, "basis": "Ontario Ministry of Labour"},
    "DE": {"display": "13.90유로 / 시간", "note": "법정 최저임금", "year": 2026, "basis": "Eurostat"},
    "HU": {"display": "322,800포린트 / 월", "note": "월 총액 기준", "year": 2026, "basis": "Eurostat"},
    "UK": {"display": "12.71파운드 / 시간", "note": "21세 이상 National Living Wage", "year": 2026, "basis": "GOV.UK (Low Pay Commission)"},
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
    "SE": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2025, "basis": "수동 보완"},
    "AT": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2025, "basis": "수동 보완"},
    "IT": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2025, "basis": "수동 보완"},
    "SG": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2025, "basis": "수동 보완"},
    "AE": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2025, "basis": "수동 보완"},
    "SA": {"display": "-", "note": "단체협약(CBA)으로 결정", "year": 2025, "basis": "수동 보완"},
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


def _is_bad_value(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"error", "err", "undefined", "null", "none", "nan", "n/a", ""}:
        return True
    return False


def load_countries_metadata() -> list:
    """식별 정보(코드/국가명/통화/권역)만 메타데이터에서 읽는다. 경제지표·거점 수기값은 읽지 않는다."""
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
        code = str(row["code"]).strip().upper()
        countries.append({
            "code": code,
            "region": row.get("region", "ALL"),
            "name_kr": row.get("name_ko", code),
            "name_en": row.get("name_en", code),
            "currency": row.get("currency", "USD"),
            "flag": code.lower(),
            "curr_unit": row.get("curr_unit", ""),
        })

    if not countries:
        raise ValueError(f"{METADATA_PATH} 에서 유효한 국가를 하나도 읽지 못했습니다.")

    return countries


COUNTRIES = load_countries_metadata()


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


def _clean_office_line(line: str) -> str:
    return " ".join((line or "").split())


def _is_city_candidate(text: str, name_index: dict) -> bool:
    """조직명 바로 다음 줄이 도시명 단독 표기인지 판별 (원문 그대로 사용, 변형하지 않음)."""
    try:
        t = _clean_office_line(text)
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


def parse_hankook_global_network(html: str, countries: list) -> dict:
    """
    공식 글로벌 네트워크 페이지 HTML → {country_code: ["조직명 (도시)", ...]}
    홈페이지에 표기된 원문을 번역/재구성하지 않고 그대로 사용한다.
    """
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
        lines = [_clean_office_line(l) for l in soup.get_text("\n").split("\n")]
        lines = [l for l in lines if l]
    except Exception as e:
        log.warning(f"[hankook] HTML 파싱 실패: {e}")
        return result

    name_index = _build_country_name_index(countries)
    current = None
    skip_next = set()

    for i, line in enumerate(lines):
        try:
            if i in skip_next:
                continue
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

            entry = line
            has_city = bool(re.search(r"\([^()]+\)\s*$", line))
            if not has_city and i + 1 < len(lines) and _is_city_candidate(lines[i + 1], name_index):
                entry = f"{line} ({lines[i + 1]})"
                skip_next.add(i + 1)

            bucket = result.setdefault(target, [])
            if entry not in bucket and len(bucket) < 12:
                bucket.append(entry)
        except Exception:
            continue

    return {code: names for code, names in result.items() if names}


def fetch_hankook_offices(countries: list) -> dict:
    """
    반환: {country_code: ["조직명 (도시)", ...]} (확보한 국가만 포함).
    data/hankook_offices_cache.json 이 존재하면 재크롤링 없이 그대로 재사용한다.
    캐시 파일이 없을 때만 크롤링하며, 성공했을 때만 캐시를 저장한다.
    """
    cached = _read_json_file(HANKOOK_CACHE_PATH)
    if isinstance(cached, dict) and isinstance(cached.get("offices"), dict):
        log.info("[hankook] data/hankook_offices_cache.json 사용 (재크롤링 없음)")
        return {k: v for k, v in cached["offices"].items() if isinstance(v, list) and v}

    log.info("[hankook] 공식 글로벌 네트워크 페이지 1회 크롤링 시작")
    offices, used_url, reason = {}, "", ""
    for url in HANKOOK_NETWORK_URLS:
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=API_TIMEOUT)
            if resp.status_code != 200:
                reason = f"HTTP {resp.status_code}"
                continue
            resp.encoding = resp.apparent_encoding or "utf-8"
            parsed = parse_hankook_global_network(resp.text, countries)
            if len(parsed) >= HANKOOK_MIN_COUNTRIES:
                offices, used_url = parsed, url
                break
            reason = f"파싱 결과 부족({len(parsed)}개국, JS 렌더링 페이지로 추정)"
        except Exception as e:
            reason = str(e)[:120]

    if not offices:
        log.warning(f"[hankook] 공식 페이지 파싱 실패 → '{OFFICE_PLACEHOLDER}' 문구 사용, 캐시는 저장하지 않음 ({reason})")
        return {}

    _write_json_file(HANKOOK_CACHE_PATH, {
        "source_url": used_url,
        "fetched_at": datetime.now(KST).isoformat(),
        "status": "ok",
        "offices": offices,
    })
    return offices


# ---------------------------------------------------------------------------
# 2) 국가 기본정보: 항목별 1순위 공식 API + 항목별 독립 캐시 (내부 수기값 없음)
#
#   수도        REST Countries                       data/cache_capital.json   (최초 1회, 영구 재사용)
#   총 인구     World Bank SP.POP.TOTL               data/cache_economic.json  (항목별 30일)
#   GDP         World Bank NY.GDP.MKTP.CD + IMF NGDPD 교차확인 → 더 최신 연도 채택
#   인플레이션  IMF WEO PCPIPCH → World Bank FP.CPI.TOTL.ZG
#   실업률      ILOSTAT → World Bank SL.UEM.TOTL.ZS
#   최저임금    OECD Data API → data/minwage_manual.json (수동 보완)
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(KST).isoformat()


def _stamp(entry, note: str = None):
    """수집 성공한 항목에 last_updated(수집일시)를 기록한다."""
    if not isinstance(entry, dict):
        return None
    stamped = dict(entry)
    stamped["last_updated"] = _now_iso()
    if note is not None:
        stamped["note"] = note
    return stamped


def _safe_call(label: str, func, *args):
    """어떤 예외가 나도 None 을 돌려주고 다음 소스로 넘어가게 한다."""
    try:
        return func(*args)
    except Exception as e:
        log.warning(f"{label} 처리 예외, 다음 출처로 전환: {str(e)[:120]}")
        return None


def _pick_newer(first, second):
    """두 소스의 값 중 기준연도가 더 최신인 것을 채택한다. 연도가 같으면 first 우선."""
    if not first and not second:
        return None
    if first and not second:
        return first
    if second and not first:
        return second
    year_first = _year_from_value(first.get("year")) or 0
    year_second = _year_from_value(second.get("year")) or 0
    return second if year_second > year_first else first


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


def _fetch_rest_countries(code: str, field: str) -> dict:
    """field: 'capital' 또는 'population'. 성공 시 {field: entry}."""
    out = {}
    source = "REST Countries"
    if _source_disabled(source):
        return out
    try:
        iso2 = _iso2_for(code)
        url = f"https://restcountries.com/v3.1/alpha/{iso2}?fields={field}"
        status, payload, _ = _api_get_json(source, url)
        if status != 200 or payload is None:
            return out
        row = payload[0] if isinstance(payload, list) and payload else payload
        if not isinstance(row, dict):
            return out
        year = datetime.now(KST).year

        if field == "capital":
            capitals = row.get("capital")
            if isinstance(capitals, list) and capitals and not _is_bad_value(capitals[0]):
                name = str(capitals[0]).strip()
                out["capital"] = {"value": name, "display": name, "source": source, "year": year}
        elif field == "population":
            pop = _to_float(row.get("population"))
            if pop is not None and pop > 0:
                entry = _make_entry("population", pop, source, year)
                if entry:
                    out["population"] = entry
    except Exception as e:
        log.warning(f"[{code}] REST Countries({field}) 처리 실패: {e}")
    return out


# ---- SDMX-JSON 공통 파서 (OECD / ILOSTAT 응답) ----------------------------
def _parse_sdmx_json(payload) -> list:
    """
    SDMX-JSON 응답을 [{차원ID: 값ID, ..., 'value': float}, ...] 로 평탄화한다.
    AllDimensions(observations) 형태와 series 형태, SDMX-JSON 1.0/2.0 구조를 모두 처리한다.
    """
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
    """'2024' / '2024-Q2' / '2024-M05' / '2024-06' → (연도, 연내 순서) 정렬 키."""
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


# ---- 실업률: ILOSTAT (SDMX) → World Bank ------------------------------------
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
        year = _year_from_value(best.get("TIME_PERIOD"))
        entry = _make_entry("unemployment", best["value"], source, year)
        if entry:
            _ILO_STATE["dots"] = dots
            _ILO_STATE["empty_streak"] = 0
            return entry

    _ILO_STATE["empty_streak"] += 1
    if _ILO_STATE["empty_streak"] >= 6 and _ILO_STATE["dots"] is None:
        _disable_source(source, "응답 형식을 확인하지 못해 World Bank 로 대체")
    return None


# ---- 최저임금: OECD → 수동 보완 ---------------------------------------------
_MINWAGE_MANUAL = None


def load_minwage_manual() -> dict:
    """
    data/minwage_manual.json 의 수동 보완 테이블을 읽는다. 파일이 없으면 기본값으로 생성한다.
    형식: {"countries": {"KR": {"display": "...", "note": "...", "year": 2026, "basis": "..."}}}
    """
    global _MINWAGE_MANUAL
    if _MINWAGE_MANUAL is not None:
        return _MINWAGE_MANUAL

    table = {code: dict(row) for code, row in MIN_WAGE_MANUAL_DEFAULT.items()}
    stored = _read_json_file(MINWAGE_MANUAL_PATH)
    if isinstance(stored, dict) and isinstance(stored.get("countries"), dict):
        for code, row in stored["countries"].items():
            if isinstance(row, dict) and not _is_bad_value(row.get("display")):
                table[str(code).upper()] = row
    else:
        _write_json_file(MINWAGE_MANUAL_PATH, {
            "_comment": "OECD 에 없는 국가의 최저임금 수동 보완 테이블. display/note/year/basis 를 직접 수정할 수 있습니다.",
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

    pay_rank = {"H": 0, "M": 1, "A": 2, "W": 3}
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


def fetch_min_wage(code: str):
    """수동 테이블에서 '법정 최저임금 없음' 이면 즉시 '-' → OECD → 수동 보완 순서."""
    manual = _manual_minwage_entry(code)
    if manual and manual["display"] == "-":
        return manual
    oecd = _safe_call(f"[{code}] OECD 최저임금", _fetch_oecd_min_wage, code)
    if oecd:
        return oecd
    return manual


def build_min_wage(code: str) -> dict:
    """아카이브 마이그레이션/안전망용: API 호출 없이 수동 보완 테이블만 사용한다."""
    entry = _safe_call(f"[{code}] 수동 최저임금", _manual_minwage_entry, code)
    if entry:
        return entry
    return {"display": PLACEHOLDER_TEXT, "note": "", "source": "-", "year": "-"}


# ---- 항목별 수집 -------------------------------------------------------------
def collect_economic_items(country: dict, need: list) -> dict:
    """
    need 에 포함된 항목만 호출한다. 반환: {item: entry(last_updated 포함)}.
    모든 소스가 실패한 항목은 결과에 포함하지 않는다.
    """
    code = country["code"]
    found = {}

    wb_keys = [k for k in ("population", "gdp") if k in need]
    wb_data = _safe_call(f"[{code}] World Bank", _fetch_world_bank, code, wb_keys) or {} if wb_keys else {}

    if "population" in need:
        entry = wb_data.get("population")
        if not entry:
            entry = (_safe_call(f"[{code}] REST Countries 인구", _fetch_rest_countries, code, "population") or {}).get("population")
        if entry:
            found["population"] = _stamp(entry)

    if "gdp" in need:
        imf = (_safe_call(f"[{code}] IMF WEO GDP", _fetch_imf_weo, code, ["gdp"]) or {}).get("gdp")
        entry = _pick_newer(wb_data.get("gdp"), imf)
        if entry:
            found["gdp"] = _stamp(entry)

    if "inflation" in need:
        entry = (_safe_call(f"[{code}] IMF WEO 물가", _fetch_imf_weo, code, ["inflation"]) or {}).get("inflation")
        if not entry:
            entry = (_safe_call(f"[{code}] World Bank 물가", _fetch_world_bank, code, ["inflation"]) or {}).get("inflation")
        if entry:
            found["inflation"] = _stamp(entry)

    if "unemployment" in need:
        entry = _safe_call(f"[{code}] ILOSTAT 실업률", _fetch_ilostat_unemployment, code)
        if not entry:
            entry = (_safe_call(f"[{code}] World Bank 실업률", _fetch_world_bank, code, ["unemployment"]) or {}).get("unemployment")
        if entry:
            found["unemployment"] = _stamp(entry)

    if "min_wage" in need:
        entry = _safe_call(f"[{code}] 최저임금", fetch_min_wage, code)
        if entry:
            found["min_wage"] = entry

    return found


def _entry_valid(entry) -> bool:
    return (isinstance(entry, dict)
            and not _is_bad_value(entry.get("display"))
            and entry.get("display") != PLACEHOLDER_TEXT)


def _entry_fresh(entry, now) -> bool:
    if not _entry_valid(entry):
        return False
    updated = _parse_iso_datetime(entry.get("last_updated"))
    if updated is None:
        return False
    return (now - updated) < timedelta(days=ECONOMIC_REFRESH_DAYS)


def load_economic_data(countries: list) -> tuple:
    """
    반환: ({code: {capital/population/gdp/inflation/unemployment/min_wage: entry}}, 기준일)
      - 수도: data/cache_capital.json 에 값이 있으면 영구 재사용 (없는 국가만 호출)
      - 나머지: data/cache_economic.json 의 항목별 last_updated 가 30일 이내면 재사용, 지난 항목만 재호출
    """
    now = datetime.now(KST)

    capital_cache = _read_json_file(CAPITAL_CACHE_PATH)
    capitals = capital_cache.get("countries") if isinstance(capital_cache, dict) and isinstance(capital_cache.get("countries"), dict) else {}
    economic_cache = _read_json_file(ECONOMIC_CACHE_PATH)
    stored = economic_cache.get("countries") if isinstance(economic_cache, dict) and isinstance(economic_cache.get("countries"), dict) else {}

    result = {}
    capital_changed = False
    economic_changed = False
    api_countries = 0

    for country in countries:
        code = country["code"]
        items = {}

        # 1) 수도 — 최초 1회 수집 후 영구 재사용
        try:
            cap = capitals.get(code)
            if _entry_valid(cap):
                items["capital"] = cap
            else:
                fetched = (_safe_call(f"[{code}] REST Countries 수도", _fetch_rest_countries, code, "capital") or {}).get("capital")
                if fetched:
                    stamped = _stamp(fetched)
                    capitals[code] = stamped
                    items["capital"] = stamped
                    capital_changed = True
        except Exception as e:
            log.warning(f"[{code}] 수도 처리 예외: {e}")

        # 2) 인구/GDP/인플레이션/실업률/최저임금 — 항목별 30일 주기
        try:
            old = stored.get(code) if isinstance(stored.get(code), dict) else {}
            need = []
            for key in ECONOMIC_ITEMS:
                entry = old.get(key)
                if _entry_fresh(entry, now):
                    if key == "min_wage" and entry.get("source") == "수동 보완":
                        latest = _manual_minwage_entry(code)
                        if latest and (latest["display"] != entry.get("display")
                                       or latest["note"] != entry.get("note")
                                       or latest["year"] != entry.get("year")):
                            entry = latest
                            old[key] = entry
                            economic_changed = True
                    items[key] = entry
                else:
                    need.append(key)

            if need:
                api_countries += 1
                fetched = collect_economic_items(country, need)
                for key in need:
                    if key in fetched:
                        items[key] = fetched[key]
                        old[key] = fetched[key]
                        economic_changed = True
                    elif _entry_valid(old.get(key)):
                        items[key] = old[key]   # 갱신 실패 시 직전 값 유지 (다음 실행에서 재시도)
            stored[code] = old
        except Exception as e:
            log.warning(f"[{code}] 경제지표 처리 예외: {e}")

        result[code] = items

    if capital_changed or not os.path.exists(CAPITAL_CACHE_PATH):
        if capitals:
            _write_json_file(CAPITAL_CACHE_PATH, {"updated_at": now.isoformat(), "countries": capitals})
    if economic_changed or not os.path.exists(ECONOMIC_CACHE_PATH):
        if any(stored.get(c["code"]) for c in countries):
            _write_json_file(ECONOMIC_CACHE_PATH, {"updated_at": now.isoformat(), "refresh_days": ECONOMIC_REFRESH_DAYS,
                                                   "countries": stored})

    latest_dates = []
    for items in result.values():
        for entry in items.values():
            dt = _parse_iso_datetime(entry.get("last_updated")) if isinstance(entry, dict) else None
            if dt:
                latest_dates.append(dt)
    asof = max(latest_dates).strftime("%Y-%m-%d") if latest_dates else now.strftime("%Y-%m-%d")

    log.info(f"[economic] 완료: 항목 갱신이 필요했던 국가 {api_countries}/{len(countries)}개 "
             f"(나머지는 캐시 재사용)")
    return result, asof


# ---------------------------------------------------------------------------
# 프로필 구성 (내부 수기값 미사용)
# ---------------------------------------------------------------------------
def _placeholder_entry() -> dict:
    return {"value": None, "display": PLACEHOLDER_TEXT, "source": "-", "year": "-"}


def build_profile(country: dict, cached: dict, shared: dict = None) -> dict:
    shared = shared or {}
    code = country.get("code", "")
    cached_profile = (cached or {}).get("profile", {}) or {}
    cached_indicators = cached_profile.get("indicators", {}) if isinstance(cached_profile.get("indicators"), dict) else {}
    econ = (shared.get("economic") or {}).get(code, {}) or {}

    indicators = {}
    for key in ("capital", "population", "gdp", "inflation", "unemployment"):
        entry = None
        try:
            api_entry = econ.get(key)
            if isinstance(api_entry, dict) and api_entry.get("source") in API_SOURCES and _entry_valid(api_entry):
                entry = dict(api_entry)
            if entry is None:
                old = cached_indicators.get(key)
                if isinstance(old, dict) and old.get("source") in API_SOURCES and _entry_valid(old):
                    entry = dict(old)
        except Exception as e:
            log.warning(f"[{code}] '{key}' 지표 구성 실패: {e}")
            entry = None
        indicators[key] = entry if entry is not None else _placeholder_entry()

    min_wage = econ.get("min_wage")
    if not _entry_valid(min_wage):
        min_wage = build_min_wage(code)
    min_wage = {
        "display": min_wage.get("display", PLACEHOLDER_TEXT),
        "note": min_wage.get("note", ""),
        "source": min_wage.get("source", "-"),
        "year": min_wage.get("year", "-"),
        "last_updated": min_wage.get("last_updated", ""),
    }

    sources = []
    years = []
    for entry in list(indicators.values()) + [min_wage]:
        src = entry.get("source")
        if src and src != "-" and src not in sources:
            sources.append(src)
        y = _year_from_value(entry.get("year"))
        if y:
            years.append(y)

    asof = shared.get("economic_asof") or "-"
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


def detect_language(text: str) -> str:
    """제목의 원문 언어 코드를 반환. 감지 실패 시 'en'."""
    try:
        t = (text or "").strip()
        if not t:
            return "en"
        if len(re.findall(r"[가-힣]", t)) >= 2:
            return "ko"
        try:
            from langdetect import detect, DetectorFactory
            DetectorFactory.seed = 0
            code = str(detect(t)).lower()
            if code.startswith("zh"):
                return "zh"
            return code or "en"
        except Exception:
            return "en"
    except Exception:
        return "en"


def _localize_item(item: dict, allow_translate: bool = True) -> dict:
    """
    기사 dict 에 original_lang / original_title / is_translated 를 채우고,
    원문이 한국어가 아니면 제목(과 요약)을 한국어로 번역한다. link 는 건드리지 않는다.
    번역 실패/예외 시 원문을 그대로 사용하고 is_translated = False.
    """
    original_title = item.get("title", "") or ""
    original_summary = item.get("summary", "") or ""
    lang = "en"
    title = original_title
    summary = original_summary
    is_translated = False
    try:
        lang = detect_language(original_title)
        if allow_translate and lang != "ko":
            try:
                translated_title = translate_to_ko(original_title)
                if translated_title and str(translated_title).strip() and translated_title != original_title:
                    title = translated_title
                    is_translated = True
                if original_summary:
                    translated_summary = translate_to_ko(original_summary)
                    if translated_summary and str(translated_summary).strip():
                        summary = translated_summary
            except Exception as e:
                log.warning(f"기사 번역 실패, 원문 그대로 사용: {e}")
                title, summary, is_translated = original_title, original_summary, False
    except Exception as e:
        log.warning(f"기사 언어 처리 실패, 원문 그대로 사용: {e}")
        title, summary, is_translated, lang = original_title, original_summary, False, "en"

    item["title"] = title
    item["summary"] = summary
    item["original_title"] = original_title
    item["original_lang"] = lang
    item["is_translated"] = is_translated
    item["lang"] = lang
    item["translated"] = is_translated
    return item


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
        "original_lang": "ko",
        "is_translated": False,
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
                _localize_item(item, allow_translate=False)
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
                _localize_item(item)
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
        "original_lang": "ko",
        "is_translated": False,
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
                _localize_item(it, allow_translate=not kr_only)
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
                    _localize_item(it)
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
                "original_lang": "ko" if _is_bad_value(cached_cat.get("original_lang")) else str(cached_cat.get("original_lang")),
                "is_translated": bool(cached_cat.get("is_translated", cached_cat.get("translated"))) if isinstance(cached_cat.get("is_translated", cached_cat.get("translated")), bool) else False,
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
                        "lang": it.get("original_lang", it.get("lang", "ko")),
                        "translated": it.get("is_translated", False),
                        "original_title": it.get("original_title", ""),
                        "original_lang": it.get("original_lang", "ko"),
                        "is_translated": it.get("is_translated", False),
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
    old_indicators = old_profile.get("indicators") if isinstance(old_profile.get("indicators"), dict) else {}

    indicators = {}
    for key in ("capital", "population", "gdp", "inflation", "unemployment"):
        old = old_indicators.get(key)
        if isinstance(old, dict) and old.get("source") in API_SOURCES and not _is_bad_value(old.get("display")):
            indicators[key] = dict(old)
        else:
            indicators[key] = _placeholder_entry()

    min_wage = build_min_wage(code)

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
        "stats_asof": old_profile.get("stats_asof") if not _is_bad_value(old_profile.get("stats_asof")) else "-",
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
        "original_lang": "ko" if _is_bad_value(old_item.get("original_lang", old_item.get("lang"))) else str(old_item.get("original_lang", old_item.get("lang"))),
        "is_translated": bool(old_item.get("is_translated", old_item.get("translated"))) if isinstance(old_item.get("is_translated", old_item.get("translated")), bool) else False,
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
        offices = old_offices if old_offices else [OFFICE_PLACEHOLDER]
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
    return [OFFICE_PLACEHOLDER], "확인 중"


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
                "name_en": country_code, "currency": "USD", "flag": "un"}
    offices, offices_source = _resolve_offices(meta, shared or {})
    try:
        profile = build_profile(meta, {}, shared or {})
    except Exception as e:
        log.error(f"[{country_code}] 안전망 프로필 구성 실패: {e}")
        profile = {"capital": "-", "population": "-", "gdp": "-", "inflation": "-", "unemployment": "-",
                   "min_wage": build_min_wage(country_code), "min_wage_note": "",
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
