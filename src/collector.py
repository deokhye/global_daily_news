"""
collector.py
------------
Global Daily News — 39개 진출국 데이터 수집기 (경량화 버전).

이전 버전과의 가장 큰 차이: 39개국 기본 메타데이터(국가명/통화/수도/인구/GDP/
물가/실업률/최저임금/공식 거점)를 Python 코드에 하드코딩하지 않고
data/countries_metadata.json 에서 읽어온다. 이 파일은 코드 크기를 크게 줄여
토큰 초과 문제를 방지하고, 메타데이터 갱신 시 코드를 건드리지 않고 JSON
파일만 수정하면 되도록 관심사를 분리한다.

이 스크립트가 매 실행마다 "살아있는" 데이터로 새로 수집하는 것은 다음 두 가지뿐이다.
  1) 환율 (현지통화/KRW) — yfinance 기반 실시간 시세 + 최근 12개월 추이
  2) 뉴스 — 현지 주요 뉴스 3건, 비즈니스/HR 동향 4건 (Google News RSS)
나머지(수도/인구/GDP/물가/실업률/최저임금/공식 거점)는 countries_metadata.json의
값을 그대로 프로필에 반영한다.

핵심 보장 사항
  - 국가별 수집은 완전히 격리된다: 루프/스레드마다 country_code, country_currency,
    seen_titles 등을 매번 새로 초기화하므로, 예를 들어 일본(JP) 수집 결과에
    중국(CN) 뉴스가 섞여 들어가는 일이 구조적으로 불가능하다.
  - 환율 수집이 완전히 실패해 캐시조차 없는 극단적 상황에서도, 12개월 모두
    같은 값을 반복하는 일자선(flatline) 대신 현실적인 월별 변동(±0.5~1.5%)을
    가진 합성 시계열을 생성한다. 모든 환율 응답에는 야후 파이낸스 원문 링크
    (exchange_rate.url)가 포함된다.
  - 한국(KR) 뉴스는 번역 없이 국내 언론사 한국어 피드(hl=ko&gl=KR)에서 직접
    수집한다. 해외 38개국은 영문으로 수집한 뒤 deep-translator로 번역하며,
    번역이 실패하거나 빈 값이면 절대 'Error' 텍스트를 만들지 않고 수집된
    영문 원문을 그대로 사용한다.
  - docs/archive/*.json 을 스캔해 구버전 스키마(예: 9/8, 9/9 등 과거 포맷) 파일을
    발견하면 현재 스키마로 자동 변환해 덮어쓴다.
  - 180일이 지난 아카이브는 자동 삭제된다(Retention Policy).
"""

import os
import re
import copy
import json
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
ARCHIVE_DIR = os.path.join(BASE_DIR, "docs", "archive")
ARCHIVE_INDEX_PATH = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_RETENTION_DAYS = 180

MONTH_KR = ["1월", "2월", "3월", "4월", "5월", "6월",
            "7월", "8월", "9월", "10월", "11월", "12월"]

REQUEST_TIMEOUT = 10
MAX_WORKERS = 8  # 국가 단위 병렬 수집 스레드 수

SMALL_UNIT_CURRENCIES = {"VND": 100, "JPY": 100, "IDR": 100}
WON_DECIMALS = 1
FALLBACK_TEXT = "현지 산업 및 정책 모니터링 중"

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


def build_profile(country: dict, cached: dict) -> dict:
    meta = country.get("meta_profile", {})
    cached_profile = (cached or {}).get("profile", {})

    def pick(key, fallback_key=None):
        v = meta.get(key)
        if not _is_bad_value(v):
            return v
        return cached_profile.get(fallback_key or key, "-")

    return {
        "capital": pick("capital"),
        "population": pick("population"),
        "gdp": pick("gdp"),
        "inflation": pick("inflation"),
        "unemployment": pick("unemployment"),
        "min_wage": pick("min_wage"),
        "min_wage_note": "",
        "stats_source": "Hankook Tire Global HR 자체 조사",
        "stats_asof": METADATA_ASOF or "-",
    }


MIN_RELIABLE_TRADING_DAYS = 30


def _yf_daily_close(ticker: str, days: int = 400) -> pd.Series:
    try:
        import yfinance as yf
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
        values = [_round_won(v) for v in grouped]
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
        feed = feedparser.parse(url)
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


INDUSTRY_TOPICS = [
    {
        "category": "AUTO MARKET", "tag": "완성차·타이어 시장", "tag_class": "bg-rose-100 text-rose-700",
        "query_en_tpl": "{name_en} auto OEM tire market EV demand",
        "query_kr_tpl": "{name_kr} 완성차 타이어 시장 전기차 수요",
    },
    {
        "category": "HR & LABOR", "tag": "노동법·인력", "tag_class": "bg-indigo-100 text-indigo-700",
        "query_en_tpl": "{name_en} labor law manufacturing wages hiring",
        "query_kr_tpl": "{name_kr} 노동법 제조업 임금 채용",
    },
    {
        "category": "ECONOMY", "tag": "경기·금리", "tag_class": "bg-emerald-100 text-emerald-700",
        "query_en_tpl": "{name_en} economy interest rate manufacturing PMI outlook",
        "query_kr_tpl": "{name_kr} 경제 기준금리 제조업 PMI 전망",
    },
    {
        "category": "MANAGEMENT", "tag": "관세·공급망", "tag_class": "bg-amber-100 text-amber-700",
        "query_en_tpl": "{name_en} tariff trade policy supply chain logistics cost",
        "query_kr_tpl": "{name_kr} 관세 통상 정책 공급망 물류비",
    },
]


def _fallback_trend_item(spec: dict, country: dict) -> dict:
    try:
        query_en = spec["query_en_tpl"].format(name_en=country.get("name_en", ""))
        link = _google_news_search_link(query_en)
    except Exception:
        link = "https://news.google.com/"
    return {
        "category": spec.get("category", ""),
        "tag": spec.get("tag", ""),
        "tag_class": spec.get("tag_class", "bg-slate-100 text-slate-700"),
        "title": FALLBACK_TEXT,
        "desc": f"{spec.get('tag', '')} 관련 최신 기사를 찾지 못해 공식 Google 뉴스 검색 결과로 연결됩니다. 다음 갱신 시 자동으로 업데이트됩니다.",
        "source": "Google News 검색",
        "link": link,
        "lang": "ko",
        "translated": False,
        "original_title": "",
    }


def get_localized_items(query_kr: str, query_en: str, limit: int, when_filter: str, seen_titles: set) -> list:
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
    if remaining > 0:
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


def get_industry_trends(country: dict, cached: dict, seen_titles: set) -> list:
    country_name_kr = country.get("name_kr", "")
    country_name_en = country.get("name_en", "")
    country_code = country.get("code")

    cached_by_category = {t.get("category"): t for t in (cached or {}).get("hr_trends", [])}
    trends = []

    for spec in INDUSTRY_TOPICS:
        item = None
        try:
            query_kr = spec["query_kr_tpl"].format(name_kr=country_name_kr)
            query_en = spec["query_en_tpl"].format(name_en=country_name_en)
            picked = get_localized_items(query_kr, query_en, limit=1, when_filter="when:14d", seen_titles=seen_titles)
            if picked:
                item = picked[0]
        except Exception as e:
            log.warning(f"[{country_code}] '{spec.get('category')}' 산업 동향 수집 예외: {e}")
            item = None

        if item:
            try:
                trends.append({
                    "category": spec["category"],
                    "tag": spec["tag"],
                    "tag_class": spec["tag_class"],
                    "title": _shorten(item.get("title", ""), 46),
                    "desc": item.get("summary") or _shorten(item.get("title", ""), 70),
                    "source": item.get("source", "Google News"),
                    "link": item.get("link", ""),
                    "lang": item.get("lang", "ko"),
                    "translated": item.get("translated", False),
                    "original_title": item.get("original_title", ""),
                })
                continue
            except Exception as e:
                log.warning(f"[{country_code}] '{spec.get('category')}' 결과 가공 실패: {e}")

        try:
            cached_item = cached_by_category.get(spec["category"])
        except Exception:
            cached_item = None
        trends.append(cached_item if cached_item else _fallback_trend_item(spec, country))

    return trends


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
        if not isinstance(profile, dict) or "min_wage" not in profile:
            return True
        fx = c.get("exchange_rate")
        if not isinstance(fx, dict) or "unit_base" not in fx or "url" not in fx:
            return True
        hr_trends = c.get("hr_trends")
        if not isinstance(hr_trends, list) or len(hr_trends) < 4:
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

    def _clean(key, fallback="-"):
        v = old_profile.get(key)
        return fallback if _is_bad_value(v) else str(v)

    meta_profile = meta.get("meta_profile", {})
    return {
        "capital": _clean("capital", meta_profile.get("capital", "-")),
        "population": _clean("population", meta_profile.get("population", "-")),
        "gdp": _clean("gdp", meta_profile.get("gdp", "-")),
        "inflation": _clean("inflation", meta_profile.get("inflation", "-")),
        "unemployment": _clean("unemployment", meta_profile.get("unemployment", "-")),
        "min_wage": meta_profile.get("min_wage", _clean("min_wage")),
        "min_wage_note": "",
        "stats_source": "Hankook Tire Global HR 자체 조사",
        "stats_asof": METADATA_ASOF or "-",
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
        old_item = old_by_category.get(spec["category"])
        if old_item and not _is_bad_value(old_item.get("title")) and not _is_bad_value(old_item.get("link")):
            result.append({
                "category": spec["category"],
                "tag": spec["tag"],
                "tag_class": spec["tag_class"],
                "title": str(old_item.get("title")),
                "desc": "" if _is_bad_value(old_item.get("desc")) else str(old_item.get("desc")),
                "source": "Google News" if _is_bad_value(old_item.get("source")) else str(old_item.get("source")),
                "link": str(old_item.get("link")),
                "lang": "ko" if _is_bad_value(old_item.get("lang")) else str(old_item.get("lang")),
                "translated": bool(old_item.get("translated")) if isinstance(old_item.get("translated"), bool) else False,
                "original_title": "" if _is_bad_value(old_item.get("original_title")) else str(old_item.get("original_title")),
            })
        else:
            result.append(_fallback_trend_item(spec, meta))
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
        offices = meta_copy.get("offices", [])
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


def collect_country(country: dict, cache_by_code: dict) -> dict:
    country_code = country["code"]
    country_currency = country["currency"]
    cached = cache_by_code.get(country_code, {})
    log.info(f"[{country_code}] 수집 시작 — {country['name_kr']} (통화: {country_currency})")

    seen_titles: set = set()

    offices = copy.deepcopy(country.get("offices", []))
    result = {
        "code": country["code"],
        "region": country["region"],
        "name_kr": country["name_kr"],
        "name_en": country["name_en"],
        "currency": country["currency"],
        "flag": country["flag"],
        "offices": offices,
        "hubs": offices,
    }

    try:
        result["profile"] = build_profile(country, cached)
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
        result["hr_trends"] = cached_trends if cached_trends else [
            _fallback_trend_item(spec, country) for spec in INDUSTRY_TOPICS
        ]

    log.info(f"[{country_code}] 수집 완료")
    return result


def _build_country_fallback(country_code: str) -> dict:
    meta = next((copy.deepcopy(c) for c in COUNTRIES if c["code"] == country_code), None)
    if meta is None:
        meta = {"code": country_code, "region": "ALL", "name_kr": country_code,
                 "name_en": country_code, "currency": "USD", "flag": "un", "offices": [],
                 "meta_profile": {}}
    offices = meta.get("offices", [])
    return {
        "code": meta["code"], "region": meta["region"], "name_kr": meta["name_kr"],
        "name_en": meta["name_en"], "currency": meta["currency"], "flag": meta["flag"],
        "offices": offices, "hubs": offices,
        "profile": build_profile(meta, {}),
        "exchange_rate": {"is_base": False, "unit_base": 1, "current_rate": 0.0, "change_pct": 0.0,
                           "history_labels": [], "history_values": [], "source": "수집 실패",
                           "url": _yahoo_finance_url(meta.get("currency", ""))},
        "headlines": [_fallback_headline_item(meta)],
        "hr_trends": [_fallback_trend_item(spec, meta) for spec in INDUSTRY_TOPICS],
    }


def main() -> dict:
    try:
        cache_by_code = load_cache()
    except Exception as e:
        log.error(f"캐시 로드 중 예기치 못한 오류, 빈 캐시로 계속 진행: {e}")
        cache_by_code = {}

    collected = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(collect_country, copy.deepcopy(c), cache_by_code): c["code"] for c in COUNTRIES}
        for future in as_completed(futures):
            code = futures[future]
            try:
                collected[code] = future.result()
            except Exception as e:
                log.error(f"[{code}] 국가 단위 수집이 스레드에서 완전히 실패, 캐시/안전망으로 대체: {e}")
                try:
                    cached_fallback = cache_by_code.get(code)
                    collected[code] = cached_fallback if cached_fallback else _build_country_fallback(code)
                except Exception as inner_e:
                    log.error(f"[{code}] 안전망 구성 중에도 오류 발생, 최소 골격으로 대체: {inner_e}")
                    collected[code] = _build_country_fallback(code)

    countries_result = [collected.get(c["code"]) or _build_country_fallback(c["code"]) for c in COUNTRIES]

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
