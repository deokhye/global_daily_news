"""
build_site.py
-------------
data/countries_data.json(collector.py 결과)을 template.html 과 결합해 docs/index.html 을 생성한다.

  - 템플릿 폴더는 template/ 와 templates/ 를 모두 탐색한다(ChoiceLoader).
  - 데이터 파일이 없거나, 비었거나, 깨져 있어도 크래시하지 않는다.
    이 경우 빈 골격 데이터로 진행하며, 실패 지점마다 traceback 전체를 print 로 남긴다.
  - JSON 은 ensure_ascii=False 로 직렬화해 한글을 보존한다.
  - </ 를 이스케이프하고 Markup 으로 감싸, 템플릿의 |safe 와 함께 JSON.parse 가 깨지지 않게 한다.
  - 템플릿 로딩·렌더링이 실패하면 최소 안내 페이지를 대신 기록한다.
  - docs/index.html 기록 자체가 불가능한 경우에만 exit 1 로 종료한다.

실행:
    python src/collector.py
    python src/build_site.py
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, timezone

try:
    import pytz
    _KST = pytz.timezone("Asia/Seoul")
except Exception:
    _KST = None

from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape, TemplateError
from markupsafe import Markup

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
log = logging.getLogger("build_site")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_PATH = os.path.join(BASE_DIR, "data", "countries_data.json")
ARCHIVE_INDEX_PATH = os.path.join(BASE_DIR, "docs", "archive", "index.json")

TEMPLATE_DIR_SINGULAR = os.path.join(BASE_DIR, "template")
TEMPLATE_DIR_PLURAL = os.path.join(BASE_DIR, "templates")
TEMPLATE_NAME = "template.html"
OUTPUT_PATH = os.path.join(BASE_DIR, "docs", "index.html")

REQUIRED_PROFILE_FIELDS = ["capital", "population", "gdp", "inflation", "unemployment", "min_wage"]
NEWS_CATEGORY_KEYS = ["HR & LABOR", "SOCIETY & CULTURE", "POLITICS & ECONOMY"]

MINIMAL_FALLBACK_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Global Daily News</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Pretendard, sans-serif;
    background: #F8F9FA;
    color: #334155;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 100vh;
    margin: 0;
    padding: 24px;
    box-sizing: border-box;
  }}
  .box {{
    max-width: 560px;
    text-align: center;
    background: white;
    border: 1px solid #E5E7EB;
    border-radius: 1.25rem;
    padding: 2.5rem 2rem;
    box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.05), 0 2px 4px -2px rgb(0 0 0 / 0.05);
  }}
  h1 {{ font-size: 1.35rem; margin: 0 0 0.75rem; color: #1E293B; }}
  p {{ font-size: 0.9rem; line-height: 1.6; color: #64748B; margin: 0 0 0.5rem; }}
  .err {{
    margin-top: 1.25rem;
    font-size: 0.7rem;
    color: #94A3B8;
    background: #F1F5F9;
    border-radius: 0.5rem;
    padding: 0.75rem;
    text-align: left;
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 260px;
    overflow-y: auto;
  }}
</style>
</head>
<body>
  <div class="box">
    <h1>Global Daily News</h1>
    <p>사이트를 생성하는 중 문제가 발생해 임시 안내 페이지를 표시하고 있습니다.</p>
    <p>다음 자동 갱신(매일 KST 06:00) 시 정상 데이터로 복구될 예정입니다.</p>
    <div class="err">생성 기록 ({timestamp}):
{error}</div>
  </div>
</body>
</html>
"""


def _now_dt() -> datetime:
    try:
        if _KST is not None:
            return datetime.now(_KST)
    except Exception:
        pass
    return datetime.now(timezone.utc)


def _now_display() -> str:
    try:
        if _KST is not None:
            return datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _empty_skeleton_data() -> dict:
    now_dt = _now_dt()
    date_str = now_dt.strftime("%Y-%m-%d")
    return {
        "schema_version": None,
        "generated_at": now_dt.isoformat(),
        "generated_at_display": _now_display(),
        "as_of_display": f"{date_str} 06:00 KST 기준",
        "regions": [{"key": "ALL", "label": "전체"}],
        "countries": [],
    }


def load_data() -> dict:
    if not os.path.exists(DATA_PATH):
        print(f"[build_site] {DATA_PATH} 파일이 존재하지 않습니다. 빈 골격 데이터로 진행합니다.")
        return _empty_skeleton_data()

    try:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        print(f"[build_site] {DATA_PATH} 파일을 읽는 중 오류 발생. 빈 골격 데이터로 진행합니다.")
        print(traceback.format_exc())
        return _empty_skeleton_data()

    if not raw or not raw.strip():
        print(f"[build_site] {DATA_PATH} 파일이 비어 있습니다. 빈 골격 데이터로 진행합니다.")
        return _empty_skeleton_data()

    try:
        data = json.loads(raw)
    except Exception:
        print(f"[build_site] {DATA_PATH} JSON 파싱 실패. 빈 골격 데이터로 진행합니다.")
        print(traceback.format_exc())
        return _empty_skeleton_data()

    if not isinstance(data, dict):
        print(f"[build_site] {DATA_PATH} 내용이 dict 형태가 아닙니다(type={type(data).__name__}). 빈 골격 데이터로 진행합니다.")
        return _empty_skeleton_data()

    try:
        data.setdefault("generated_at", _now_dt().isoformat())
        data.setdefault("generated_at_display", _now_display())
        data.setdefault("as_of_display", f"{str(data.get('generated_at'))[:10]} 06:00 KST 기준")
        data.setdefault("regions", [{"key": "ALL", "label": "전체"}])
        data.setdefault("countries", [])

        if not isinstance(data.get("countries"), list):
            print(f"[build_site] countries 필드가 리스트가 아닙니다(type={type(data.get('countries')).__name__}). 빈 리스트로 대체합니다.")
            data["countries"] = []
        if not isinstance(data.get("regions"), list):
            print(f"[build_site] regions 필드가 리스트가 아닙니다(type={type(data.get('regions')).__name__}). 기본값으로 대체합니다.")
            data["regions"] = [{"key": "ALL", "label": "전체"}]
    except Exception:
        print("[build_site] 데이터 기본값 보정 중 오류. 빈 골격 데이터로 진행합니다.")
        print(traceback.format_exc())
        return _empty_skeleton_data()

    return data


def load_archive_range(today_str: str) -> tuple:
    try:
        if os.path.exists(ARCHIVE_INDEX_PATH):
            with open(ARCHIVE_INDEX_PATH, "r", encoding="utf-8") as f:
                idx = json.load(f)
            if isinstance(idx, dict):
                min_date = idx.get("min_date") or today_str
                max_date = idx.get("latest") or today_str
                if str(max_date) < today_str:
                    max_date = today_str
                return str(min_date), str(max_date)
        else:
            print(f"[build_site] {ARCHIVE_INDEX_PATH} 이 아직 없습니다 (최초 실행) → 오늘 날짜로 폴백합니다.")
    except Exception:
        print("[build_site] 아카이브 인덱스 로드 실패 → 오늘 날짜로 폴백합니다.")
        print(traceback.format_exc())
    return today_str, today_str


def _is_bad_value(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip().lower() in {"error", "err", "undefined", "null", "none", "nan", "n/a", "", "[object object]"}:
        return True
    return False


def _validate(data: dict) -> None:
    try:
        countries = data.get("countries", [])
        if not isinstance(countries, list) or not countries:
            print("[build_site] countries 목록이 비어 있습니다 — collector.py를 먼저 실행했는지 확인하세요.")
            return

        if data.get("schema_version") is None:
            print("[build_site] schema_version 이 없습니다 — 구버전 collector 결과일 수 있습니다(화면은 호환 모드로 표시).")

        for c in countries:
            try:
                if not isinstance(c, dict):
                    continue
                code = c.get("code")
                profile = c.get("profile", {}) or {}
                missing = [f for f in REQUIRED_PROFILE_FIELDS if not profile.get(f)]
                if missing:
                    log.warning(f"[{code}] profile 필드 누락: {missing} — 화면에는 '-'로 표시됩니다.")

                min_wage = profile.get("min_wage")
                if isinstance(min_wage, dict):
                    if _is_bad_value(min_wage.get("display")):
                        log.warning(f"[{code}] min_wage.display 값이 비어있거나 손상됨")
                elif _is_bad_value(min_wage):
                    log.warning(f"[{code}] min_wage 값이 비어있거나 손상됨")

                if "exchange_rate" not in c:
                    log.warning(f"[{code}] exchange_rate 필드가 없습니다.")

                news = c.get("news_categories")
                if isinstance(news, list):
                    present = {n.get("category") for n in news if isinstance(n, dict)}
                    lacking = [k for k in NEWS_CATEGORY_KEYS if k not in present]
                    if lacking:
                        log.warning(f"[{code}] news_categories 누락 카테고리: {lacking}")
                else:
                    log.warning(f"[{code}] news_categories 가 없습니다 — 구버전 headlines/hr_trends 로 호환 표시됩니다.")

                if not (c.get("hr_trends") or []):
                    log.warning(f"[{code}] hr_trends(AUTO MARKET)가 비어 있습니다.")
            except Exception as e:
                print(f"[build_site] _validate: 국가 항목 검증 중 오류(무시하고 계속): {e}")
                continue
    except Exception:
        print("[build_site] _validate 전체 실패(무시하고 빌드 계속):")
        print(traceback.format_exc())


def _write_minimal_fallback(error_message: str) -> None:
    try:
        page = MINIMAL_FALLBACK_HTML_TEMPLATE.format(
            timestamp=_now_display(),
            error=str(error_message).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"),
        )
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(page)
        print(f"[build_site] 최소 안내 페이지를 {OUTPUT_PATH} 에 대신 기록했습니다.")
    except Exception:
        print("[build_site] FATAL: 최소 안내 페이지 기록마저 실패했습니다.")
        print(traceback.format_exc())
        raise


def _resolve_template_env() -> Environment:
    candidate_dirs = [d for d in (TEMPLATE_DIR_SINGULAR, TEMPLATE_DIR_PLURAL) if os.path.isdir(d)]
    if not candidate_dirs:
        print(f"[build_site] 템플릿 폴더를 찾지 못했습니다: '{TEMPLATE_DIR_SINGULAR}', '{TEMPLATE_DIR_PLURAL}'")
        candidate_dirs = [TEMPLATE_DIR_SINGULAR, TEMPLATE_DIR_PLURAL]
    else:
        print(f"[build_site] 템플릿 탐색 경로: {candidate_dirs}")
    loader = ChoiceLoader([FileSystemLoader(d, encoding="utf-8") for d in candidate_dirs])
    return Environment(loader=loader, autoescape=select_autoescape(["html", "xml"]))


def _serialize_dashboard(data: dict, as_of_display: str) -> Markup:
    try:
        payload = {
            "schema_version": data.get("schema_version"),
            "as_of_display": as_of_display,
            "generated_at": data.get("generated_at"),
            "regions": data.get("regions", []),
            "countries": data.get("countries", []),
        }
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        raw_json = raw_json.replace("</", "<\\/").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
        return Markup(raw_json)
    except Exception:
        print("[build_site] ERROR: 대시보드 JSON 직렬화 실패, 빈 데이터로 대체합니다.")
        print(traceback.format_exc())
        return Markup(json.dumps({"as_of_display": as_of_display, "regions": [], "countries": []}, ensure_ascii=False))


def build(data: dict) -> bool:
    _validate(data)

    try:
        today_str = str(data.get("generated_at", ""))[:10]
        datetime.strptime(today_str, "%Y-%m-%d")
    except Exception:
        today_str = _now_dt().strftime("%Y-%m-%d")

    archive_min, archive_max = load_archive_range(today_str)
    as_of_display = data.get("as_of_display") or f"{today_str} 06:00 KST 기준"

    try:
        env = _resolve_template_env()
        template = env.get_template(TEMPLATE_NAME)
    except TemplateError as e:
        err = (f"Jinja2 템플릿 로딩 실패 ('{TEMPLATE_DIR_SINGULAR}' 와 '{TEMPLATE_DIR_PLURAL}' 양쪽에서 "
               f"{TEMPLATE_NAME} 을 찾지 못함): {e}")
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        _write_minimal_fallback(err)
        return False
    except Exception as e:
        err = f"템플릿 로딩 중 예기치 못한 오류: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        _write_minimal_fallback(err)
        return False

    ctx = {
        "as_of_display": as_of_display,
        "generated_at_date": today_str,
        "archive_min_date": archive_min,
        "archive_max_date": archive_max,
        "dashboard_json": _serialize_dashboard(data, as_of_display),
    }

    try:
        page = template.render(**ctx)
    except TemplateError as e:
        err = f"Jinja2 템플릿 렌더링 실패: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        _write_minimal_fallback(err)
        return False
    except Exception as e:
        err = f"템플릿 렌더링 중 예기치 못한 오류: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        _write_minimal_fallback(err)
        return False

    try:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(page)
    except Exception:
        print(f"[build_site] FATAL: {OUTPUT_PATH} 파일 기록 실패.")
        print(traceback.format_exc())
        raise

    country_count = len(data.get("countries", []))
    print(f"[build_site] 생성 완료 → {OUTPUT_PATH} ({country_count}개국, 아카이브 범위 {archive_min}~{archive_max})")
    return True


def main() -> None:
    try:
        data = load_data()
    except Exception:
        print("[build_site] load_data() 호출 중 예기치 못한 오류가 발생했습니다.")
        print(traceback.format_exc())
        data = _empty_skeleton_data()

    try:
        build(data)
    except Exception:
        print("[build_site] FATAL: build_site.py 실행이 복구 불가능한 오류로 중단되었습니다.")
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
