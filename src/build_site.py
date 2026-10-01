"""
build_site.py
--------------
data/countries_data.json (collector.py가 생성한 오늘자 데이터) 을
template.html 에 임베드하여 docs/index.html 을 생성한다.

이번 수정 사항
  1) 템플릿 디렉토리 탐색 보강 — 저장소 구조상 폴더명이 단수형 "template" 일 수도,
     복수형 "templates" 일 수도 있으므로 FileSystemLoader에 두 경로를 모두 등록해
     어느 쪽이든 TemplateNotFound 없이 찾아내도록 한다.
  2) data/countries_data.json 로드를 완전히 안전하게 처리 — 파일이 없거나/비었거나/
     JSON 파싱에 실패해도 절대 크래시하지 않고 빈 골격 데이터로 폴백하며, 모든 실패
     지점에서 traceback.format_exc() 전체를 표준 출력(print)에 남겨 GitHub Actions
     로그에서 바로 원인을 확인할 수 있게 한다.
  3) raw_json 직렬화를 json.dumps(ensure_ascii=False, default=str) 로 처리해 한글이
     \\uXXXX 로 깨지지 않도록 하고, </script> 이스케이프 + Markup 래핑으로 Jinja2
     autoescape에 의한 이중 인용구/따옴표 충돌을 원천 차단한다.
  4) docs/index.html 자체를 디스크에 쓰는 것조차 실패하는 경우(진짜로 복구 불가능한
     상황)에만 최종적으로 실패(exit 1)를 알리며, 그 외 모든 경로에서는 최소한이라도
     유효한 HTML 파일을 남기고 정상 종료한다.

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

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_PATH = os.path.join(BASE_DIR, "data", "countries_data.json")
ARCHIVE_INDEX_PATH = os.path.join(BASE_DIR, "docs", "archive", "index.json")

TEMPLATE_DIR_SINGULAR = os.path.join(BASE_DIR, "template")
TEMPLATE_DIR_PLURAL = os.path.join(BASE_DIR, "templates")
TEMPLATE_NAME = "template.html"
OUTPUT_PATH = os.path.join(BASE_DIR, "docs", "index.html")

REQUIRED_PROFILE_FIELDS = ["capital", "population", "gdp", "inflation", "unemployment", "min_wage"]

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
    <div class="err">build_site.py 오류 기록 ({timestamp}):
{error}</div>
  </div>
</body>
</html>
"""


def _now_display() -> str:
    try:
        if _KST is not None:
            return datetime.now(_KST).strftime("%Y-%m-%d %H:%M KST")
    except Exception:
        pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _empty_skeleton_data() -> dict:
    now_dt = datetime.now(_KST) if _KST is not None else datetime.now(timezone.utc)
    date_str = now_dt.strftime("%Y-%m-%d")
    return {
        "generated_at": now_dt.isoformat(),
        "generated_at_display": _now_display(),
        "as_of_display": f"{date_str} 06:00 KST 기준 (데이터 없음)",
        "regions": [{"key": "ALL", "label": "전체"}],
        "countries": [],
    }


def load_data() -> dict:
    if not os.path.exists(DATA_PATH):
        msg = f"[build_site] {DATA_PATH} 파일이 존재하지 않습니다. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.warning(msg)
        return _empty_skeleton_data()

    try:
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            raw = f.read()
    except Exception:
        print(f"[build_site] {DATA_PATH} 파일을 읽는 중 오류 발생. 빈 골격 데이터로 진행합니다.")
        print(traceback.format_exc())
        log.error(f"{DATA_PATH} 파일 읽기 실패")
        return _empty_skeleton_data()

    if not raw or not raw.strip():
        msg = f"[build_site] {DATA_PATH} 파일이 비어 있습니다. 빈 골격 데이터로 진행합니다."
        print(msg)
        log.warning(msg)
        return _empty_skeleton_data()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[build_site] {DATA_PATH} JSON 파싱 실패. 빈 골격 데이터로 진행합니다.")
        print(traceback.format_exc())
        log.error(f"{DATA_PATH} JSON 파싱 실패")
        return _empty_skeleton_data()
    except Exception:
        print(f"[build_site] {DATA_PATH} 로드 중 예기치 못한 오류. 빈 골격 데이터로 진행합니다.")
        print(traceback.format_exc())
        log.error(f"{DATA_PATH} 로드 중 예기치 못한 오류")
        return _empty_skeleton_data()

    if not isinstance(data, dict):
        msg = f"[build_site] {DATA_PATH} 내용이 dict 형태가 아닙니다(type={type(data).__name__}). 빈 골격 데이터로 진행합니다."
        print(msg)
        log.error(msg)
        return _empty_skeleton_data()

    try:
        default_generated_at = datetime.now(_KST).isoformat() if _KST is not None else datetime.now(timezone.utc).isoformat()
    except Exception:
        default_generated_at = datetime.now(timezone.utc).isoformat()

    data.setdefault("generated_at", default_generated_at)
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

    return data


def load_archive_range(today_str: str) -> tuple:
    try:
        if os.path.exists(ARCHIVE_INDEX_PATH):
            with open(ARCHIVE_INDEX_PATH, "r", encoding="utf-8") as f:
                idx = json.load(f)
            min_date = idx.get("min_date") or today_str
            max_date = idx.get("latest") or today_str
            return min_date, max_date
        else:
            print(f"[build_site] {ARCHIVE_INDEX_PATH} 이 아직 없습니다 (최초 실행) → 오늘 날짜로 폴백합니다.")
    except Exception:
        print("[build_site] 아카이브 인덱스 로드 실패 → 오늘 날짜로 폴백합니다.")
        print(traceback.format_exc())
        log.warning("아카이브 인덱스 로드 실패 → 오늘 날짜로 폴백")
    return today_str, today_str


def _is_bad_value(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip().lower() in {"error", "err", "undefined", "null", "none", "nan", "n/a", "", "[object object]"}:
        return True
    return False


def _validate(data: dict) -> None:
    try:
        if not isinstance(data, dict):
            print(f"[build_site] _validate: data가 dict가 아닙니다(type={type(data).__name__}), 검증을 건너뜁니다.")
            return

        countries = data.get("countries", [])
        if not isinstance(countries, list):
            print("[build_site] _validate: countries가 리스트가 아닙니다, 검증을 건너뜁니다.")
            return

        if not countries:
            print("[build_site] countries 목록이 비어 있습니다 — collector.py를 먼저 실행했는지 확인하세요.")
            log.warning("countries 목록이 비어 있습니다 — collector.py를 먼저 실행했는지 확인하세요.")
            return

        for c in countries:
            try:
                if not isinstance(c, dict):
                    continue
                profile = c.get("profile", {}) or {}
                missing = [f for f in REQUIRED_PROFILE_FIELDS if not profile.get(f)]
                if missing:
                    log.warning(f"[{c.get('code')}] profile 필드 누락: {missing} — 화면에는 '-'로 표시됩니다.")

                min_wage = profile.get("min_wage")
                if isinstance(min_wage, dict):
                    if _is_bad_value(min_wage.get("display")):
                        log.warning(f"[{c.get('code')}] min_wage.display 값이 비어있거나 손상됨: {min_wage}")
                elif _is_bad_value(min_wage):
                    log.warning(f"[{c.get('code')}] min_wage 값이 비어있거나 손상됨: {min_wage!r}")

                if "exchange_rate" not in c:
                    log.warning(f"[{c.get('code')}] exchange_rate 필드 자체가 없습니다.")

                headlines = c.get("headlines", []) or []
                hr_trends = c.get("hr_trends", []) or []
                if not headlines:
                    log.warning(f"[{c.get('code')}] headlines가 비어 있습니다.")
                if not hr_trends:
                    log.warning(f"[{c.get('code')}] hr_trends가 비어 있습니다.")
            except Exception as e:
                print(f"[build_site] _validate: 국가 항목 검증 중 오류(무시하고 계속): {e}")
                continue
    except Exception as e:
        print(f"[build_site] _validate 전체 실패(무시하고 빌드 계속): {e}")


def _write_minimal_fallback(error_message: str) -> None:
    try:
        html = MINIMAL_FALLBACK_HTML_TEMPLATE.format(
            timestamp=_now_display(),
            error=str(error_message).replace("<", "&lt;").replace(">", "&gt;"),
        )
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[build_site] 최소 안내 페이지를 {OUTPUT_PATH} 에 대신 기록했습니다.")
        log.warning(f"최소 안내 페이지를 {OUTPUT_PATH} 에 대신 기록했습니다: {error_message}")
    except Exception:
        print("[build_site] FATAL: 최소 안내 페이지 기록마저 실패했습니다.")
        print(traceback.format_exc())
        raise


def _resolve_template_env() -> Environment:
    candidate_dirs = [d for d in (TEMPLATE_DIR_SINGULAR, TEMPLATE_DIR_PLURAL) if os.path.isdir(d)]
    if not candidate_dirs:
        candidate_dirs = [TEMPLATE_DIR_SINGULAR, TEMPLATE_DIR_PLURAL]
    loader = ChoiceLoader([FileSystemLoader(d) for d in candidate_dirs])
    return Environment(loader=loader, autoescape=select_autoescape(["html"]))


def build(data: dict) -> bool:
    _validate(data)

    try:
        today_str = str(data.get("generated_at", ""))[:10] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    archive_min, archive_max = load_archive_range(today_str)

    try:
        as_of_display = data.get("as_of_display") or f"{today_str} 06:00 KST 기준"
    except Exception:
        as_of_display = f"{today_str} 06:00 KST 기준"

    try:
        env = _resolve_template_env()
        template = env.get_template(TEMPLATE_NAME)
    except TemplateError as e:
        err = (
            f"Jinja2 템플릿 로딩 실패 (template.html 을 '{TEMPLATE_DIR_SINGULAR}' 와 "
            f"'{TEMPLATE_DIR_PLURAL}' 양쪽에서 찾지 못함): {e}"
        )
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        log.error(err)
        _write_minimal_fallback(err)
        return False
    except Exception as e:
        err = f"템플릿 로딩 중 예기치 못한 오류: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        log.error(err)
        _write_minimal_fallback(err)
        return False

    try:
        raw_json = json.dumps(
            {
                "as_of_display": as_of_display,
                "regions": data.get("regions", []),
                "countries": data.get("countries", []),
            },
            ensure_ascii=False,
            default=str,
        ).replace("</", "<\\/")
        dashboard_json = Markup(raw_json)
    except Exception:
        print("[build_site] ERROR: 대시보드 JSON 직렬화 실패, 빈 데이터로 대체합니다.")
        print(traceback.format_exc())
        log.error("대시보드 JSON 직렬화 실패")
        dashboard_json = Markup(
            json.dumps({"as_of_display": as_of_display, "regions": [], "countries": []}, ensure_ascii=False)
        )

    ctx = {
        "as_of_display": as_of_display,
        "generated_at_date": today_str,
        "archive_min_date": archive_min,
        "archive_max_date": archive_max,
        "dashboard_json": dashboard_json,
    }

    try:
        html = template.render(**ctx)
    except TemplateError as e:
        err = f"Jinja2 템플릿 렌더링 실패: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        log.error(err)
        _write_minimal_fallback(err)
        return False
    except Exception as e:
        err = f"템플릿 렌더링 중 예기치 못한 오류: {e}"
        print(f"[build_site] ERROR: {err}")
        print(traceback.format_exc())
        log.error(err)
        _write_minimal_fallback(err)
        return False

    try:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        print(f"[build_site] FATAL: {OUTPUT_PATH} 파일 기록 실패.")
        print(traceback.format_exc())
        log.error(f"{OUTPUT_PATH} 파일 기록 실패")
        raise

    country_count = len(data.get("countries", []))
    msg = f"생성 완료 → {OUTPUT_PATH} ({country_count}개국, 아카이브 범위 {archive_min}~{archive_max})"
    print(f"[build_site] {msg}")
    log.info(msg)
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
        log.exception("build_site.py 실행 중 처리되지 않은 예외가 발생했습니다")
        sys.exit(1)


if __name__ == "__main__":
    main()
