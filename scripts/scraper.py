"""
AOneRoom Trending Scraper + IMDB/TMDB ID Matcher
=================================================
GitHub Actions deployable version.

Outputs (in ./data/):
  - entertainment_data_part_N.json       → all items (split at 2 MB)
  - 100percent_matching_part_N.json      → score ≥ 90
  - 100percentornearmatching_part_N.json → score 70–89
  - index.json                           → manifest of all parts + stats

Deduplication:
  - Loads ALL existing JSON parts on startup
  - Skips any subjectId already present
  - Appends new items to the correct part (or creates new parts as needed)
"""

import os
import re
import sys
import json
import math
import time
import requests
from pathlib import Path
from difflib import SequenceMatcher

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
BASE_URL              = "https://h5-api.aoneroom.com/wefeed-h5api-bff/subject/trending"
TMDB_API_KEY          = os.environ.get("6fad3f86b8452ee232deb7977d7dcf58", "")
TMDB_SEARCH_MOVIE     = "https://api.themoviedb.org/3/search/movie"
TMDB_SEARCH_TV        = "https://api.themoviedb.org/3/search/tv"
TMDB_MOVIE_DETAIL     = "https://api.themoviedb.org/3/movie/{}"
TMDB_TV_DETAIL        = "https://api.themoviedb.org/3/tv/{}"
SFLIX_BASE            = "https://sflix.film/spa/videoPlayPage/movies/"

DATA_DIR              = Path(os.environ.get("DATA_DIR", "data"))
MAX_FILE_BYTES        = 2 * 1024 * 1024   # 2 MB hard cap per file
PAGES_TO_FETCH        = int(os.environ.get("PAGES_TO_FETCH", "10"))
PER_PAGE              = int(os.environ.get("PER_PAGE", "100"))

MAIN_HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept"         : "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer"        : "https://www.aoneroom.com/",
    "Origin"         : "https://www.aoneroom.com",
}

# ──────────────────────────────────────────────
# HELPERS – JSON splitting / loading
# ──────────────────────────────────────────────

def load_existing_parts(prefix: str) -> tuple[list[dict], set[str]]:
    """
    Load all *_part_N.json files matching prefix.
    Returns (all_items, seen_ids).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    all_items: list[dict] = []
    seen_ids: set[str]    = set()

    pattern = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    parts = sorted(
        (f for f in DATA_DIR.iterdir() if pattern.match(f.name)),
        key=lambda f: int(pattern.match(f.name).group(1))
    )

    for part_file in parts:
        try:
            items = json.loads(part_file.read_text(encoding="utf-8"))
            for it in items:
                sid = it.get("subjectId", "")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    all_items.append(it)
        except Exception as e:
            print(f"  [WARN] Could not load {part_file.name}: {e}")

    return all_items, seen_ids


def save_split_parts(prefix: str, items: list[dict]) -> list[str]:
    """
    Write items to numbered part files, each ≤ MAX_FILE_BYTES.
    Old parts are fully replaced (we always rewrite from scratch to avoid drift).
    Returns list of filenames written.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Remove old parts for this prefix
    old_pattern = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    for f in DATA_DIR.iterdir():
        if old_pattern.match(f.name):
            f.unlink()

    if not items:
        return []

    written: list[str] = []
    part_num    = 1
    part_items: list[dict] = []

    for item in items:
        part_items.append(item)
        # Estimate size
        estimated = len(json.dumps(part_items, ensure_ascii=False, indent=2).encode("utf-8"))
        if estimated >= MAX_FILE_BYTES:
            # Save current batch (without the item that pushed it over)
            part_items.pop()
            fname = DATA_DIR / f"{prefix}_part_{part_num}.json"
            fname.write_text(json.dumps(part_items, ensure_ascii=False, indent=2), encoding="utf-8")
            actual_kb = fname.stat().st_size / 1024
            print(f"  📄  {fname.name}  ({len(part_items)} items, {actual_kb:.1f} KB)")
            written.append(fname.name)
            part_num  += 1
            part_items = [item]   # start fresh part with the overflow item

    if part_items:
        fname = DATA_DIR / f"{prefix}_part_{part_num}.json"
        fname.write_text(json.dumps(part_items, ensure_ascii=False, indent=2), encoding="utf-8")
        actual_kb = fname.stat().st_size / 1024
        print(f"  📄  {fname.name}  ({len(part_items)} items, {actual_kb:.1f} KB)")
        written.append(fname.name)

    return written


def save_index(stats: dict):
    """Write data/index.json with run stats and part manifests."""
    index_path = DATA_DIR / "index.json"
    index_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  📋  index.json updated")


# ──────────────────────────────────────────────
# STEP 1 – Fetch AOneRoom pages
# ──────────────────────────────────────────────

def fetch_page(page: int, per_page: int = 100) -> list:
    params = {"page": page, "perPage": per_page}
    try:
        r = requests.get(BASE_URL, params=params, headers=MAIN_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            print(f"  [!] Page {page}: API error code={data.get('code')}")
            return []
        subjects = data.get("data", {}).get("subjectList", [])
        print(f"  [✓] Page {page}: {len(subjects)} items")
        return subjects
    except Exception as e:
        print(f"  [✗] Page {page} failed: {e}")
        return []


# ──────────────────────────────────────────────
# STEP 2 – TMDB helpers
# ──────────────────────────────────────────────

def str_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def year_from_date(date_str: str) -> str:
    return date_str[:4] if date_str else ""


def score_candidate(candidate: dict, title: str, year: str,
                    imdb_val: str, imdb_cnt: int, is_tv: bool) -> float:
    cand_title = candidate.get("name" if is_tv else "title", "")
    cand_date  = candidate.get("first_air_date" if is_tv else "release_date", "")
    cand_year  = year_from_date(cand_date)
    cand_vote  = str(candidate.get("vote_average", ""))
    cand_cnt   = candidate.get("vote_count", 0)

    # Title (0–50)
    t_score = str_sim(title, cand_title) * 50

    # Year (0–20)
    y_score = 0.0
    if year and cand_year:
        diff    = abs(int(year) - int(cand_year))
        y_score = 20 if diff == 0 else (10 if diff == 1 else 0)

    # Rating value (0–15)
    r_score = 0.0
    try:
        diff_r  = abs(float(imdb_val) - float(cand_vote))
        r_score = max(0.0, 15 - diff_r * 5)
    except (ValueError, TypeError):
        pass

    # Rating count (0–15) log-scale
    c_score = 0.0
    if imdb_cnt and cand_cnt:
        ratio   = min(imdb_cnt, cand_cnt) / max(imdb_cnt, cand_cnt)
        c_score = ratio * 15

    return t_score + y_score + r_score + c_score


def tmdb_search(title: str, year: str, imdb_val: str,
                imdb_cnt: int, genre_str: str) -> dict:
    if not TMDB_API_KEY:
        return {}

    results = []
    for search_url, is_tv in [(TMDB_SEARCH_TV, True), (TMDB_SEARCH_MOVIE, False)]:
        params: dict = {
            "api_key" : TMDB_API_KEY,
            "query"   : title,
            "language": "en-US",
            "page"    : 1,
        }
        if year:
            params["first_air_date_year" if is_tv else "year"] = year
        try:
            r = requests.get(search_url, params=params, timeout=10)
            r.raise_for_status()
            for c in r.json().get("results", [])[:5]:
                sc = score_candidate(c, title, year, imdb_val, imdb_cnt, is_tv)
                results.append((sc, c, is_tv))
            time.sleep(0.25)
        except Exception:
            pass

    if not results:
        return {}

    results.sort(key=lambda x: x[0], reverse=True)
    best_score, best_cand, best_is_tv = results[0]
    best_cand["_match_score"] = round(best_score, 2)
    best_cand["_is_tv"]       = best_is_tv
    return best_cand


def get_imdb_id(tmdb_id: int, is_tv: bool) -> str:
    if not TMDB_API_KEY:
        return ""
    detail_url = TMDB_TV_DETAIL if is_tv else TMDB_MOVIE_DETAIL
    ext_url    = detail_url.format(tmdb_id) + "/external_ids"
    try:
        r = requests.get(ext_url, params={"api_key": TMDB_API_KEY}, timeout=10)
        r.raise_for_status()
        return r.json().get("imdb_id", "")
    except Exception:
        return ""


# ──────────────────────────────────────────────
# STEP 3 – Build enriched item
# ──────────────────────────────────────────────

def build_item(serial_no: int, subject: dict) -> dict:
    title     = subject.get("title", "")
    date      = subject.get("releaseDate", "")
    imdb_val  = subject.get("imdbRatingValue", "")
    imdb_cnt  = subject.get("imdbRatingCount", 0)
    genre     = subject.get("genre", "")
    detail    = subject.get("detailPath", "")
    cover_url = subject.get("cover", {}).get("url", "") if subject.get("cover") else ""

    year  = year_from_date(date)
    match = tmdb_search(title, year, imdb_val, imdb_cnt, genre)

    tmdb_id     = match.get("id", "")
    is_tv       = match.get("_is_tv", False)
    score       = match.get("_match_score", 0)
    imdb_id     = get_imdb_id(tmdb_id, is_tv) if tmdb_id else ""
    combined_id = f"{imdb_id}/{tmdb_id}" if (imdb_id or tmdb_id) else "N/A"

    return {
        "serial_no"        : serial_no,
        "main_url"         : f"{SFLIX_BASE}{detail}",
        "detailPath"       : detail,
        "title"            : title,
        "releaseDate"      : date,
        "subjectId"        : subject.get("subjectId", ""),
        "imdb_id/tmdb_id"  : combined_id,
        "genre"            : genre,
        "countryName"      : subject.get("countryName", ""),
        "imdbRatingValue"  : imdb_val,
        "imdbRatingCount"  : imdb_cnt,
        "url"              : cover_url,
        "_match_score"     : score,
    }


def clean(lst: list[dict]) -> list[dict]:
    return [{k: v for k, v in it.items() if k != "_match_score"} for it in lst]


# ──────────────────────────────────────────────
# STEP 4 – Main
# ──────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  AOneRoom Trending Scraper + TMDB/IMDB Enricher")
    print(f"  Pages: 1 → {PAGES_TO_FETCH}  |  Per page: {PER_PAGE}")
    print(f"  Data dir : {DATA_DIR.resolve()}")
    print(f"  TMDB key : {'✓ set' if TMDB_API_KEY else '✗ NOT SET — IDs will be empty'}")
    print("=" * 60)

    if not TMDB_API_KEY:
        print("\n[ERROR] TMDB_API_KEY environment variable is not set.")
        print("  Add it as a GitHub Actions secret and reference it in the workflow.")

    # ── 4a. Load existing data (deduplication) ──
    print("\n→ Loading existing data for deduplication …")
    existing_all,    seen_all    = load_existing_parts("entertainment_data")
    existing_perfect, seen_perf  = load_existing_parts("100percent_matching")
    existing_near,   seen_near   = load_existing_parts("100percentornearmatching")

    # Unified seen set (subjectIds already stored anywhere)
    seen_global: set[str] = set()
    for it in existing_all:
        sid = it.get("subjectId", "")
        if sid:
            seen_global.add(sid)

    print(f"  Already stored: {len(existing_all)} unique items")

    # ── 4b. Fetch new pages ──
    print(f"\n→ Fetching {PAGES_TO_FETCH} pages …")
    raw_subjects: list[dict] = []
    for page in range(1, PAGES_TO_FETCH + 1):
        print(f"  Page {page}/{PAGES_TO_FETCH} …")
        raw_subjects.extend(fetch_page(page, PER_PAGE))
        if page < PAGES_TO_FETCH:
            time.sleep(0.4)

    # Deduplicate source list by subjectId
    seen_this_run: set[str] = set()
    unique_new: list[dict]  = []
    for s in raw_subjects:
        sid = s.get("subjectId", "")
        if sid and sid not in seen_global and sid not in seen_this_run:
            seen_this_run.add(sid)
            unique_new.append(s)

    print(f"\n  Fetched       : {len(raw_subjects)} raw")
    print(f"  Already known : {len(raw_subjects) - len(unique_new)}")
    print(f"  New to enrich : {len(unique_new)}")

    if not unique_new:
        print("\n  Nothing new to process. Exiting.")
        # Still rewrite index to reflect current state
        save_index({
            "last_run"         : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_all"        : len(existing_all),
            "total_perfect"    : len(existing_perfect),
            "total_near"       : len(existing_near),
            "new_this_run"     : 0,
            "pages_fetched"    : PAGES_TO_FETCH,
        })
        return

    # ── 4c. Enrich new items ──
    print("\n→ Enriching new items with TMDB/IMDB …")
    new_all, new_perfect, new_near = [], [], []
    base_serial = len(existing_all) + 1

    for i, subject in enumerate(unique_new, start=base_serial):
        title = subject.get("title", "")
        print(f"  [{i - base_serial + 1}/{len(unique_new)}] {title}")
        item  = build_item(i, subject)
        new_all.append(item)

        sc = item["_match_score"]
        if sc >= 90:
            new_perfect.append(item)
        elif sc >= 70:
            new_near.append(item)

        time.sleep(0.3)

    # ── 4d. Merge and deduplicate before saving ──
    def merge_unique(existing: list[dict], additions: list[dict]) -> list[dict]:
        seen: set[str] = set()
        merged: list[dict] = []
        for it in existing + additions:
            sid = it.get("subjectId", "")
            key = sid if sid else it.get("title", "")
            if key and key not in seen:
                seen.add(key)
                merged.append(it)
        return merged

    all_items     = merge_unique(existing_all,     clean(new_all))
    all_perfect   = merge_unique(existing_perfect, clean(new_perfect))
    all_near      = merge_unique(existing_near,    clean(new_near))

    # Renumber serial_no sequentially after merge
    for idx, it in enumerate(all_items, start=1):
        it["serial_no"] = idx

    # ── 4e. Save split parts ──
    print("\n→ Saving files …")
    parts_all     = save_split_parts("entertainment_data",         all_items)
    parts_perfect = save_split_parts("100percent_matching",        all_perfect)
    parts_near    = save_split_parts("100percentornearmatching",   all_near)

    # ── 4f. Save index ──
    save_index({
        "last_run"           : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_all"          : len(all_items),
        "total_perfect"      : len(all_perfect),
        "total_near"         : len(all_near),
        "new_this_run"       : len(new_all),
        "pages_fetched"      : PAGES_TO_FETCH,
        "max_file_size_mb"   : MAX_FILE_BYTES / (1024 * 1024),
        "parts": {
            "entertainment_data"        : parts_all,
            "100percent_matching"       : parts_perfect,
            "100percentornearmatching"  : parts_near,
        },
    })

    print("\n" + "=" * 60)
    print(f"  Done!")
    print(f"  New this run : {len(new_all)}")
    print(f"  Total all    : {len(all_items)}  →  {len(parts_all)} part(s)")
    print(f"  Total 100%   : {len(all_perfect)}  →  {len(parts_perfect)} part(s)")
    print(f"  Total near   : {len(all_near)}  →  {len(parts_near)} part(s)")
    print("=" * 60)


if __name__ == "__main__":
    main()
