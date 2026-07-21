"""
AOneRoom Trending Scraper + IMDB/TMDB ID Matcher
=================================================
GitHub Actions deployable version.

Pipeline order
──────────────
  1. Load existing data (deduplication).
  2. Fetch raw pages from AOneRoom API.
  3. Store raw URLs immediately → main_urls_list.txt  (≤ 2 MB per file, auto-split).
  4. Store raw (pre-match) items → raw_fetched_part_N.json  (≤ 2 MB, auto-split).
  5. Enrich new items with TMDB / IMDB.
  6. Merge all three result buckets into one unified set of split files:
         data/all_data_part_N.json  (≤ 2 MB each)
     with per-item  "_bucket"  field so you can filter later:
         "perfect"  → score ≥ 90
         "near"     → score 70–89
         "other"    → score < 70
  7. Write index.json + main_urls_list.txt parts manifest.

Legacy separate bucket files are still written for backward compatibility:
  data/entertainment_data_part_N.json
  data/100percent_matching_part_N.json
  data/100percentornearmatching_part_N.json
"""

import os
import re
import sys
import json
import time
import requests
from pathlib import Path
from difflib import SequenceMatcher

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
BASE_URL              = "https://h5-api.aoneroom.com/wefeed-h5api-bff/subject/trending"
TMDB_API_KEY          = os.environ.get("TMDB_API_KEY", "6fad3f86b8452ee232deb7977d7dcf58")
TMDB_SEARCH_MOVIE     = "https://api.themoviedb.org/3/search/movie"
TMDB_SEARCH_TV        = "https://api.themoviedb.org/3/search/tv"
TMDB_MOVIE_DETAIL     = "https://api.themoviedb.org/3/movie/{}"
TMDB_TV_DETAIL        = "https://api.themoviedb.org/3/tv/{}"
SFLIX_BASE            = "https://sflix.film/spa/videoPlayPage/movies/"

DATA_DIR              = Path(os.environ.get("DATA_DIR", "data"))
MAX_FILE_BYTES        = 2 * 1024 * 1024   # 2 MB hard cap per file
PAGES_TO_FETCH        = int(os.environ.get("PAGES_TO_FETCH", "10"))
PER_PAGE              = int(os.environ.get("PER_PAGE", "100"))

# Prefix for the unified merged output (all buckets in one set)
UNIFIED_PREFIX        = "all_data"
# Prefix for raw (pre-match) snapshot
RAW_PREFIX            = "raw_fetched"
# URL list base name (without _part_N.txt suffix)
URL_LIST_BASE         = "main_urls_list"

MAIN_HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept"         : "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer"        : "https://www.aoneroom.com/",
    "Origin"         : "https://www.aoneroom.com",
}


# ══════════════════════════════════════════════
#  HELPERS – JSON split / load
# ══════════════════════════════════════════════

def load_existing_parts(prefix: str) -> tuple[list[dict], set[str]]:
    """
    Load all *_part_N.json files matching prefix.
    Returns (all_items, seen_subject_ids).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    all_items: list[dict] = []
    seen_ids:  set[str]   = set()

    pattern = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    parts = sorted(
        (f for f in DATA_DIR.iterdir() if pattern.match(f.name)),
        key=lambda f: int(pattern.match(f.name).group(1)),
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
    Fully replaces old parts for this prefix.
    Returns list of filenames written.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    old_pattern = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    for f in DATA_DIR.iterdir():
        if old_pattern.match(f.name):
            f.unlink()

    if not items:
        return []

    written:    list[str]  = []
    part_num    = 1
    part_items: list[dict] = []

    for item in items:
        part_items.append(item)
        estimated = len(json.dumps(part_items, ensure_ascii=False, indent=2).encode("utf-8"))
        if estimated >= MAX_FILE_BYTES:
            part_items.pop()
            fname = DATA_DIR / f"{prefix}_part_{part_num}.json"
            fname.write_text(
                json.dumps(part_items, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            actual_kb = fname.stat().st_size / 1024
            print(f"  📄  {fname.name}  ({len(part_items)} items, {actual_kb:.1f} KB)")
            written.append(fname.name)
            part_num  += 1
            part_items = [item]

    if part_items:
        fname = DATA_DIR / f"{prefix}_part_{part_num}.json"
        fname.write_text(
            json.dumps(part_items, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        actual_kb = fname.stat().st_size / 1024
        print(f"  📄  {fname.name}  ({len(part_items)} items, {actual_kb:.1f} KB)")
        written.append(fname.name)

    return written


def save_index(stats: dict):
    """Write data/index.json with run stats and part manifests."""
    index_path = DATA_DIR / "index.json"
    index_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print("  📋  index.json updated")


# ══════════════════════════════════════════════
#  HELPERS – URL list (main_urls_list.txt)
# ══════════════════════════════════════════════

def load_existing_urls() -> set[str]:
    """
    Read all main_urls_list_part_N.txt files and return the set of unique URLs.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    pattern = re.compile(rf"^{re.escape(URL_LIST_BASE)}_part_(\d+)\.txt$")
    parts = sorted(
        (f for f in DATA_DIR.iterdir() if pattern.match(f.name)),
        key=lambda f: int(pattern.match(f.name).group(1)),
    )
    for part_file in parts:
        try:
            for line in part_file.read_text(encoding="utf-8").splitlines():
                url = line.strip()
                if url:
                    seen.add(url)
        except Exception as e:
            print(f"  [WARN] Could not load {part_file.name}: {e}")
    return seen


def save_url_list(all_urls: list[str]) -> list[str]:
    """
    Write the full de-duplicated URL list to main_urls_list_part_N.txt files.
    Each file ≤ MAX_FILE_BYTES.  Fully replaces old parts.
    Returns list of filenames written.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    old_pattern = re.compile(rf"^{re.escape(URL_LIST_BASE)}_part_(\d+)\.txt$")
    for f in DATA_DIR.iterdir():
        if old_pattern.match(f.name):
            f.unlink()

    if not all_urls:
        return []

    written:       list[str]  = []
    part_num       = 1
    part_lines:    list[str]  = []
    current_bytes  = 0

    for url in all_urls:
        line       = url + "\n"
        line_bytes = len(line.encode("utf-8"))

        if current_bytes + line_bytes >= MAX_FILE_BYTES and part_lines:
            fname = DATA_DIR / f"{URL_LIST_BASE}_part_{part_num}.txt"
            fname.write_text("".join(part_lines), encoding="utf-8")
            actual_kb = fname.stat().st_size / 1024
            print(f"  🔗  {fname.name}  ({len(part_lines)} URLs, {actual_kb:.1f} KB)")
            written.append(fname.name)
            part_num      += 1
            part_lines     = []
            current_bytes  = 0

        part_lines.append(line)
        current_bytes += line_bytes

    if part_lines:
        fname = DATA_DIR / f"{URL_LIST_BASE}_part_{part_num}.txt"
        fname.write_text("".join(part_lines), encoding="utf-8")
        actual_kb = fname.stat().st_size / 1024
        print(f"  🔗  {fname.name}  ({len(part_lines)} URLs, {actual_kb:.1f} KB)")
        written.append(fname.name)

    return written


# ══════════════════════════════════════════════
#  STEP 1 – Fetch AOneRoom pages
# ══════════════════════════════════════════════

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


# ══════════════════════════════════════════════
#  STEP 2 – TMDB helpers
# ══════════════════════════════════════════════

def str_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def year_from_date(date_str: str) -> str:
    return date_str[:4] if date_str else ""


def score_candidate(
    candidate: dict, title: str, year: str,
    imdb_val: str, imdb_cnt: int, is_tv: bool,
) -> float:
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


def tmdb_search(
    title: str, year: str, imdb_val: str,
    imdb_cnt: int, genre_str: str,
) -> dict:
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


# ══════════════════════════════════════════════
#  STEP 3 – Build raw item (pre-match snapshot)
# ══════════════════════════════════════════════

def build_raw_item(serial_no: int, subject: dict) -> dict:
    """
    Lightweight item built from the raw API response — no TMDB calls.
    Stored in raw_fetched_part_N.json BEFORE any enrichment.
    """
    detail    = subject.get("detailPath", "")
    cover_url = subject.get("cover", {}).get("url", "") if subject.get("cover") else ""
    return {
        "serial_no"      : serial_no,
        "main_url"       : f"{SFLIX_BASE}{detail}",
        "detailPath"     : detail,
        "title"          : subject.get("title", ""),
        "releaseDate"    : subject.get("releaseDate", ""),
        "subjectId"      : subject.get("subjectId", ""),
        "genre"          : subject.get("genre", ""),
        "countryName"    : subject.get("countryName", ""),
        "imdbRatingValue": subject.get("imdbRatingValue", ""),
        "imdbRatingCount": subject.get("imdbRatingCount", 0),
        "url"            : cover_url,
    }


# ══════════════════════════════════════════════
#  STEP 4 – Build enriched item (post-match)
# ══════════════════════════════════════════════

def build_enriched_item(serial_no: int, subject: dict) -> dict:
    """
    Full enrichment: calls TMDB, then fetches external IMDB id.
    Returns item dict with _match_score and _bucket fields.
    """
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

    bucket = "perfect" if score >= 90 else ("near" if score >= 70 else "other")

    return {
        "serial_no"      : serial_no,
        "main_url"       : f"{SFLIX_BASE}{detail}",
        "detailPath"     : detail,
        "title"          : title,
        "releaseDate"    : date,
        "subjectId"      : subject.get("subjectId", ""),
        "imdb_id/tmdb_id": combined_id,
        "genre"          : genre,
        "countryName"    : subject.get("countryName", ""),
        "imdbRatingValue": imdb_val,
        "imdbRatingCount": imdb_cnt,
        "url"            : cover_url,
        "_match_score"   : score,
        "_bucket"        : bucket,
    }


def clean_score(lst: list[dict]) -> list[dict]:
    """Remove internal _match_score key before saving."""
    return [{k: v for k, v in it.items() if k != "_match_score"} for it in lst]


# ══════════════════════════════════════════════
#  MERGE HELPER
# ══════════════════════════════════════════════

def merge_unique(existing: list[dict], additions: list[dict]) -> list[dict]:
    seen:   set[str]  = set()
    merged: list[dict] = []
    for it in existing + additions:
        sid = it.get("subjectId", "")
        key = sid if sid else it.get("title", "")
        if key and key not in seen:
            seen.add(key)
            merged.append(it)
    return merged


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  AOneRoom Trending Scraper + TMDB/IMDB Enricher")
    print(f"  Pages: 1 → {PAGES_TO_FETCH}  |  Per page: {PER_PAGE}")
    print(f"  Data dir : {DATA_DIR.resolve()}")
    print(f"  TMDB key : {'✓ set' if TMDB_API_KEY else '✗ NOT SET — IDs will be empty'}")
    print("=" * 60)

    # ── 1. Load existing data (deduplication) ──────────────────
    print("\n→ Loading existing data for deduplication …")

    existing_all,    seen_all    = load_existing_parts("entertainment_data")
    existing_perfect, _          = load_existing_parts("100percent_matching")
    existing_near,   _           = load_existing_parts("100percentornearmatching")
    existing_unified, _          = load_existing_parts(UNIFIED_PREFIX)
    existing_raw,    _           = load_existing_parts(RAW_PREFIX)

    # Unified global seen set – subjectIds already stored in entertainment_data
    # (entertainment_data is the primary store; unified may overlap)
    seen_global: set[str] = {it["subjectId"] for it in existing_all if it.get("subjectId")}

    print(f"  Already stored (all)    : {len(existing_all)}")
    print(f"  Already stored (unified): {len(existing_unified)}")
    print(f"  Already stored (raw)    : {len(existing_raw)}")

    # ── 2. Load existing URLs ──────────────────────────────────
    print("\n→ Loading existing URL list …")
    existing_url_set: set[str] = load_existing_urls()

    # Also harvest main_urls from all existing JSON parts (backfill)
    for it in existing_all + existing_unified + existing_raw:
        mu = it.get("main_url", "")
        if mu:
            existing_url_set.add(mu)

    print(f"  Known URLs so far: {len(existing_url_set)}")

    # ── 3. Fetch raw pages ────────────────────────────────────
    print(f"\n→ Fetching {PAGES_TO_FETCH} pages …")
    raw_subjects: list[dict] = []
    for page in range(1, PAGES_TO_FETCH + 1):
        print(f"  Page {page}/{PAGES_TO_FETCH} …")
        raw_subjects.extend(fetch_page(page, PER_PAGE))
        if page < PAGES_TO_FETCH:
            time.sleep(0.4)

    # Deduplicate by subjectId for enrichment pipeline
    seen_this_run: set[str]  = set()
    unique_new:    list[dict] = []
    for s in raw_subjects:
        sid = s.get("subjectId", "")
        if sid and sid not in seen_global and sid not in seen_this_run:
            seen_this_run.add(sid)
            unique_new.append(s)

    print(f"\n  Fetched raw    : {len(raw_subjects)}")
    print(f"  Already known  : {len(raw_subjects) - len(unique_new)}")
    print(f"  New to enrich  : {len(unique_new)}")

    # ── 4. Collect ALL main_urls (existing + newly fetched pages) ──
    #       Do this BEFORE any TMDB/IMDB matching.
    print("\n→ Collecting main_urls (pre-match) …")

    new_page_urls: list[str] = []
    for s in raw_subjects:
        detail = s.get("detailPath", "")
        if detail:
            url = f"{SFLIX_BASE}{detail}"
            if url not in existing_url_set:
                existing_url_set.add(url)
                new_page_urls.append(url)

    all_urls_ordered: list[str] = sorted(existing_url_set)   # stable sort
    print(f"  New URLs this run  : {len(new_page_urls)}")
    print(f"  Total unique URLs  : {len(all_urls_ordered)}")

    # ── 5. Save URL list NOW (pre-match) ──────────────────────
    print("\n→ Saving main_urls_list (pre-match) …")
    url_parts = save_url_list(all_urls_ordered)

    # ── 6. Build and save raw snapshot (pre-match JSON) ───────
    print("\n→ Saving raw (pre-match) JSON snapshot …")
    base_serial_raw = len(existing_raw) + 1
    new_raw_items: list[dict] = []
    seen_raw_ids: set[str] = {it.get("subjectId", "") for it in existing_raw}

    for i, subject in enumerate(unique_new, start=base_serial_raw):
        sid = subject.get("subjectId", "")
        if sid not in seen_raw_ids:
            seen_raw_ids.add(sid)
            new_raw_items.append(build_raw_item(i, subject))

    all_raw = merge_unique(existing_raw, new_raw_items)
    for idx, it in enumerate(all_raw, start=1):
        it["serial_no"] = idx
    parts_raw = save_split_parts(RAW_PREFIX, all_raw)

    # ── 7. Early exit if nothing new to enrich ────────────────
    if not unique_new:
        print("\n  Nothing new to enrich. Updating index and exiting.")
        save_index({
            "last_run"           : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_all"          : len(existing_all),
            "total_perfect"      : len(existing_perfect),
            "total_near"         : len(existing_near),
            "total_unified"      : len(existing_unified),
            "total_raw"          : len(all_raw),
            "new_this_run"       : 0,
            "pages_fetched"      : PAGES_TO_FETCH,
            "total_unique_urls"  : len(all_urls_ordered),
            "url_list_parts"     : url_parts,
            "parts": {
                "entertainment_data"       : [],
                "100percent_matching"      : [],
                "100percentornearmatching" : [],
                UNIFIED_PREFIX             : [],
                RAW_PREFIX                 : parts_raw,
            },
        })
        return

    # ── 8. Enrich new items with TMDB/IMDB (post-URL storage) ─
    print("\n→ Enriching new items with TMDB/IMDB …")
    new_enriched: list[dict] = []
    base_serial = len(existing_all) + 1

    for i, subject in enumerate(unique_new, start=base_serial):
        title = subject.get("title", "")
        print(f"  [{i - base_serial + 1}/{len(unique_new)}] {title}")
        item = build_enriched_item(i, subject)
        new_enriched.append(item)
        time.sleep(0.3)

    # Bucket split for backward-compat files
    new_perfect = [it for it in new_enriched if it["_match_score"] >= 90]
    new_near    = [it for it in new_enriched if 70 <= it["_match_score"] < 90]

    # ── 9. Merge into legacy buckets ──────────────────────────
    all_items   = merge_unique(existing_all,     clean_score(new_enriched))
    all_perfect = merge_unique(existing_perfect, clean_score(new_perfect))
    all_near    = merge_unique(existing_near,    clean_score(new_near))

    for idx, it in enumerate(all_items, start=1):
        it["serial_no"] = idx

    # ── 10. Merge into unified single-file-set ─────────────────
    #  Unified = entertainment_data items + any extras from perfect/near
    #  (since entertainment_data already contains everything, unified ≡ all_items
    #   but with _bucket tag preserved for filtering)
    #
    #  We rebuild from scratch so _bucket is always present.
    unified_by_sid: dict[str, dict] = {
        it["subjectId"]: it for it in existing_unified if it.get("subjectId")
    }
    for it in clean_score(new_enriched):
        sid = it.get("subjectId", "")
        if sid:
            unified_by_sid[sid] = it

    # Merge any items in all_items that have no _bucket (older runs)
    for it in all_items:
        sid = it.get("subjectId", "")
        if sid and sid not in unified_by_sid:
            entry = dict(it)
            if "_bucket" not in entry:
                entry["_bucket"] = "other"
            unified_by_sid[sid] = entry

    all_unified = list(unified_by_sid.values())
    for idx, it in enumerate(all_unified, start=1):
        it["serial_no"] = idx

    # ── 11. Save all file sets ─────────────────────────────────
    print("\n→ Saving files …")
    parts_all     = save_split_parts("entertainment_data",        all_items)
    parts_perfect = save_split_parts("100percent_matching",       all_perfect)
    parts_near    = save_split_parts("100percentornearmatching",  all_near)
    parts_unified = save_split_parts(UNIFIED_PREFIX,              all_unified)

    # ── 12. Save index ─────────────────────────────────────────
    save_index({
        "last_run"           : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_all"          : len(all_items),
        "total_perfect"      : len(all_perfect),
        "total_near"         : len(all_near),
        "total_unified"      : len(all_unified),
        "total_raw"          : len(all_raw),
        "new_this_run"       : len(new_enriched),
        "pages_fetched"      : PAGES_TO_FETCH,
        "max_file_size_mb"   : MAX_FILE_BYTES / (1024 * 1024),
        "total_unique_urls"  : len(all_urls_ordered),
        "url_list_parts"     : url_parts,
        "parts": {
            "entertainment_data"       : parts_all,
            "100percent_matching"      : parts_perfect,
            "100percentornearmatching" : parts_near,
            UNIFIED_PREFIX             : parts_unified,
            RAW_PREFIX                 : parts_raw,
        },
    })

    print("\n" + "=" * 60)
    print(f"  Done!")
    print(f"  New this run     : {len(new_enriched)}")
    print(f"  Total all        : {len(all_items)}  →  {len(parts_all)} part(s)")
    print(f"  Total 100%       : {len(all_perfect)}  →  {len(parts_perfect)} part(s)")
    print(f"  Total near       : {len(all_near)}  →  {len(parts_near)} part(s)")
    print(f"  Total unified    : {len(all_unified)}  →  {len(parts_unified)} part(s)")
    print(f"  Total raw        : {len(all_raw)}  →  {len(parts_raw)} part(s)")
    print(f"  Unique URLs      : {len(all_urls_ordered)}  →  {len(url_parts)} URL file(s)")
    print("=" * 60)


if __name__ == "__main__":
    main()
