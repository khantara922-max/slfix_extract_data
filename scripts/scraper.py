"""
AOneRoom Trending Scraper + IMDB/TMDB ID Matcher
=================================================
GitHub Actions deployable version.

Pipeline
────────
  1. Load existing  data/all_data_part_N.json  (deduplication).
  2. Load existing  data/main_urls_list_part_N.txt  (URL deduplication).
  3. Fetch raw pages from AOneRoom API.
  4. Add every new main_url to the URL list  → save BEFORE matching.
  5. Enrich only truly-new items with TMDB / IMDB.
  6. Merge + save single output:  data/all_data_part_N.json  (≤ 2 MB each).
  7. Save  data/main_urls_list_part_N.txt  (≤ 2 MB each, unique URLs only).
  8. Update  data/index.json.

Output files
────────────
  data/all_data_part_1.json          ← single unified JSON, auto-split at 2 MB
  data/all_data_part_2.json
  ...
  data/main_urls_list_part_1.txt     ← unique main_urls, auto-split at 2 MB
  data/main_urls_list_part_2.txt
  ...
  data/index.json                    ← manifest + run stats

Each JSON item shape
────────────────────
{
  "serial_no"       : 1,
  "main_url"        : "https://sflix.film/spa/videoPlayPage/movies/classified-QQr8uu3azH2",
  "detailPath"      : "classified-QQr8uu3azH2",
  "title"           : "Classified",
  "releaseDate"     : "2024-08-22",
  "subjectId"       : "2268370771397542104",
  "imdb_id/tmdb_id" : "tt27714840/1124641",
  "genre"           : "Drama",
  "countryName"     : "United States",
  "imdbRatingValue" : "5.6",
  "imdbRatingCount" : 263,
  "url"             : "https://pbcdnw.aoneroom.com/image/..."
}
"""

import os
import re
import json
import time
import requests
from pathlib import Path
from difflib import SequenceMatcher

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
BASE_URL          = "https://h5-api.aoneroom.com/wefeed-h5api-bff/subject/trending"
TMDB_API_KEY      = os.environ.get("TMDB_API_KEY", "6fad3f86b8452ee232deb7977d7dcf58")
TMDB_SEARCH_MOVIE = "https://api.themoviedb.org/3/search/movie"
TMDB_SEARCH_TV    = "https://api.themoviedb.org/3/search/tv"
TMDB_MOVIE_DETAIL = "https://api.themoviedb.org/3/movie/{}"
TMDB_TV_DETAIL    = "https://api.themoviedb.org/3/tv/{}"
SFLIX_BASE        = "https://sflix.film/spa/videoPlayPage/movies/"

DATA_DIR          = Path(os.environ.get("DATA_DIR", "data"))
MAX_FILE_BYTES    = 2 * 1024 * 1024          # 2 MB hard cap per file
PAGES_TO_FETCH    = int(os.environ.get("PAGES_TO_FETCH", "10"))
PER_PAGE          = int(os.environ.get("PER_PAGE", "100"))

JSON_PREFIX       = "all_data"               # only JSON prefix used
URL_PREFIX        = "main_urls_list"         # only URL-list prefix used

MAIN_HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept"         : "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer"        : "https://www.aoneroom.com/",
    "Origin"         : "https://www.aoneroom.com",
}


# ══════════════════════════════════════════════
#  JSON helpers
# ══════════════════════════════════════════════

def load_json_parts(prefix: str) -> tuple[list[dict], set[str]]:
    """
    Load all  <prefix>_part_N.json  files.
    Returns (items_list, seen_subject_ids).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    items: list[dict] = []
    seen:  set[str]   = set()

    pat = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    for f in sorted(DATA_DIR.iterdir(),
                    key=lambda f: int(m.group(1)) if (m := pat.match(f.name)) else -1):
        if not pat.match(f.name):
            continue
        try:
            for it in json.loads(f.read_text(encoding="utf-8")):
                sid = it.get("subjectId", "")
                if sid and sid not in seen:
                    seen.add(sid)
                    items.append(it)
        except Exception as e:
            print(f"  [WARN] Could not load {f.name}: {e}")

    return items, seen


def save_json_parts(prefix: str, items: list[dict]) -> list[str]:
    """
    Write items to  <prefix>_part_N.json  files, each ≤ MAX_FILE_BYTES.
    Deletes old parts first.  Returns list of filenames written.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    pat = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    for f in DATA_DIR.iterdir():
        if pat.match(f.name):
            f.unlink()

    if not items:
        return []

    written: list[str] = []
    part_n   = 1
    batch:   list[dict] = []

    for item in items:
        batch.append(item)
        if len(json.dumps(batch, ensure_ascii=False, indent=2).encode()) >= MAX_FILE_BYTES:
            batch.pop()                       # remove item that tipped the scale
            _flush_json(prefix, part_n, batch, written)
            part_n += 1
            batch   = [item]                  # start new part with overflow item

    if batch:
        _flush_json(prefix, part_n, batch, written)

    return written


def _flush_json(prefix: str, part_n: int, batch: list[dict], written: list[str]):
    fname = DATA_DIR / f"{prefix}_part_{part_n}.json"
    fname.write_text(json.dumps(batch, ensure_ascii=False, indent=2), encoding="utf-8")
    kb = fname.stat().st_size / 1024
    print(f"  📄  {fname.name}  ({len(batch)} items, {kb:.1f} KB)")
    written.append(fname.name)


# ══════════════════════════════════════════════
#  URL-list helpers
# ══════════════════════════════════════════════

def load_url_parts(prefix: str) -> set[str]:
    """Return set of every URL found in  <prefix>_part_N.txt  files."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    pat = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.txt$")
    for f in sorted(DATA_DIR.iterdir(),
                    key=lambda f: int(m.group(1)) if (m := pat.match(f.name)) else -1):
        if not pat.match(f.name):
            continue
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                u = line.strip()
                if u:
                    seen.add(u)
        except Exception as e:
            print(f"  [WARN] Could not load {f.name}: {e}")
    return seen


def save_url_parts(prefix: str, urls: list[str]) -> list[str]:
    """
    Write sorted unique URL list to  <prefix>_part_N.txt  files, each ≤ MAX_FILE_BYTES.
    Deletes old parts first.  Returns list of filenames written.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    pat = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.txt$")
    for f in DATA_DIR.iterdir():
        if pat.match(f.name):
            f.unlink()

    if not urls:
        return []

    written: list[str] = []
    part_n  = 1
    lines:  list[str] = []
    cur_bytes = 0

    for url in urls:
        line  = url + "\n"
        lb    = len(line.encode())
        if cur_bytes + lb >= MAX_FILE_BYTES and lines:
            _flush_txt(prefix, part_n, lines, written)
            part_n   += 1
            lines     = []
            cur_bytes = 0
        lines.append(line)
        cur_bytes += lb

    if lines:
        _flush_txt(prefix, part_n, lines, written)

    return written


def _flush_txt(prefix: str, part_n: int, lines: list[str], written: list[str]):
    fname = DATA_DIR / f"{prefix}_part_{part_n}.txt"
    fname.write_text("".join(lines), encoding="utf-8")
    kb = fname.stat().st_size / 1024
    print(f"  🔗  {fname.name}  ({len(lines)} URLs, {kb:.1f} KB)")
    written.append(fname.name)


# ══════════════════════════════════════════════
#  AOneRoom fetch
# ══════════════════════════════════════════════

def fetch_page(page: int, per_page: int) -> list:
    try:
        r = requests.get(
            BASE_URL,
            params={"page": page, "perPage": per_page},
            headers=MAIN_HEADERS,
            timeout=15,
        )
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
#  TMDB / IMDB helpers
# ══════════════════════════════════════════════

def _sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()

def _year(date_str: str) -> str:
    return date_str[:4] if date_str else ""


def _score(cand: dict, title: str, year: str,
           imdb_val: str, imdb_cnt: int, is_tv: bool) -> float:
    ct   = cand.get("name" if is_tv else "title", "")
    cy   = _year(cand.get("first_air_date" if is_tv else "release_date", ""))
    cv   = str(cand.get("vote_average", ""))
    cc   = cand.get("vote_count", 0)

    ts = _sim(title, ct) * 50
    ys = 0.0
    if year and cy:
        d  = abs(int(year) - int(cy))
        ys = 20 if d == 0 else (10 if d == 1 else 0)
    rs = 0.0
    try:
        rs = max(0.0, 15 - abs(float(imdb_val) - float(cv)) * 5)
    except (ValueError, TypeError):
        pass
    cs = (min(imdb_cnt, cc) / max(imdb_cnt, cc) * 15) if imdb_cnt and cc else 0.0

    return ts + ys + rs + cs


def tmdb_search(title: str, year: str, imdb_val: str, imdb_cnt: int) -> dict:
    if not TMDB_API_KEY:
        return {}
    results = []
    for url, is_tv in [(TMDB_SEARCH_TV, True), (TMDB_SEARCH_MOVIE, False)]:
        params: dict = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US", "page": 1}
        if year:
            params["first_air_date_year" if is_tv else "year"] = year
        try:
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
            for c in r.json().get("results", [])[:5]:
                results.append((_score(c, title, year, imdb_val, imdb_cnt, is_tv), c, is_tv))
            time.sleep(0.25)
        except Exception:
            pass
    if not results:
        return {}
    results.sort(key=lambda x: x[0], reverse=True)
    sc, best, is_tv = results[0]
    best["_score"] = round(sc, 2)
    best["_is_tv"] = is_tv
    return best


def get_imdb_id(tmdb_id: int, is_tv: bool) -> str:
    if not TMDB_API_KEY:
        return ""
    url = (TMDB_TV_DETAIL if is_tv else TMDB_MOVIE_DETAIL).format(tmdb_id) + "/external_ids"
    try:
        r = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=10)
        r.raise_for_status()
        return r.json().get("imdb_id", "")
    except Exception:
        return ""


# ══════════════════════════════════════════════
#  Build final item (post-match)
# ══════════════════════════════════════════════

def build_item(serial_no: int, subject: dict) -> dict:
    title    = subject.get("title", "")
    date     = subject.get("releaseDate", "")
    imdb_val = subject.get("imdbRatingValue", "")
    imdb_cnt = subject.get("imdbRatingCount", 0)
    detail   = subject.get("detailPath", "")
    cover    = subject.get("cover", {}).get("url", "") if subject.get("cover") else ""

    match   = tmdb_search(title, _year(date), imdb_val, imdb_cnt)
    tmdb_id = match.get("id", "")
    is_tv   = match.get("_is_tv", False)
    imdb_id = get_imdb_id(tmdb_id, is_tv) if tmdb_id else ""
    comb_id = f"{imdb_id}/{tmdb_id}" if (imdb_id or tmdb_id) else "N/A"

    return {
        "serial_no"      : serial_no,
        "main_url"       : f"{SFLIX_BASE}{detail}",
        "detailPath"     : detail,
        "title"          : title,
        "releaseDate"    : date,
        "subjectId"      : subject.get("subjectId", ""),
        "imdb_id/tmdb_id": comb_id,
        "genre"          : subject.get("genre", ""),
        "countryName"    : subject.get("countryName", ""),
        "imdbRatingValue": imdb_val,
        "imdbRatingCount": imdb_cnt,
        "url"            : cover,
    }


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  AOneRoom Trending Scraper + TMDB/IMDB Enricher")
    print(f"  Pages: 1 → {PAGES_TO_FETCH}  |  Per page: {PER_PAGE}")
    print(f"  Data dir : {DATA_DIR.resolve()}")
    print(f"  TMDB key : {'✓ set' if TMDB_API_KEY else '✗ NOT SET'}")
    print("=" * 60)

    # ── 1. Load existing JSON data ─────────────────────────────
    print("\n→ Loading existing data …")
    existing_items, seen_ids = load_json_parts(JSON_PREFIX)
    print(f"  Loaded {len(existing_items)} items, {len(seen_ids)} unique subjectIds")

    # ── 2. Load existing URL list ──────────────────────────────
    print("\n→ Loading existing URL list …")
    known_urls: set[str] = load_url_parts(URL_PREFIX)

    # Back-fill URLs from existing JSON items (older runs may predate url list)
    for it in existing_items:
        mu = it.get("main_url", "")
        if mu:
            known_urls.add(mu)
    print(f"  Known URLs: {len(known_urls)}")

    # ── 3. Fetch pages ─────────────────────────────────────────
    print(f"\n→ Fetching {PAGES_TO_FETCH} pages …")
    raw_subjects: list[dict] = []
    for page in range(1, PAGES_TO_FETCH + 1):
        raw_subjects.extend(fetch_page(page, PER_PAGE))
        if page < PAGES_TO_FETCH:
            time.sleep(0.4)

    # ── 4. Collect new main_urls (PRE-MATCH) ───────────────────
    print("\n→ Collecting main_urls before matching …")
    new_urls_added = 0
    for s in raw_subjects:
        detail = s.get("detailPath", "")
        if detail:
            url = f"{SFLIX_BASE}{detail}"
            if url not in known_urls:
                known_urls.add(url)
                new_urls_added += 1

    all_urls_sorted = sorted(known_urls)
    print(f"  New URLs this run : {new_urls_added}")
    print(f"  Total unique URLs : {len(all_urls_sorted)}")

    # ── 5. Save URL list NOW (before any TMDB calls) ──────────
    print("\n→ Saving main_urls_list (pre-match) …")
    url_parts = save_url_parts(URL_PREFIX, all_urls_sorted)

    # ── 6. Determine which subjects are truly new ──────────────
    seen_this_run: set[str]  = set()
    unique_new:    list[dict] = []
    for s in raw_subjects:
        sid = s.get("subjectId", "")
        if sid and sid not in seen_ids and sid not in seen_this_run:
            seen_this_run.add(sid)
            unique_new.append(s)

    print(f"\n  Raw fetched    : {len(raw_subjects)}")
    print(f"  Already known  : {len(raw_subjects) - len(unique_new)}")
    print(f"  New to enrich  : {len(unique_new)}")

    if not unique_new:
        print("\n  Nothing new — updating index and exiting.")
        _write_index(len(existing_items), 0, len(all_urls_sorted), url_parts, [])
        return

    # ── 7. Enrich new items (TMDB / IMDB) ─────────────────────
    print("\n→ Enriching new items with TMDB/IMDB …")
    new_items: list[dict] = []
    base_serial = len(existing_items) + 1

    for i, subject in enumerate(unique_new, start=base_serial):
        title = subject.get("title", "")
        print(f"  [{i - base_serial + 1}/{len(unique_new)}] {title}")
        new_items.append(build_item(i, subject))
        time.sleep(0.3)

    # ── 8. Merge (dedup by subjectId) ─────────────────────────
    seen_merged: set[str]  = set()
    merged:      list[dict] = []
    for it in existing_items + new_items:
        sid = it.get("subjectId") or it.get("title", "")
        if sid and sid not in seen_merged:
            seen_merged.add(sid)
            merged.append(it)

    # Re-number serially
    for idx, it in enumerate(merged, start=1):
        it["serial_no"] = idx

    # ── 9. Save single unified JSON ────────────────────────────
    print("\n→ Saving all_data JSON parts …")
    json_parts = save_json_parts(JSON_PREFIX, merged)

    # ── 10. Update index ───────────────────────────────────────
    _write_index(len(merged), len(new_items), len(all_urls_sorted), url_parts, json_parts)

    print("\n" + "=" * 60)
    print(f"  Done!")
    print(f"  New this run  : {len(new_items)}")
    print(f"  Total items   : {len(merged)}  →  {len(json_parts)} JSON part(s)")
    print(f"  Unique URLs   : {len(all_urls_sorted)}  →  {len(url_parts)} URL file(s)")
    print("=" * 60)


def _write_index(total: int, new: int, total_urls: int,
                 url_parts: list[str], json_parts: list[str]):
    stats = {
        "last_run"          : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_items"       : total,
        "new_this_run"      : new,
        "pages_fetched"     : PAGES_TO_FETCH,
        "max_file_size_mb"  : MAX_FILE_BYTES / (1024 * 1024),
        "total_unique_urls" : total_urls,
        "parts": {
            JSON_PREFIX : json_parts,
            URL_PREFIX  : url_parts,
        },
    }
    path = DATA_DIR / "index.json"
    path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print("  📋  index.json updated")


if __name__ == "__main__":
    main()
