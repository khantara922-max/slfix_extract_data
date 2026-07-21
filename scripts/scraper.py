"""
AOneRoom Trending Scraper + IMDB/TMDB ID Matcher
=================================================
GitHub Actions deployable version.

Pipeline
────────
  0. Load  data/page_already_extracted.txt  → know which pages were done.
  1. Load existing  data/all_data_part_N.json  (deduplication by subjectId AND main_url).
  2. Load existing  data/main_urls_list_part_N.json  (URL deduplication).
  3. Determine which pages to fetch this run  (PAGES_TO_FETCH controls the
     *total* page budget; pages already in page_already_extracted.txt are skipped).
  4. Fetch raw pages from AOneRoom API — only un-extracted pages.
  5. Skip any fetched URL already present in all_data OR main_urls_list → no reprocessing.
  6. Add every new main_url to the URL list → save BEFORE matching.
  7. Enrich only truly-new items with TMDB / IMDB.
  8. Items whose TMDB/IMDB IDs are missing or completely non-matching →
       data/imdb_tmdb_or_any_of_this_not_found_or_not_matching_part_N.json  (≤ 2 MB each).
  9. Merge + save single output:  data/all_data_part_N.json  (≤ 2 MB each).
 10. Save  data/main_urls_list_part_N.txt  (≤ 2 MB each, unique URLs only).
 11. Append newly-fetched page numbers to  data/page_already_extracted.txt.
 12. Update  data/index.json.

page_already_extracted.txt  format
───────────────────────────────────
One integer per line, representing a page number that has already been
successfully fetched and processed.  Example:

  1
  2
  3
  …
  20

On the NEXT run (say PAGES_TO_FETCH=30), the script reads this file,
sees pages 1-20 are done, and only fetches pages 21-30.
After that run the file will contain 1-30, and so on.

Output files
────────────
  data/all_data_part_1.json
  data/all_data_part_2.json
  ...
  data/main_urls_list_part_1.txt
  data/main_urls_list_part_2.txt
  ...
  data/imdb_tmdb_or_any_of_this_not_found_or_not_matching_part_1.json
  data/imdb_tmdb_or_any_of_this_not_found_or_not_matching_part_2.json
  ...
  data/page_already_extracted.txt
  data/index.json

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

Not-found / not-matching item shape (same fields, plus reason)
──────────────────────────────────────────────────────────────
{
  ... (all standard fields) ...
  "imdb_id/tmdb_id" : "N/A",
  "not_found_reason": "no_tmdb_match"   # or "imdb_id_missing" / "tmdb_id_missing" / "ids_not_matching"
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

JSON_PREFIX       = "all_data"
URL_PREFIX        = "main_urls_list"
NOT_FOUND_PREFIX  = "imdb_tmdb_or_any_of_this_not_found_or_not_matching"
PAGE_TRACKER_FILE = "page_already_extracted.txt"   # ← NEW

MAIN_HEADERS = {
    "User-Agent"     : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept"         : "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer"        : "https://www.aoneroom.com/",
    "Origin"         : "https://www.aoneroom.com",
}


# ══════════════════════════════════════════════
#  PAGE TRACKER  (NEW)
# ══════════════════════════════════════════════

def load_extracted_pages() -> set[int]:
    """
    Read  data/page_already_extracted.txt  and return the set of page
    numbers that have already been successfully fetched.
    Returns an empty set if the file does not exist yet.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tracker = DATA_DIR / PAGE_TRACKER_FILE
    if not tracker.exists():
        print(f"  (no {PAGE_TRACKER_FILE} found – starting fresh)")
        return set()
    pages: set[int] = set()
    for line in tracker.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.isdigit():
            pages.add(int(line))
    print(f"  Already extracted pages: {sorted(pages)}")
    return pages


def save_extracted_pages(pages: set[int]) -> None:
    """
    Write the complete set of extracted page numbers to
    data/page_already_extracted.txt  (one integer per line, sorted).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tracker = DATA_DIR / PAGE_TRACKER_FILE
    content = "\n".join(str(p) for p in sorted(pages)) + "\n"
    tracker.write_text(content, encoding="utf-8")
    print(f"  💾  {PAGE_TRACKER_FILE}  updated  ({len(pages)} pages tracked)")


def compute_pages_to_run(already_done: set[int], target_total: int) -> list[int]:
    """
    Given pages already extracted and a target total page budget,
    return the list of NEW page numbers to fetch this run.

    Example
    ───────
    already_done  = {1, 2, …, 20}
    target_total  = 30          (PAGES_TO_FETCH env-var)
    → returns [21, 22, …, 30]

    If PAGES_TO_FETCH=10 and pages 1-10 are already done, returns [].
    If PAGES_TO_FETCH=5  and nothing is done yet, returns [1,2,3,4,5].

    Strategy: we always aim for pages 1 … target_total and skip the
    ones we already have.  This handles gaps too (e.g. a page that
    failed previously will be retried next run if it was never saved).
    """
    all_target = set(range(1, target_total + 1))
    pending    = sorted(all_target - already_done)
    return pending


# ══════════════════════════════════════════════
#  JSON helpers
# ══════════════════════════════════════════════

def load_json_parts(prefix: str) -> tuple[list[dict], set[str], set[str]]:
    """
    Load all  <prefix>_part_N.json  files.
    Returns (items_list, seen_subject_ids, seen_main_urls).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    items:     list[dict] = []
    seen_ids:  set[str]   = set()
    seen_urls: set[str]   = set()

    pat = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    for f in sorted(
        DATA_DIR.iterdir(),
        key=lambda f: int(m.group(1)) if (m := pat.match(f.name)) else -1,
    ):
        if not pat.match(f.name):
            continue
        try:
            for it in json.loads(f.read_text(encoding="utf-8")):
                sid = it.get("subjectId", "")
                mu  = it.get("main_url",  "")
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    items.append(it)
                if mu:
                    seen_urls.add(mu)
        except Exception as e:
            print(f"  [WARN] Could not load {f.name}: {e}")

    return items, seen_ids, seen_urls


def load_not_found_parts(prefix: str) -> tuple[list[dict], set[str]]:
    """
    Load existing not-found/not-matching records.
    Returns (items_list, seen_main_urls_in_not_found).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    items: list[dict] = []
    seen:  set[str]   = set()

    pat = re.compile(rf"^{re.escape(prefix)}_part_(\d+)\.json$")
    for f in sorted(
        DATA_DIR.iterdir(),
        key=lambda f: int(m.group(1)) if (m := pat.match(f.name)) else -1,
    ):
        if not pat.match(f.name):
            continue
        try:
            for it in json.loads(f.read_text(encoding="utf-8")):
                mu = it.get("main_url", "")
                if mu and mu not in seen:
                    seen.add(mu)
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
    for f in sorted(
        DATA_DIR.iterdir(),
        key=lambda f: int(m.group(1)) if (m := pat.match(f.name)) else -1,
    ):
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

    written:   list[str] = []
    part_n     = 1
    lines:     list[str] = []
    cur_bytes  = 0

    for url in urls:
        line = url + "\n"
        lb   = len(line.encode())
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
    ct  = cand.get("name" if is_tv else "title", "")
    cy  = _year(cand.get("first_air_date" if is_tv else "release_date", ""))
    cv  = str(cand.get("vote_average", ""))
    cc  = cand.get("vote_count", 0)

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
#  ID validation helper
# ══════════════════════════════════════════════

def _classify_id_result(imdb_id: str, tmdb_id, match: dict) -> tuple[bool, str]:
    """
    Returns (is_problematic: bool, reason: str).

    Conditions flagged as not-found / not-matching:
      • No TMDB match at all                        → "no_tmdb_match"
      • TMDB match found but TMDB ID is missing     → "tmdb_id_missing"
      • TMDB ID found but IMDB ID could not be fetched → "imdb_id_missing"
      • Both IDs present but title similarity < 0.5 → "ids_not_matching"
    """
    if not match:
        return True, "no_tmdb_match"

    if not tmdb_id:
        return True, "tmdb_id_missing"

    if not imdb_id:
        return True, "imdb_id_missing"

    # Extra sanity: check IMDB ID format (must start with "tt")
    if not str(imdb_id).startswith("tt"):
        return True, "imdb_id_invalid_format"

    # Check that the best match score is not suspiciously low
    score = match.get("_score", 0)
    if score < 25:                            # threshold – title barely matched
        return True, "ids_not_matching"

    return False, ""


# ══════════════════════════════════════════════
#  Build final item (post-match)
# ══════════════════════════════════════════════

def build_item(serial_no: int, subject: dict) -> tuple[dict, bool, str]:
    """
    Returns (item_dict, is_problematic, reason).
    """
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

    is_prob, reason = _classify_id_result(imdb_id, tmdb_id, match)

    item = {
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

    if is_prob:
        item["not_found_reason"] = reason

    return item, is_prob, reason


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  AOneRoom Trending Scraper + TMDB/IMDB Enricher")
    print(f"  PAGES_TO_FETCH target : {PAGES_TO_FETCH}  |  Per page: {PER_PAGE}")
    print(f"  Data dir : {DATA_DIR.resolve()}")
    print(f"  TMDB key : {'✓ set' if TMDB_API_KEY else '✗ NOT SET'}")
    print("=" * 60)

    # ── 0. Load page tracker ───────────────────────────────────
    print("\n→ Loading page tracker …")
    already_extracted: set[int] = load_extracted_pages()

    # Determine which pages to actually fetch this run
    pages_this_run: list[int] = compute_pages_to_run(already_extracted, PAGES_TO_FETCH)

    if not pages_this_run:
        print(f"\n  ✅  All pages up to {PAGES_TO_FETCH} already extracted.")
        print(f"      To fetch more, increase PAGES_TO_FETCH beyond {PAGES_TO_FETCH}.")
        _write_index(
            total=0, new=0, total_urls=0, total_not_found=0,
            url_parts=[], json_parts=[], not_found_parts=[],
            already_extracted_pages=sorted(already_extracted),
            pages_this_run=[],
        )
        return

    print(f"  Pages to fetch this run : {pages_this_run}")

    # ── 1. Load existing JSON data ─────────────────────────────
    print("\n→ Loading existing all_data JSON …")
    existing_items, seen_ids, seen_urls_from_json = load_json_parts(JSON_PREFIX)
    print(f"  Loaded {len(existing_items)} items, {len(seen_ids)} unique subjectIds")

    # ── 2. Load existing not-found records ────────────────────
    print("\n→ Loading existing not-found records …")
    existing_not_found, seen_not_found_urls = load_not_found_parts(NOT_FOUND_PREFIX)
    print(f"  Loaded {len(existing_not_found)} not-found records")

    # ── 3. Load existing URL list ──────────────────────────────
    print("\n→ Loading existing URL list …")
    known_urls: set[str] = load_url_parts(URL_PREFIX)

    # Back-fill URLs from existing JSON items and not-found records
    for it in existing_items:
        mu = it.get("main_url", "")
        if mu:
            known_urls.add(mu)
    for it in existing_not_found:
        mu = it.get("main_url", "")
        if mu:
            known_urls.add(mu)
    known_urls |= seen_urls_from_json
    known_urls |= seen_not_found_urls
    print(f"  Known URLs (all sources): {len(known_urls)}")

    # ── 4. Fetch pages (only new ones) ────────────────────────
    print(f"\n→ Fetching {len(pages_this_run)} new page(s): {pages_this_run} …")
    raw_subjects:       list[dict] = []
    successfully_fetched: set[int] = set()

    for idx, page in enumerate(pages_this_run):
        subjects = fetch_page(page, PER_PAGE)
        if subjects is not None:               # empty list is still "fetched OK"
            raw_subjects.extend(subjects)
            successfully_fetched.add(page)
        if idx < len(pages_this_run) - 1:
            time.sleep(0.4)

    print(f"  Successfully fetched pages : {sorted(successfully_fetched)}")
    if len(successfully_fetched) < len(pages_this_run):
        failed = set(pages_this_run) - successfully_fetched
        print(f"  ⚠  Failed pages (will retry next run): {sorted(failed)}")

    # ── 5. Filter out already-known URLs ──────────────────────
    print("\n→ Filtering out already-known URLs …")
    skipped_url    = 0
    unique_new:    list[dict] = []
    seen_this_run: set[str]   = set()

    for s in raw_subjects:
        detail = s.get("detailPath", "")
        sid    = s.get("subjectId", "")
        if not detail:
            continue

        candidate_url = f"{SFLIX_BASE}{detail}"

        if candidate_url in known_urls:
            skipped_url += 1
            continue

        if sid and sid in seen_ids:
            skipped_url += 1
            continue

        key = candidate_url
        if key in seen_this_run:
            continue
        seen_this_run.add(key)
        unique_new.append(s)

    print(f"  Skipped (already known) : {skipped_url}")
    print(f"  New to enrich           : {len(unique_new)}")

    # ── 6. Collect new main_urls (PRE-MATCH) ──────────────────
    print("\n→ Adding new main_urls to known set …")
    new_urls_added = 0
    for s in unique_new:
        detail = s.get("detailPath", "")
        if detail:
            url = f"{SFLIX_BASE}{detail}"
            if url not in known_urls:
                known_urls.add(url)
                new_urls_added += 1

    all_urls_sorted = sorted(known_urls)
    print(f"  New URLs this run : {new_urls_added}")
    print(f"  Total unique URLs : {len(all_urls_sorted)}")

    # ── 7. Save URL list NOW (before any TMDB calls) ──────────
    print("\n→ Saving main_urls_list (pre-match) …")
    url_parts = save_url_parts(URL_PREFIX, all_urls_sorted)

    # ── 8. Save page tracker NOW (pages safely recorded) ──────
    # Only mark pages that were successfully fetched
    updated_extracted = already_extracted | successfully_fetched
    print("\n→ Saving page tracker …")
    save_extracted_pages(updated_extracted)

    if not unique_new:
        print("\n  Nothing new to enrich — updating index and exiting.")
        _write_index(
            total=len(existing_items),
            new=0,
            total_urls=len(all_urls_sorted),
            total_not_found=len(existing_not_found),
            url_parts=url_parts,
            json_parts=[],
            not_found_parts=[],
            already_extracted_pages=sorted(updated_extracted),
            pages_this_run=pages_this_run,
        )
        return

    # ── 9. Enrich new items (TMDB / IMDB) ─────────────────────
    print("\n→ Enriching new items with TMDB/IMDB …")
    new_items:     list[dict] = []
    new_not_found: list[dict] = []
    base_serial = len(existing_items) + 1

    for i, subject in enumerate(unique_new, start=base_serial):
        title = subject.get("title", "")
        print(f"  [{i - base_serial + 1}/{len(unique_new)}] {title}")
        item, is_prob, reason = build_item(i, subject)
        if is_prob:
            print(f"    ⚠  Not found / not matching → {reason}")
            new_not_found.append(item)
        else:
            new_items.append(item)
        time.sleep(0.3)

    print(f"\n  Enriched OK      : {len(new_items)}")
    print(f"  Not found/match  : {len(new_not_found)}")

    # ── 10. Merge all_data (dedup by subjectId) ───────────────
    seen_merged: set[str]   = set()
    merged:      list[dict] = []
    for it in existing_items + new_items:
        sid = it.get("subjectId") or it.get("title", "")
        if sid and sid not in seen_merged:
            seen_merged.add(sid)
            merged.append(it)

    for idx, it in enumerate(merged, start=1):
        it["serial_no"] = idx

    # ── 11. Merge not-found (dedup by main_url) ───────────────
    seen_nf:   set[str]   = set(seen_not_found_urls)
    merged_nf: list[dict] = list(existing_not_found)
    for it in new_not_found:
        mu = it.get("main_url", "")
        if mu and mu not in seen_nf:
            seen_nf.add(mu)
            merged_nf.append(it)

    for idx, it in enumerate(merged_nf, start=1):
        it["serial_no"] = idx

    # ── 12. Save unified all_data JSON ────────────────────────
    print("\n→ Saving all_data JSON parts …")
    json_parts = save_json_parts(JSON_PREFIX, merged)

    # ── 13. Save not-found JSON ───────────────────────────────
    print("\n→ Saving not-found/not-matching JSON parts …")
    not_found_parts = save_json_parts(NOT_FOUND_PREFIX, merged_nf)

    # ── 14. Update index ──────────────────────────────────────
    _write_index(
        total=len(merged),
        new=len(new_items),
        total_urls=len(all_urls_sorted),
        total_not_found=len(merged_nf),
        url_parts=url_parts,
        json_parts=json_parts,
        not_found_parts=not_found_parts,
        already_extracted_pages=sorted(updated_extracted),
        pages_this_run=pages_this_run,
    )

    print("\n" + "=" * 60)
    print(f"  Done!")
    print(f"  Pages fetched this run   : {sorted(successfully_fetched)}")
    print(f"  Total pages ever done    : {sorted(updated_extracted)}")
    print(f"  New (matched) this run   : {len(new_items)}")
    print(f"  Not found this run       : {len(new_not_found)}")
    print(f"  Total all_data items     : {len(merged)}  →  {len(json_parts)} part(s)")
    print(f"  Total not-found items    : {len(merged_nf)}  →  {len(not_found_parts)} part(s)")
    print(f"  Unique URLs              : {len(all_urls_sorted)}  →  {len(url_parts)} URL file(s)")
    print("=" * 60)


def _write_index(
    total: int,
    new: int,
    total_urls: int,
    total_not_found: int,
    url_parts: list[str],
    json_parts: list[str],
    not_found_parts: list[str],
    already_extracted_pages: list[int],
    pages_this_run: list[int],
):
    stats = {
        "last_run"                  : time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_items"               : total,
        "new_this_run"              : new,
        "pages_fetched_this_run"    : pages_this_run,
        "pages_extracted_total"     : already_extracted_pages,
        "max_page_extracted"        : max(already_extracted_pages) if already_extracted_pages else 0,
        "max_file_size_mb"          : MAX_FILE_BYTES / (1024 * 1024),
        "total_unique_urls"         : total_urls,
        "total_not_found"           : total_not_found,
        "parts": {
            JSON_PREFIX       : json_parts,
            URL_PREFIX        : url_parts,
            NOT_FOUND_PREFIX  : not_found_parts,
        },
    }
    path = DATA_DIR / "index.json"
    path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print("  📋  index.json updated")


if __name__ == "__main__":
    main()
