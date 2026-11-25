"""
ECI Candidate Affidavit scraper - Airflow DAG
=============================================
Walks every dropdown on https://affidavit.eci.gov.in/candidate-affidavit
(Election -> Election Type -> State -> Phase -> Constituency), reads every
candidate card, opens every "View more" profile page and saves the profile
data (JSON + raw HTML), the candidate photo and every affidavit /
counter-affidavit PDF, plus CSV indexes.

Tasks
-----
  prepare_run_folder  -> creates this run's folder
                         <target>/eci_affidavit_run_<YYYYMMDD>_<HHMMSS>_<unix>
                         and deletes the oldest run folders so only the
                         latest "Runs To Keep" (default 3) remain
  discover_elections  -> reads the Election dropdown (filtered by the
                         "Election Filter" param)
  scrape_election     -> one mapped task per election, run one at a time;
                         resumable, so a retry continues where it stopped
  build_csv_reports   -> builds candidates_all.csv, one candidates.csv per
                         election, and verification_report.csv

Output layout (inside each run folder):

  _data/                     candidates_all.csv, verification_report.csv,
                             scrape.log, failed_downloads.txt,
                             unavailable_on_site.txt, catalog/, state/
  <Election>/<Type>/<State>/<Phase N>/<Constituency>/<Name>__nom<id>/
      profile.json  profile.html  photo.jpg  Affidavit_<id>.pdf ...

Requirements on the Airflow workers:  pip install requests beautifulsoup4
"""

from __future__ import annotations

import base64
import math
from collections import Counter
import csv
import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlencode

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from bs4 import BeautifulSoup
except ImportError as _e:  # pragma: no cover
    raise ImportError("Missing packages on the Airflow worker. Run:  pip install requests beautifulsoup4") from _e

BASE = "https://affidavit.eci.gov.in"
LIST_PAGE = BASE + "/candidate-affidavit"
STATE_VERSION = "2"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0 Safari/537.36")

log = logging.getLogger("eci")


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
_BAD = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
             *(f"LPT{i}" for i in range(1, 10))}


def safe_name(text: str, maxlen: int = 80) -> str:
    """Make a string safe as a Windows/macOS/Linux folder or file name."""
    s = _BAD.sub("_", (text or "").strip())
    s = re.sub(r"\s+", " ", s).strip(" .")
    if not s:
        s = "_unknown"
    if s.upper() in _RESERVED:
        s = "_" + s
    return s[:maxlen].rstrip(" .")


def clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def parse_options(html: str) -> list[tuple[str, str]]:
    """Return (value, text) for every <option> with a non-empty value."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for o in soup.find_all("option"):
        v = (o.get("value") or "").strip()
        if v:
            out.append((v, clean(o.get_text())))
    return out


def decode_direct_url(token: str) -> str | None:
    """Some pages store a plain base64-encoded file URL instead of an encrypted token."""
    if token.startswith("eyJ"):  # encrypted payload, must go through /affidavit-pdf-download/
        return None
    try:
        raw = base64.b64decode(token + "=" * (-len(token) % 4)).decode("utf-8", "strict")
    except Exception:
        return None
    return raw if raw.startswith("http") else None


def split_election_value(v: str) -> dict:
    """'34-AC-BYE-4-63' -> electionId, constType, ElectType, typeId, dbId."""
    p = v.strip().split("-")
    return {"electionId": p[0], "constType": p[1], "ElectType": p[2],
            "typeId": p[3] if len(p) > 3 else "", "dbId": p[4] if len(p) > 4 else ""}


# --------------------------------------------------------------------------- #
# parsing of listing + profile pages (pure functions, testable offline)
# --------------------------------------------------------------------------- #
def parse_listing(html: str) -> dict:
    """Parse one page of CandidateCustomFilter results."""
    soup = BeautifulSoup(html, "html.parser")
    cards = []
    for tr in soup.find_all("tr"):
        a = tr.find("a", href=re.compile(r"/show-profile/"))
        if not a:
            continue
        box = tr.find(class_="details-name") or tr
        h4 = box.find("h4")
        img = tr.find("img")
        card = {
            "name": clean(h4.get_text()) if h4 else "",
            "profile_url": a["href"].replace("http://", "https://", 1),
            "photo_url": img.get("src", "") if img else "",
        }
        for p in box.find_all("p"):
            strong = p.find("strong")
            if not strong:
                continue
            key = clean(strong.get_text()).rstrip(":").strip()
            val = clean(p.get_text())
            val = val[len(clean(strong.get_text())):].strip() if val.startswith(clean(strong.get_text())) else val
            if key:
                card[key.lower()] = val
        cards.append(card)

    total = None
    m = re.search(r"of\s*<span[^>]*>\s*([\d,]+)\s*</span>\s*results", html)
    if m:
        total = int(m.group(1).replace(",", ""))
    has_next = bool(soup.find("a", rel="next"))
    return {"cards": cards, "total": total, "has_next": has_next}


def parse_profile(html: str) -> dict:
    """Parse a show-profile page into sections, hidden ids and documents."""
    soup = BeautifulSoup(html, "html.parser")
    sections: dict[str, dict] = {}
    for label in soup.find_all("label"):
        key = clean(label.get_text()).rstrip(":").strip()
        val_el = label.find_next_sibling()
        if val_el is None and label.parent is not None:
            val_el = label.parent.find_next_sibling()
        val = clean(val_el.get_text(" ")) if val_el else ""
        hdr = label.find_previous("h3")
        sec = clean(hdr.get_text()) if hdr else "Details"
        d = sections.setdefault(sec, {})
        if key:
            if key in d and d[key] != val:
                d[key] = f"{d[key]} | {val}"
            else:
                d[key] = val
        elif val and d:
            # unlabeled continuation row (e.g. party name in Hindi)
            last = list(d)[-1]
            d[last] = f"{d[last]} | {val}"

    hidden = {}
    for inp in soup.find_all("input", type="hidden"):
        iid = inp.get("id") or inp.get("name")
        if iid and not iid.startswith("pdfUrl") and iid != "_token":
            hidden[iid] = inp.get("value", "")

    docs, seen_tokens = [], set()
    counter_ids = [b.get("data-counter-id") for b in soup.find_all(attrs={"data-counter-id": True})]
    for inp in soup.find_all("input", attrs={"name": "pdfUrl"}) + soup.find_all("input", id=re.compile(r"^pdfUrl")):
        token = inp.get("value", "")
        if not token or token in seen_tokens:
            continue
        seen_tokens.add(token)
        doc_id = (inp.get("id") or "")[len("pdfUrl"):]
        direct = decode_direct_url(token)
        kind = "Affidavit"
        if (doc_id and soup.find(attrs={"data-counter-id": doc_id})) or (direct and "counter" in direct.lower()):
            kind = "CounterAffidavit"
        if not doc_id and kind == "CounterAffidavit" and counter_ids:
            doc_id = counter_ids.pop(0) or ""
        holder = inp.find_parent(class_="info") or inp.parent
        h4 = holder.find("h4") if holder else None
        title = clean(h4.get_text()) if h4 else kind
        if "counter" in title.lower():
            kind = "CounterAffidavit"
        cnt = soup.find(id=f"updateCount{doc_id}") if doc_id else None
        uploaded = ""
        if holder:
            m = re.search(r"Uploaded On:\s*([^\n<]+?\d{2}:\d{2}:\d{2})", holder.get_text(" "))
            if m:
                uploaded = clean(m.group(1))
        docs.append({"doc_id": doc_id, "kind": kind, "title": title, "token": token,
                     "direct_url": direct or "",
                     "download_count": clean(cnt.get_text()) if cnt else "",
                     "uploaded_on": uploaded})
    used = set()
    for i, d in enumerate(docs, 1):
        stem = f"{d['kind']}_{d['doc_id'] or i}"
        while stem in used:
            stem += f"_{i}"
        used.add(stem)
        d["file"] = stem + ".pdf"

    # any other direct file links on the page
    extra = []
    for a in soup.find_all("a", href=True):
        if re.search(r"\.(pdf|jpe?g|png|docx?|xlsx?|zip)(\?|$)", a["href"], re.I):
            extra.append(a["href"])

    photo = ""
    prev = soup.find(id="imagePreview")
    if prev and prev.find("img"):
        photo = prev.find("img").get("src", "")

    return {"sections": sections, "hidden": hidden, "documents": docs,
            "other_files": sorted(set(extra)), "photo_url": photo}


# --------------------------------------------------------------------------- #
# scraper
# --------------------------------------------------------------------------- #
class Scraper:
    def __init__(self, out: Path, delay: float, workers: int, pdf: bool, photos: bool):
        self.out = out
        self.data_dir = out / "_data"
        self.state_dir = self.data_dir / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.delay = delay
        self.workers = workers
        self.pdf = pdf
        self.photos = photos
        self.token = ""
        self._lock = threading.Lock()
        self._tls = threading.local()
        self.done_file = self.state_dir / "done_constituencies.txt"
        vfile = self.state_dir / "version.txt"
        if not vfile.exists() or vfile.read_text().strip() != STATE_VERSION:
            # bookkeeping from an older version of this script: re-check everything
            # (files already on disk are kept and are not downloaded again)
            if self.done_file.exists():
                self.done_file.replace(self.state_dir / "done_constituencies.old.txt")
            vfile.write_text(STATE_VERSION)
        self.done = set(self.done_file.read_text("utf-8").split("\n")) if self.done_file.exists() else set()
        self._seen_noms = None
        self.target_failed = False
        self.rows_file = self.state_dir / "rows.jsonl"
        self.verify_rows: list[dict] = []
        self.main = self._new_session()
        self.refresh_token()

    # ---- http ------------------------------------------------------------- #
    def _new_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Referer": LIST_PAGE,
                          "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                          "Accept-Language": "en-IN,en;q=0.9"})
        retry = Retry(total=8, connect=8, read=8, backoff_factor=3,
                      status_forcelist=(429, 500, 502, 503, 504), respect_retry_after_header=True,
                      allowed_methods=frozenset({"GET", "POST"}))
        s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=max(4, self.workers * 2)))
        return s

    @property
    def sess(self) -> requests.Session:
        # one session per worker thread for file downloads; main thread uses self.main
        if threading.current_thread() is threading.main_thread():
            return self.main
        s = getattr(self._tls, "s", None)
        if s is None:
            s = self._new_session()
            s.cookies.update(self.main.cookies)
            self._tls.s = s
        return s

    def reset_session(self):
        with self._lock:
            self.main = self._new_session()
            self._tls = threading.local()
        try:
            self.refresh_token()
        except Exception as e:
            log.warning("Could not refresh session yet: %s", e)

    def refresh_token(self):
        r = self.main.get(LIST_PAGE, timeout=60)
        r.raise_for_status()
        m = re.search(r'name="csrf-token"\s+content="([^"]+)"', r.text)
        if not m:
            raise RuntimeError("Could not find CSRF token on the listing page")
        self.token = m.group(1)
        self.first_page_html = r.text

    def get(self, url, **kw):
        timeout = kw.pop("timeout", 90)
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self.sess.get(url, timeout=timeout, **kw)
                if r.status_code in (403, 419) and "affidavit.eci.gov.in" in url and attempt < 3:
                    # ECI's firewall temporarily blocks traffic it thinks is too fast
                    wait = (2, 5, 10)[attempt] * 60
                    log.warning("Site returned %s (temporary block). Pausing %d min, then retrying with a fresh session...",
                                r.status_code, wait // 60)
                    time.sleep(wait)
                    self.reset_session()
                    continue
                r.raise_for_status()
                return r
            except (requests.ConnectionError, requests.Timeout, requests.exceptions.RetryError,
                    requests.exceptions.ChunkedEncodingError) as e:
                if attempt == 3:
                    raise
                wait = 60 * (attempt + 1)
                log.warning("Server busy/unreachable (%s). Cooling down %ds...", type(e).__name__, wait)
                time.sleep(wait)

    def post(self, path, data):
        for attempt in range(3):
            time.sleep(self.delay)
            r = self.main.post(BASE + path, data={"_token": self.token, **data},
                               headers={"X-Requested-With": "XMLHttpRequest"}, timeout=60)
            if r.status_code == 419:  # CSRF / session expired
                log.info("Session expired, refreshing token")
                self.refresh_token()
                continue
            if r.status_code == 403 and attempt < 2:
                wait = (2, 5)[attempt] * 60
                log.warning("Site returned 403 (temporary block). Pausing %d min...", wait // 60)
                time.sleep(wait)
                self.reset_session()
                continue
            r.raise_for_status()
            return r.text
        raise RuntimeError(f"POST {path} kept failing with 419")

    # ---- dropdowns -------------------------------------------------------- #
    def elections(self):
        soup = BeautifulSoup(self.first_page_html, "html.parser")
        sel = soup.find("select", id="electionType")
        return [(o["value"].strip(), clean(o.get_text())) for o in sel.find_all("option") if o.get("value")]

    def election_types(self, election_value):
        return parse_options(self.post("/getAllElection", {"electionType": election_value}))

    def states(self, t):
        return parse_options(self.post("/getElectionState", {k: t[k] for k in ("electionId", "constType", "dbId", "ElectType")}))

    def phases(self, t, st):
        return parse_options(self.post("/getElectionPhase", {**{k: t[k] for k in ("electionId", "constType", "dbId", "ElectType")}, "st_code": st}))

    def constituencies(self, t, st, phase):
        return parse_options(self.post("/allConstListPhs", {**{k: t[k] for k in ("electionId", "constType", "dbId", "ElectType")},
                                                           "st_code": st, "phaseid": phase}))

    # ---- listing ---------------------------------------------------------- #
    def list_candidates(self, params: dict) -> tuple[list[dict], int | None]:
        """Every card on every page. Cards are NOT de-duplicated here: one person can
        file several nominations that look identical on the list page; they are told
        apart later by the nomination id on the profile page."""
        cards, total, page, pages = [], None, 1, None
        while page < 5000:
            url = BASE + "/CandidateCustomFilter?" + urlencode({**params, "page": page})
            res = parse_listing(self.get(url).text)
            if total is None and res["total"] is not None:
                total = res["total"]
                pages = max(1, math.ceil(total / (len(res["cards"]) or 10)))
            cards.extend(res["cards"])
            if not res["cards"]:
                break
            if pages is not None:
                if page >= pages:
                    break
            elif not res["has_next"]:
                break
            page += 1
        if total is None:
            total = len(cards)
        return cards, total

    # ---- candidate -------------------------------------------------------- #
    def save_candidate(self, card: dict, ctx: dict, folder: Path) -> dict:
        html = self.get(card["profile_url"]).text
        prof = parse_profile(html)
        nom = prof["hidden"].get("nomidHidden") or hashlib.md5(card_key(card).encode()).hexdigest()[:8]
        if self._seen_noms is not None:
            with self._lock:
                if nom in self._seen_noms:
                    return None  # same nomination listed twice
                self._seen_noms.add(nom)
        cdir = folder / safe_name(f"{card.get('name') or 'candidate'}__nom{nom}", 100)
        cdir.mkdir(parents=True, exist_ok=True)
        (cdir / "profile.html").write_text(html, "utf-8")

        files = []
        photo = prof["photo_url"] or card.get("photo_url")
        if self.photos and photo:
            ext = os.path.splitext(photo.split("?")[0])[1].lower() or ".jpg"
            f = self.download(photo, cdir / f"photo{ext}", expect=None)
            if f:
                files.append(f.name)

        for d in prof["documents"]:
            target = cdir / d["file"]
            if self.pdf and d["token"]:
                url = d["direct_url"] or f"{BASE}/affidavit-pdf-download/{d['token']}"
                f = self.download(url, target, expect=b"%PDF")
                d["downloaded"] = bool(f)
                if f:
                    files.append(f.name)
        for i, u in enumerate(prof["other_files"]):
            name = safe_name(f"file{i+1}_" + os.path.basename(u.split("?")[0]), 120)
            f = self.download(u if u.startswith("http") else BASE + u, cdir / name, expect=None)
            if f:
                files.append(f.name)

        record = {**ctx, "listing": {k: v for k, v in card.items()}, **prof, "folder": str(cdir.relative_to(self.out))}
        for d in record["documents"]:
            d.pop("token", None)  # session-bound, useless later
        (cdir / "profile.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), "utf-8")
        return flatten(record)

    def download(self, url: str, target: Path, expect: bytes | None) -> Path | None:
        if target.exists() and target.stat().st_size > 0:
            return target
        part = target.with_suffix(target.suffix + ".part")
        err = None
        for attempt in range(4):
            try:
                time.sleep(self.delay)
                with self.sess.get(url, stream=True, timeout=300) as r:
                    r.raise_for_status()
                    expected_len = int(r.headers.get("Content-Length") or 0)
                    written = 0
                    with open(part, "wb") as fh:
                        first = True
                        for chunk in r.iter_content(1 << 16):
                            if first and expect and not chunk.startswith(expect):
                                raise ValueError(f"not a {expect!r} file (got {chunk[:20]!r})")
                            first = False
                            fh.write(chunk)
                            written += len(chunk)
                    if expected_len and written < expected_len:
                        raise IOError(f"incomplete: {written}/{expected_len} bytes")
                os.replace(part, target)
                return target
            except requests.HTTPError as e:
                err = e
                if e.response is not None and e.response.status_code in (403, 404, 410):
                    # the file is missing on ECI's own server (the site's Download button
                    # fails too); record it and don't hold the constituency open for it
                    log.warning("Not available on the ECI site (%s): %s", e.response.status_code, target)
                    with self._lock:
                        with open(self.data_dir / "unavailable_on_site.txt", "a", encoding="utf-8") as fh:
                            fh.write(f"{target}\t{url}\t{e.response.status_code}\n")
                    return None
            except Exception as e:
                err = e
            try:
                part.unlink(missing_ok=True)
            except OSError:
                pass
            if attempt < 3:
                wait = 30 * (attempt + 1)
                log.info("Retrying %s in %ds (%s)", target.name, wait, str(err)[:80])
                time.sleep(wait)
        log.warning("Download failed %s -> %s: %s", url[:120], target.name, str(err)[:200])
        self._mark_failed()
        with self._lock:
            with open(self.data_dir / "failed_downloads.txt", "a", encoding="utf-8") as fh:
                fh.write(f"{target}\t{url}\t{err}\n")
        return None

    def _mark_failed(self):
        with self._lock:
            self.target_failed = True

    # ---- orchestration ---------------------------------------------------- #
    def run(self, election_filter=None, state_filter=None, list_only=False, election_values=None,
            write_csv=True):
        catalog = []
        for ev, ename in self.elections():
            if election_values is not None and ev not in election_values:
                continue
            if election_filter and election_filter.lower() not in ename.lower():
                continue
            log.info("=== Election: %s", ename)
            e_entry = {"value": ev, "name": ename, "types": []}
            catalog.append(e_entry)
            for tv, tname in self.election_types(ev):
                t = split_election_value(tv)
                t_label = f"{t['constType']}-{t['ElectType']}"
                t_entry = {"value": tv, "name": tname, "states": []}
                e_entry["types"].append(t_entry)
                for sv, sname in self.states(t):
                    if state_filter and state_filter.lower() not in sname.lower():
                        continue
                    phases = self.phases(t, sv) or [("", "")]
                    s_entry = {"value": sv, "name": sname, "phases": []}
                    t_entry["states"].append(s_entry)
                    for pv, pname in phases:
                        consts = self.constituencies(t, sv, pv) if pv else []
                        s_entry["phases"].append({"value": pv, "name": pname,
                                                  "constituencies": [{"value": c, "name": n} for c, n in consts]})
                        if list_only:
                            continue
                        base_params = {"electionType": ev, "election": tv, "states": sv}
                        if pv:
                            base_params["phase"] = pv
                        ctx_base = {"election": ename, "election_type": tname, "state": sname, "state_code": sv,
                                    "phase": pv}
                        targets = consts or [("", "_all")]
                        for cv, cname in targets:
                            params = {**base_params, **({"constId": cv} if cv else {})}
                            folder = (self.out / safe_name(ename) / safe_name(t_label) / safe_name(sname) /
                                      (safe_name(f"Phase {pv}") if pv else "Phase _none") / safe_name(cname))
                            try:
                                self.process_target(params, {**ctx_base, "constituency": cname, "const_no": cv}, folder)
                            except Exception as e:
                                log.error("  %s: failed (%s) - skipped for now, will be retried on the next run", cname, e)
                                self._seen_noms = None
                    if not list_only:
                        try:
                            self.verify_state(ev, tv, sv, ename, tname, sname, t_label)
                        except Exception as e:
                            log.error("  state check %s failed (%s) - will be retried on the next run", sname, e)
            (self.data_dir / "catalog" ).mkdir(exist_ok=True)
            (self.data_dir / "catalog" / f"{safe_name(ename)}.json").write_text(
                json.dumps(e_entry, ensure_ascii=False, indent=2), "utf-8")
        if not list_only and write_csv:
            self.write_csvs()
        elif self.verify_rows:
            self.write_verification()
        log.info("Finished.")
        return catalog

    def process_target(self, params, ctx, folder):
        key = json.dumps(params, sort_keys=True)
        if key in self.done:
            return
        self.target_failed = False
        self._seen_noms = set()
        total = None
        for rnd in range(3):  # extra rounds only while the site's count is not reached
            cards, total = self.list_candidates(params)
            before = len(self._seen_noms)
            self.save_cards(cards, ctx, folder)
            got = len(self._seen_noms)
            log.info("  %s / %s / %s: %d nominations saved (site says %s)%s", ctx["state"], ctx["phase"] or "-",
                     ctx["constituency"], got, total, f" [round {rnd+1}]" if rnd else "")
            if total is None or got >= total or got == before:
                break
        got = len(self._seen_noms)
        self._seen_noms = None
        if got < (total or 0):
            self.target_failed = True
        self.verify_rows.append({**{k: ctx[k] for k in ("election", "election_type", "state", "phase", "constituency")},
                                 "site_total": total, "collected": got, "ok": not self.target_failed})
        if self.target_failed:
            log.warning("  %s: incomplete, will be retried on the next run", ctx["constituency"])
            return
        with self._lock:
            self.done.add(key)
            with open(self.done_file, "a", encoding="utf-8") as fh:
                fh.write(key + "\n")

    def save_cards(self, cards, ctx, folder):
        rows = []
        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            futs = {ex.submit(self.save_candidate, c, ctx, folder): c for c in cards}
            for f in as_completed(futs):
                try:
                    r = f.result()
                    if r is not None:
                        rows.append(r)
                except Exception as e:
                    self._mark_failed()
                    log.error("Candidate failed %s: %s", futs[f].get("name"), e)
                    with open(self.data_dir / "failed_candidates.txt", "a", encoding="utf-8") as fh:
                        fh.write(json.dumps({**ctx, "card": futs[f], "error": str(e)}, ensure_ascii=False) + "\n")
        with self._lock, open(self.rows_file, "a", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")

    def verify_state(self, ev, tv, sv, ename, tname, sname, t_label):
        """Safety net: list the whole state without phase/constituency filters and
        pick up any candidate the constituency-level pass did not see."""
        key = json.dumps({"verify": [ev, tv, sv]})
        if key in self.done:
            return
        self.target_failed = False
        cards, total = self.list_candidates({"electionType": ev, "election": tv, "states": sv})
        # count unique saved nominations per listing key (rows.jsonl may repeat a folder across runs)
        have, seen_f = Counter(), set()
        if self.rows_file.exists():
            with open(self.rows_file, encoding="utf-8") as fh:
                for line in fh:
                    r = json.loads(line)
                    if (r.get("election") == ename and r.get("election_type") == tname
                            and r.get("state") == sname and r.get("folder") not in seen_f):
                        seen_f.add(r.get("folder"))
                        have[r.get("_card_key")] += 1
        short = Counter(card_key(c) for c in cards) - have
        missing = []
        for c in cards:
            k = card_key(c)
            if short[k] > 0:
                missing.append(c)
                short[k] -= 1
        if missing:
            log.info("  state check %s: %d candidate(s) not reached via constituency filters; saving them", sname, len(missing))
            for c in missing:
                cname = c.get("constituency") or "_unassigned"
                folder = self.out / safe_name(ename) / safe_name(t_label) / safe_name(sname) / "_found_by_state_check" / safe_name(cname)
                self.save_cards([c], {"election": ename, "election_type": tname, "state": sname, "state_code": sv,
                                      "phase": "", "constituency": cname, "const_no": ""}, folder)
        self.verify_rows.append({"election": ename, "election_type": tname, "state": sname, "phase": "ALL",
                                 "constituency": "ALL (state total)", "site_total": total,
                                 "collected": sum(have.values()) + len(missing),
                                 "ok": (sum(have.values()) + len(missing)) >= (total or 0)})
        if self.target_failed:
            return  # retry the state check next run
        with self._lock:
            self.done.add(key)
            with open(self.done_file, "a", encoding="utf-8") as fh:
                fh.write(key + "\n")

    def write_csvs(self):
        rows = []
        if self.rows_file.exists():
            with open(self.rows_file, encoding="utf-8") as fh:
                by_folder = {}
                for l in fh:
                    if l.strip():
                        r = json.loads(l)
                        by_folder[r.get("folder") or id(r)] = r
                rows = list(by_folder.values())
        if rows:
            fields = []
            for r in rows:
                for k in r:
                    if k not in fields:
                        fields.append(k)
            write_csv(self.data_dir / "candidates_all.csv", rows, fields)
            by_e: dict[str, list] = {}
            for r in rows:
                by_e.setdefault(r.get("election", "_"), []).append(r)
            for e, rs in by_e.items():
                write_csv(self.out / safe_name(e) / "candidates.csv", rs, fields)
        self.write_verification()
        log.info("CSV written: %d candidates", len(rows))

    def write_verification(self):
        if self.verify_rows:
            vf = self.data_dir / "verification_report.csv"
            new = not vf.exists()
            with open(vf, "a", newline="", encoding="utf-8-sig") as fh:
                w = csv.DictWriter(fh, fieldnames=list(self.verify_rows[0]))
                if new:
                    w.writeheader()
                w.writerows(self.verify_rows)
            self.verify_rows = []


def card_key(c: dict) -> str:
    return "|".join([c.get("name", ""), c.get("party", ""), c.get("constituency", ""),
                     c.get("status", ""), c.get("photo_url", "").lower()])


def flatten(rec: dict) -> dict:
    row = {k: rec.get(k, "") for k in ("election", "election_type", "state", "state_code", "phase",
                                       "constituency", "const_no")}
    row["_card_key"] = card_key(rec.get("listing", {}))
    for k, v in rec.get("listing", {}).items():
        if k != "profile_url":
            row[f"list_{k}"] = v
    for sec, kv in rec.get("sections", {}).items():
        for k, v in kv.items():
            row[f"{sec} :: {k}"] = v
    for k, v in rec.get("hidden", {}).items():
        row[f"id_{k}"] = v
    for i, d in enumerate(rec.get("documents", []), 1):
        for k in ("kind", "doc_id", "download_count", "uploaded_on", "file", "downloaded"):
            row[f"doc{i}_{k}"] = d.get(k, "")
    row["photo_url"] = rec.get("photo_url", "")
    row["folder"] = rec.get("folder", "")
    return row


def write_csv(path: Path, rows: list[dict], fields: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:  # utf-8-sig so Excel shows Hindi correctly
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ============================================================
# DAG
# ============================================================

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

from datetime import datetime, timedelta
import shutil


RUN_PREFIX = "eci_affidavit_run"
RUN_DIR_PATTERN = re.compile(rf"^{RUN_PREFIX}_\d{{8}}_\d{{6}}_(\d{{9,11}})$")


def list_run_folders(base: Path) -> list[Path]:
    """Run folders created by this DAG, oldest first (by the unix timestamp at the end)."""
    if not base.is_dir():
        return []
    runs = []
    for p in base.iterdir():
        m = RUN_DIR_PATTERN.match(p.name)
        if p.is_dir() and not p.is_symlink() and m:
            runs.append((int(m.group(1)), p))
    return [p for _, p in sorted(runs)]


def _setup_logging(out: Path):
    """Send the scraper's log to the Airflow task log AND to _data/scrape.log."""
    (out / "_data").mkdir(parents=True, exist_ok=True)
    log.setLevel(logging.INFO)
    log_file = str(out / "_data" / "scrape.log")
    if not any(isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == log_file
               for h in log.handlers):
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(fh)


def _make_scraper(params, run_dir: str) -> "Scraper":
    out = Path(run_dir)
    _setup_logging(out)
    return Scraper(
        out,
        delay=float(params["delay_seconds"]),
        workers=max(1, int(params["workers"])),
        pdf=bool(params["download_pdfs"]),
        photos=bool(params["download_photos"]),
    )


with DAG(
    dag_id="eci_candidate_affidavit_scraper",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    default_args={
        # the ECI site is flaky: a failed election is retried later and the
        # scraper resumes from where it stopped (finished work is skipped)
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
    },

    tags=[
        "eci",
        "elections",
        "affidavits",
        "scraper",
    ],

    params={

        "target_directory": Param(
            default="/DATA/Media/programs_and_routines/eci_affidavit/outbound",
            type="string",
            title="Target Directory Location",
            description=(
                "Base folder. Every DAG run gets its own sub-folder named "
                "eci_affidavit_run_<YYYYMMDD>_<HHMMSS>_<unix timestamp>."
            ),
        ),

        "runs_to_keep": Param(
            default=3,
            type="integer",
            minimum=1,
            title="Runs To Keep",
            description=(
                "How many run folders to keep. When a new run starts, the oldest "
                "run folders beyond this number are deleted."
            ),
        ),

        "election_filter": Param(
            default="",
            type="string",
            title="Election Filter",
            description=(
                "Only scrape elections whose name contains this text "
                "(e.g. '2024' or 'SEP-OCT-2026'). Leave empty for ALL elections."
            ),
        ),

        "state_filter": Param(
            default="",
            type="string",
            title="State Filter",
            description="Only scrape states whose name contains this text. Leave empty for ALL states.",
        ),

        "download_pdfs": Param(
            default=True,
            type="boolean",
            title="Download Affidavit PDFs",
            description="Download every affidavit / counter-affidavit PDF.",
        ),

        "download_photos": Param(
            default=True,
            type="boolean",
            title="Download Candidate Photos",
            description="Download every candidate photo.",
        ),

        "workers": Param(
            default=1,
            type="integer",
            minimum=1,
            maximum=4,
            title="Parallel Downloads",
            description="Candidates processed in parallel. Keep low to avoid ECI blocking.",
        ),

        "delay_seconds": Param(
            default=1.0,
            type="number",
            minimum=0,
            title="Delay Between Requests",
            description="Seconds to wait between requests to the ECI site.",
        ),
    },
) as dag:

    @task
    def prepare_run_folder(**context):
        """Create this run's folder and delete the oldest run folders,
        keeping only the latest `runs_to_keep` (this run included)."""

        params = context["params"]
        base = Path(params["target_directory"]).expanduser()
        keep = max(1, int(params["runs_to_keep"]))

        if not str(base).strip() or base == Path(base.anchor):
            raise ValueError(f"Refusing to use '{base}' as the target directory.")

        base.mkdir(parents=True, exist_ok=True)

        # Folder name: only A-Z a-z 0-9 _   e.g. eci_affidavit_run_20261125_102300_1795408380
        now = datetime.now()
        unix_ts = int(now.timestamp())
        run_dir = base / f"{RUN_PREFIX}_{now.strftime('%Y%m%d_%H%M%S')}_{unix_ts}"
        while run_dir.exists():  # two triggers in the same second
            unix_ts += 1
            run_dir = base / f"{RUN_PREFIX}_{now.strftime('%Y%m%d_%H%M%S')}_{unix_ts}"
        run_dir.mkdir()
        print(f"Run folder: {run_dir}")

        # Rotation: keep the newest `keep` folders, delete the rest.
        # Only folders matching the run-folder name pattern are ever touched.
        runs = list_run_folders(base)
        to_delete = [p for p in runs[:-keep] if p.resolve() != run_dir.resolve()]
        for old in to_delete:
            if old.resolve().parent != base.resolve():
                continue
            print(f"Deleting old run folder: {old}")
            shutil.rmtree(old)

        kept = list_run_folders(base)
        print(f"Run folders kept ({len(kept)}/{keep}):")
        for p in kept:
            print(f"  {p.name}")

        return str(run_dir)

    @task
    def discover_elections(run_dir: str, **context):
        """Read the Election dropdown and return the elections to scrape."""

        params = context["params"]
        scraper = _make_scraper(params, run_dir)

        wanted = (params["election_filter"] or "").strip().lower()

        elections = [
            {"value": value, "name": name}
            for value, name in scraper.elections()
            if not wanted or wanted in name.lower()
        ]

        if not elections:
            raise ValueError(
                f"No election matches the filter '{params['election_filter']}'."
            )

        print(f"{len(elections)} election(s) to scrape:")
        for e in elections:
            print(f"  {e['name']}  ({e['value']})")

        return elections

    @task(
        # one election at a time: the scraper's resume files are shared and
        # the ECI site blocks traffic that is too fast
        max_active_tis_per_dag=1,
        execution_timeout=None,
    )
    def scrape_election(election: dict, run_dir: str, **context):
        """Scrape every type / state / phase / constituency of one election.
        Retries of this run resume inside the same run folder."""

        params = context["params"]
        scraper = _make_scraper(params, run_dir)

        print(f"Scraping election: {election['name']}")

        scraper.run(
            election_filter=None,
            state_filter=(params["state_filter"] or None),
            election_values={election["value"]},
            write_csv=False,
        )

        incomplete_file = scraper.data_dir / "verification_report.csv"
        print(f"Finished: {election['name']}")
        print(f"Verification report: {incomplete_file}")

        return election["name"]

    @task(trigger_rule="all_done")
    def build_csv_reports(finished, run_dir: str, **context):
        """Combine everything collected in this run into the CSV files."""

        out = Path(run_dir)
        _setup_logging(out)

        # build the CSVs without contacting the site
        scraper = Scraper.__new__(Scraper)
        scraper.out = out
        scraper.data_dir = out / "_data"
        scraper.rows_file = scraper.data_dir / "state" / "rows.jsonl"
        scraper.verify_rows = []
        scraper.write_csvs()

        unavailable = scraper.data_dir / "unavailable_on_site.txt"
        failed = scraper.data_dir / "failed_downloads.txt"

        print("")
        print("=" * 70)
        print("ECI AFFIDAVIT SCRAPE COMPLETE")
        print("=" * 70)
        print(f"Output dir          : {out}")
        print(f"All candidates CSV  : {scraper.data_dir / 'candidates_all.csv'}")
        print(f"Verification report : {scraper.data_dir / 'verification_report.csv'}")
        if unavailable.exists():
            print(f"Missing on ECI site : {sum(1 for _ in open(unavailable, encoding='utf-8'))} file(s)")
        if failed.exists():
            print(f"Failed downloads    : see {failed} (re-run the DAG to retry)")
        print("=" * 70)

        return {
            "output_directory": str(out),
            "candidates_csv": str(scraper.data_dir / "candidates_all.csv"),
            "verification_report": str(scraper.data_dir / "verification_report.csv"),
        }

    run_dir = prepare_run_folder()
    elections = discover_elections(run_dir)
    finished = scrape_election.partial(run_dir=run_dir).expand(election=elections)
    build_csv_reports(finished, run_dir)
