#!/usr/bin/env python3
# -*- coding: utf-8 -*-
VERSION = "get_turnos v1.4-login-robusto"

import os, re, sys, json, time, hashlib, calendar
import datetime as dt
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# ---------------- Config ----------------
BASE = os.getenv("IVU_BASE_URL", "https://wcrew-ilsa.trenitalia.it").rstrip("/")
LOGIN_POST = "/mbweb/j_security_check"
DUTIES_PATH = "/mbweb/main/ivu/desktop/duties"

DATA_DIR = os.getenv("TURNOS_DATA_DIR", "./data")
MESES_A_LEER = int(os.getenv("MESES_A_LEER", "1"))  # 1 = mes actual, 2 = actual + siguiente

# Timeouts y reintentos robustos
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC", "90"))
RETRIES = int(os.getenv("HTTP_RETRIES", "5"))
SLEEP_BASE = float(os.getenv("HTTP_BACKOFF", "1.5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("get_turnos")

# ---------------- Utils ----------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def sha256_text(t: str) -> str:
    return "sha256:" + hashlib.sha256(t.encode("utf-8", errors="ignore")).hexdigest()

DEBUG_DIR = os.path.join(DATA_DIR, "debug")
def snapshot(name: str, content: str):
    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
        path = os.path.join(DEBUG_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log.info(f"[DEBUG] snapshot -> {path}")
    except Exception as e:
        log.warning(f"[DEBUG] no se pudo escribir snapshot {name}: {e}")

def http_req(s: requests.Session, method: str, url: str, **kw) -> requests.Response:
    """
    Envoltorio con reintentos + backoff. Captura y reintenta timeouts/transitorios.
    """
    last = None
    for i in range(RETRIES):
        try:
            r = s.request(method, url, timeout=HTTP_TIMEOUT, **kw)
            # Trata 5xx como transitorios
            if r.status_code >= 500:
                raise requests.RequestException(f"status {r.status_code}")
            return r
        except Exception as ex:
            last = ex
            sleep = SLEEP_BASE * (i + 1)
            log.warning(f"[HTTP {method}] Intento {i+1}/{RETRIES} fallo: {ex} -> reintentando en {sleep:.1f}s")
            time.sleep(sleep)
    raise last

def http_get(s: requests.Session, url: str, **kw) -> requests.Response:
    return http_req(s, "GET", url, **kw)

def http_post(s: requests.Session, url: str, **kw) -> requests.Response:
    return http_req(s, "POST", url, **kw)

def months_to_read() -> List[Tuple[int,int]]:
    today = dt.date.today()
    out = []
    y, m = today.year, today.month
    for i in range(MESES_A_LEER):
        yy = y + (m+i-1)//12
        mm = (m+i-1)%12 + 1
        out.append((yy, mm))
    return out

# ---------------- Modelos ----------------
@dataclass
class Dia:
    date: str
    status: str                    # SERVICIO | DESCANSO | LD | I | LIBRE
    tipo: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    overnight: bool = False
    location_from: Optional[str] = None
    location_to: Optional[str] = None
    train_number: Optional[str] = None
    raw_frag_href: Optional[str] = None
    html_hash: Optional[str] = None
    notes: List[str] = None

# ---------------- Parsers ----------------
HORA = r"(\d{1,2}):(\d{2})"

def norm_hhmm(s: Optional[str]) -> Optional[str]:
    if not s: return None
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s)
    if not m: return None
    h, mm = int(m.group(1)), int(m.group(2))
    if 0 <= h < 24 and 0 <= mm < 60:
        return f"{h:02d}:{mm:02d}"
    return None

def detect_status(texts: List[str], has_hours: bool) -> str:
    j = " ".join(t.upper() for t in texts)
    if "LD" in j: return "LD"
    if "DESCANSO" in j: return "DESCANSO"
    if re.search(r"\bI\b", j) and not has_hours: return "I"
    return "SERVICIO" if has_hours else "LIBRE"

def is_overnight(start_hhmm: Optional[str], end_hhmm: Optional[str]) -> bool:
    if not start_hhmm or not end_hhmm: return False
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, end_hhmm.split(":"))
    return (eh*60+em) < (sh*60+sm)

def parse_day_html(date_str: str, html: str, href: str) -> Dia:
    soup = BeautifulSoup(html, "html.parser")
    head_txt = soup.get_text(" ", strip=True)

    # tabla principal si existe
    body = soup.select_one("table.duty-components-table")
    tipo=None; start=None; end=None; f=None; t=None; tren=None
    labels = []

    if body:
        labels.append(body.get_text(" ", strip=True))
        starts = [td.get_text(" ", strip=True) for td in body.select("td.start_time")]
        ends   = [td.get_text(" ", strip=True) for td in body.select("td.end_time")]
        locs_f = [td.get_text(" ", strip=True) for td in body.select("td.start_location_long_name")]
        locs_t = [td.get_text(" ", strip=True) for td in body.select("td.end_location_long_name")]
        if starts: start = norm_hhmm(starts[0])
        if ends:   end   = norm_hhmm(ends[-1])
        if locs_f: f = locs_f[0]
        if locs_t: t = locs_t[-1]
        tipo_el = body.select_one("td.component-type, td.type")
        if tipo_el:
            tipo = tipo_el.get_text(" ", strip=True) or None
        trips = [td.get_text(" ", strip=True) for td in body.select("td.trip_numbers")]
        first = next((x for x in trips if x), "")
        m = re.search(r"\b([A-Z]{1,3}\d{2,5}|\d{3,5})\b", first) if first else None
        if m: tren = m.group(1)
        if not tren:
            m2 = re.search(r"\b([A-Z]{1,3}\d{2,5}|\d{3,5})\b", body.get_text(" ", strip=True))
            tren = m2.group(1) if m2 else None

    # fallback global a horas si no hay tabla
    if not start or not end:
        horas = re.findall(HORA, head_txt)
        if horas:
            if not start: start = norm_hhmm(":".join(horas[0]))
            if not end:   end   = norm_hhmm(":".join(horas[-1]))

    has_hours = bool(start and end)
    labels.append(head_txt)
    status = detect_status(labels, has_hours)
    overnight = is_overnight(start, end)

    return Dia(
        date=date_str, status=status, tipo=tipo,
        start=start, end=end, overnight=overnight,
        location_from=f, location_to=t, train_number=tren,
        raw_frag_href=href, html_hash=sha256_text(html),
        notes=[]
    )

# ---------------- Core ----------------
def do_login(s: requests.Session, user: str, pwd: str):
    """
    Login robusto:
    - Pre-GET de /mbweb/ para cookies base (snapshot).
    - POST a /mbweb/j_security_check con cabeceras realistas (snapshot de estado).
    - GET a /duties para verificar sesión (snapshot).
    """
    # Cabeceras comunes
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/118.0.0.0 Safari/537.36"),
        "Accept-Language": "es-ES,es;q=0.9",
    })

    # 1) PRE-GET
    pre = http_get(s, urljoin(BASE, "/mbweb/"))
    snapshot("LOGIN_PRE.html", pre.text)

    # 2) POST j_security_check
    data = {"j_username": user, "j_password": pwd}
    headers = {
        "Origin": BASE,
        "Referer": urljoin(BASE, "/mbweb/"),
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    post = http_post(s, urljoin(BASE, LOGIN_POST), data=data, headers=headers)
    # snapshot del status y parte del cuerpo (no siempre devuelve HTML útil)
    snapshot("LOGIN_POST_status.txt", f"status={post.status_code}\nurl={post.url}\nheaders={dict(post.headers)}\n")

    # 3) Verificación de sesión en /duties
    r2 = http_get(s, urljoin(BASE, DUTIES_PATH), headers={"Referer": urljoin(BASE, "/mbweb/")})
    snapshot("LOGIN_AFTER.html", r2.text)

    if "Cerrar sesión" not in r2.text and "logout" not in r2.text.lower():
        raise RuntimeError("Login fallido (no aparece sesión activa)")

    log.info("Login correcto ✅")
    return r2

def get_base_dir_from_duties_url(duties_url: str) -> str:
    p = urlparse(duties_url)
    return re.sub(r"/[^/]*$", "", p.path)

def candidate_month_urls(base_dir_from_duties: str) -> List[str]:
    bd = base_dir_from_duties.rstrip("/")
    cands = [
        f"{bd}/_-duty-table",
        f"{bd}/_-duty-table?force=1",
        "/mbweb/_-duty-table",
        "/mbweb/_-duty-table?force=1",
        "/_-duty-table",
        "/_-duty-table?force=1",
    ]
    seen=set(); out=[]
    for u in cands:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def fetch_month_ajax_html(s: requests.Session, base_dir: str) -> Tuple[Optional[str], Optional[str]]:
    hdr = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
        "Referer": urljoin(BASE, DUTIES_PATH)
    }
    for rel in candidate_month_urls(base_dir):
        url = urljoin(BASE, rel)
        try:
            r = http_get(s, url, headers=hdr)
            if re.search(r"beginDate=\d{4}-\d{2}-\d{2}", r.text):
                log.info(f"_-duty-table OK vía {rel}")
                return r.text, rel
        except Exception as ex:
            log.warning(f"_-duty-table fallo vía {rel}: {ex}")
            continue
    return None, None

def extract_dates_and_empid(html: str) -> Tuple[List[str], Optional[str]]:
    dates = sorted(set(re.findall(r"beginDate=(\d{4}-\d{2}-\d{2})", html)))
    empid = None
    m = re.search(r"allocatedEmployeeId=(\d+)", html)
    if m: empid = m.group(1)
    return dates, empid

def day_url(base_dir: str, date_ymd: str, empid: Optional[str]) -> str:
    qs = f"beginDate={date_ymd}&showUserInfo=true"
    if empid:
        qs += f"&allocatedEmployeeId={empid}"
    return base_dir.rstrip("/") + "/_-duty-details-day?" + qs

def fetch_day_html(s: requests.Session, base_dir: str, date_ymd: str, empid: Optional[str]) -> Tuple[str, str]:
    hdr = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
        "Referer": urljoin(BASE, DUTIES_PATH)
    }
    rels = [
        day_url(base_dir, date_ymd, empid),
        "/mbweb/_-duty-details-day?beginDate=" + date_ymd + ("&allocatedEmployeeId="+empid if empid else "") + "&showUserInfo=true",
        "/_-duty-details-day?beginDate=" + date_ymd + ("&allocatedEmployeeId="+empid if empid else "") + "&showUserInfo=true",
    ]
    tried = set()
    for rel in rels:
        if rel in tried: continue
        tried.add(rel)
        url = urljoin(BASE, rel)
        r = http_get(s, url, headers=hdr)
        if r.status_code == 200 and r.text.strip():
            return rel, r.text
    raise RuntimeError("No pude obtener detalle de día por ninguna ruta candidata")

def iter_month_days(year: int, month: int) -> List[str]:
    last = calendar.monthrange(year, month)[1]
    return [f"{year:04d}-{month:02d}-{d:02d}" for d in range(1, last+1)]

# ---------------- Main ----------------
def main():
    print(f"=== {VERSION} ===", file=sys.stderr)
    user = os.getenv("IVU_USER"); pwd = os.getenv("IVU_PASS")
    if not user or not pwd:
        log.error("Faltan IVU_USER / IVU_PASS"); sys.exit(1)

    ensure_dir(DATA_DIR)

    with requests.Session() as s:
        s.headers.update({
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/118.0.0.0 Safari/537.36"),
            "Accept-Language": "es-ES,es;q=0.9",
        })

        log.info("Haciendo login en IVU…")
        try:
            r_duties = do_login(s, user, pwd)
        except Exception as ex:
            snapshot("LOGIN_ERROR.txt", f"{type(ex).__name__}: {ex}")
            raise

        # DEBUG base_dir
        base_dir = get_base_dir_from_duties_url(r_duties.url)
        log.info(f"[DEBUG] base_dir calculado = {base_dir}")

        months = months_to_read()
        log.info(f"Meses a leer: {months}")

        for (yy, mm) in months:
            month_html, used_url = fetch_month_ajax_html(s, base_dir)
            log.info(f"[DEBUG] _-duty-table usado = {used_url}")
            if month_html:
                snapshot("01_month.html", month_html)

            dates: List[str] = []
            empid: Optional[str] = None

            if month_html:
                dts, emp = extract_dates_and_empid(month_html)
                dates = [d for d in dts if d.startswith(f"{yy:04d}-{mm:02d}")]
                empid = emp
            log.info(f"[{yy}-{mm:02d}] Fechas encontradas en _-duty-table: {len(dates)}")

            if not dates:
                log.warning(f"No se obtuvieron fechas por _-duty-table para {yy}-{mm:02d}. Activo barrido diario.")
                dates = iter_month_days(yy, mm)

            by_month: Dict[str, List[Dia]] = {}
            tested = 0
            for ymd in dates:
                try:
                    href, html_day = fetch_day_html(s, base_dir, ymd, empid)
                    if tested < 3:
                        snapshot(f"day_{ymd}.html", html_day)
                        log.info(f"[DEBUG] Probed day URL = {href}")
                        tested += 1
                    dia = parse_day_html(ymd, html_day, href)
                    ym = ymd[:7]
                    by_month.setdefault(ym, []).append(dia)
                    if dia.status == "SERVICIO":
                        log.info(f"[{ymd}] {dia.status} {dia.start}-{dia.end} {dia.tipo or ''} {dia.location_from or ''} → {dia.location_to or ''}")
                    else:
                        log.info(f"[{ymd}] {dia.status}")
                except Exception as ex:
                    log.warning(f"[{ymd}] Error leyendo día: {ex}")

            if by_month:
                now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
                for ym, days in sorted(by_month.items()):
                    payload = {
                        "employee_id": empid,
                        "generated_at": now_iso,
                        "source": BASE.replace("https://","").replace("http://",""),
                        "year_month": ym,
                        "days": [asdict(d) for d in days],
                    }
                    out = os.path.join(DATA_DIR, f"turnos_{ym}.json")
                    with open(out, "w", encoding="utf-8") as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                    log.info(f"💾 Guardado {out} ({len(days)} días)")
            else:
                marker = os.path.join(DATA_DIR, f"NO_DATA_{yy}-{mm:02d}.txt")
                with open(marker, "w", encoding="utf-8") as f:
                    f.write("No se pudo obtener información de ningún día del mes.\n")
                log.warning(f"No se detectaron días/turnos en {yy}-{mm:02d}. Verifica rutas o credenciales.")

if __name__ == "__main__":
    import sys
    main()
