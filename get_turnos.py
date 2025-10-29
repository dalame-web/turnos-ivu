#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, json, time, hashlib
import datetime as dt
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ---------------- Config ----------------
BASE = os.getenv("IVU_BASE_URL", "https://wcrew-ilsa.trenitalia.it").rstrip("/")
# En IVU tu login real es /mbweb/j_security_check (según HTML de credenciales)
LOGIN_POST = "/mbweb/j_security_check"
DUTIES_PATH = "/mbweb/main/ivu/desktop/duties"  # vista que define el directorio base
DATA_DIR = os.getenv("TURNOS_DATA_DIR", "./data")
MESES_A_LEER = int(os.getenv("MESES_A_LEER", "1"))  # actual (+1 si quieres el siguiente)

HTTP_TIMEOUT = 30
RETRIES = 3
SLEEP = 1.2

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("get_turnos")

# ---------------- Utils ----------------
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def sha256_text(t: str) -> str:
    return "sha256:" + hashlib.sha256(t.encode("utf-8", errors="ignore")).hexdigest()

def http_get(s: requests.Session, url: str, **kw) -> requests.Response:
    last = None
    for i in range(RETRIES):
        try:
            r = s.get(url, timeout=HTTP_TIMEOUT, **kw)
            if r.status_code >= 500:
                raise requests.RequestException(f"status {r.status_code}")
            return r
        except Exception as ex:
            last = ex
            time.sleep(SLEEP * (i+1))
    raise last

def http_post(s: requests.Session, url: str, data=None, **kw) -> requests.Response:
    last = None
    for i in range(RETRIES):
        try:
            r = s.post(url, data=data, timeout=HTTP_TIMEOUT, **kw)
            if r.status_code >= 500:
                raise requests.RequestException(f"status {r.status_code}")
            return r
        except Exception as ex:
            last = ex
            time.sleep(SLEEP * (i+1))
    raise last

def month_list() -> List[Tuple[int,int]]:
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
    # cabecera/tabla (cuando IVU la pinta)
    head = soup.select_one("table.allocation-info, table.duty_header_attribute, table.table-header-block")
    body = soup.select_one("table.duty-components-table")
    tipo = None; start=None; end=None; f=None; t=None; tren=None
    labels = []

    # tipo desde cabecera (si existe)
    if head:
        # buscar texto general
        labels.append(head.get_text(" ", strip=True))

    if body:
        # horas: primera start y última end
        starts = [td.get_text(" ", strip=True) for td in body.select("td.start_time")]
        ends   = [td.get_text(" ", strip=True) for td in body.select("td.end_time")]
        locs_f = [td.get_text(" ", strip=True) for td in body.select("td.start_location_long_name")]
        locs_t = [td.get_text(" ", strip=True) for td in body.select("td.end_location_long_name")]
        if starts: start = norm_hhmm(starts[0])
        if ends:   end   = norm_hhmm(ends[-1])
        if locs_f: f = locs_f[0]
        if locs_t: t = locs_t[-1]
        # tipo aproximado
        tipo_el = body.select_one("td.component-type, td.type")
        if tipo_el:
            tipo = tipo_el.get_text(" ", strip=True) or None
        labels.append(body.get_text(" ", strip=True))
        # tren
        trips = [td.get_text(" ", strip=True) for td in body.select("td.trip_numbers")]
        first = next((x for x in trips if x), "")
        m = re.search(r"\b([A-Z]{1,3}\d{2,5}|\d{3,5})\b", first) if first else None
        if m: tren = m.group(1)
        if not tren:
            m2 = re.search(r"\b([A-Z]{1,3}\d{2,5}|\d{3,5})\b", body.get_text(" ", strip=True))
            tren = m2.group(1) if m2 else None

    # fallback: horas por regex global
    if not start or not end:
        horas = re.findall(HORA, soup.get_text(" ", strip=True))
        if horas:
            if not start: start = norm_hhmm(":".join(horas[0]))
            if not end:   end   = norm_hhmm(":".join(horas[-1]))

    has_hours = bool(start and end)
    status = detect_status(labels, has_hours)
    overnight = is_overnight(start, end)

    return Dia(
        date=date_str, status=status, tipo=tipo,
        start=start, end=end, overnight=overnight,
        location_from=f, location_to=t, train_number=tren,
        raw_frag_href=href, html_hash=sha256_text(html),
        notes=[]
    )

# ---------------- Core (AJAX endpoints) ----------------
def do_login(s: requests.Session, user: str, pwd: str):
    # Necesitamos cookie de sesión de /mbweb/ antes del POST
    landing = http_get(s, urljoin(BASE, "/mbweb/"))
    # POST del formulario estándar j_security_check
    data = {"j_username": user, "j_password": pwd}
    r = http_post(s, urljoin(BASE, LOGIN_POST), data=data)
    if r.status_code not in (200, 302):
        raise RuntimeError("Login HTTP inesperado")
    # comprobar que entrar en duties responde autenticado
    r2 = http_get(s, urljoin(BASE, DUTIES_PATH))
    if "Cerrar sesión" not in r2.text and "logout" not in r2.text.lower():
        raise RuntimeError("Login fallido (no aparece sesión activa)")
    log.info("Login correcto ✅")

def get_base_dir_and_month_html(s: requests.Session) -> Tuple[str, str]:
    """
    Abre /duties para obtener el directorio base (/mbweb/main/ivu/desktop/)
    y pide por AJAX '_-duty-table' para volcar el HTML del mes.
    """
    duties = http_get(s, urljoin(BASE, DUTIES_PATH))
    # Obtener el directorio base de la URL final:
    # p.ej. https://.../mbweb/main/ivu/desktop/duties  -> base_dir = /mbweb/main/ivu/desktop/
    from urllib.parse import urlparse
    p = urlparse(duties.url)
    base_dir = re.sub(r"[^/]+$", "", p.path)  # quita 'duties'
    # Cargar el mes por AJAX
    ajax_url = urljoin(BASE, base_dir + "_-duty-table")
    hdr = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
        "Referer": urljoin(BASE, DUTIES_PATH)
    }
    month = http_get(s, ajax_url, headers=hdr).text
    return base_dir, month

def extract_dates_and_empid(html: str) -> Tuple[List[str], Optional[str]]:
    dates = sorted(set(re.findall(r"beginDate=(\d{4}-\d{2}-\d{2})", html)))
    empid = None
    m = re.search(r"allocatedEmployeeId=(\d+)", html)
    if m: empid = m.group(1)
    return dates, empid

def fetch_day_html(s: requests.Session, base_dir: str, date_ymd: str, empid: Optional[str]) -> Tuple[str, str]:
    qs = f"beginDate={date_ymd}&showUserInfo=true"
    if empid:
        qs += f"&allocatedEmployeeId={empid}"
    rel = f"_-duty-details-day?{qs}"
    url = urljoin(BASE, base_dir + rel)
    hdr = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
        "Referer": urljoin(BASE, DUTIES_PATH)
    }
    r = http_get(s, url, headers=hdr)
    return rel, r.text

# ---------------- Main ----------------
def main():
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
        do_login(s, user, pwd)

        # 1) Cargar mes vía AJAX
        base_dir, month_html = get_base_dir_and_month_html(s)
        dates, empid = extract_dates_and_empid(month_html)

        if not dates:
            log.warning("No se detectaron fechas en _-duty-table; revisa autenticación o rutas.")
            marker = os.path.join(DATA_DIR, "NO_DATA.txt")
            with open(marker, "w", encoding="utf-8") as f:
                f.write("Sin fechas en _-duty-table (AJAX). Revisa selectores/rutas/cookies.\n")
            log.info(f"Marcador escrito: {marker}")
            return

        # 2) Por cada día, pedir detalle AJAX
        by_month: Dict[str, List[Dia]] = {}
        for ymd in dates:
            try:
                href, html_day = fetch_day_html(s, base_dir, ymd, empid)
                dia = parse_day_html(ymd, html_day, href)
                ym = ymd[:7]
                by_month.setdefault(ym, []).append(dia)
                if dia.status == "SERVICIO":
                    log.info(f"[{ymd}] {dia.status} {dia.start}-{dia.end} {dia.tipo or ''} {dia.location_from or ''} → {dia.location_to or ''}")
                else:
                    log.info(f"[{ymd}] {dia.status}")
            except Exception as ex:
                log.warning(f"[{ymd}] Error leyendo día: {ex}")

        # 3) Guardar JSON por mes
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

if __name__ == "__main__":
    main()
