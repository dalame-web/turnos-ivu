#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parte 1: Login a IVU + lectura de turnos + guardado a JSON por mes.
- Lee el/los meses visibles (configurable con MESES_A_LEER).
- Por cada día, abre el detalle, clasifica el estado y normaliza campos.
- Genera un archivo JSON por mes en TURNOS_DATA_DIR (por defecto ./data).

NOTAS:
- Este script está preparado para adaptarse a pequeños cambios de HTML.
- Los selectores CSS están parametrizados en SELECTORS para facilitar ajuste.
- La detección de "dormida" se basa en cruce de medianoche y heurística simple.

Autor: Blue (ChatGPT)
"""

import os
import re
import sys
import json
import time
import hashlib
import logging
import datetime as dt
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlencode

# -----------------------------
# Configuración y constantes
# -----------------------------
IVU_BASE_URL = os.getenv("IVU_BASE_URL", "https://wcrew-ilsa.trenitalia.it").rstrip("/")
IVU_LOGIN_PATH = "/_login"  # Ajusta si tu instancia difiere
MESES_A_LEER = int(os.getenv("MESES_A_LEER", "1"))  # 1 = mes actual; 2 = actual + siguiente
DATA_DIR = os.getenv("TURNOS_DATA_DIR", "./data")

# Selectores (ajústalos si IVU cambia)
SELECTORS = {
    "calendar_days": ".ivu-calendar .ivu-calendar-day, .calendar .day, .ivu-day",  # fallback múltiple
    "day_href": "a[href*='duty-details-day'], a[href*='duty-details']",            # enlace a detalle de día
    "details_header": ".duty-details-header, .duty-header, header",
    "table_components": ".duty-components-table, table.components",
    "table_row": ".duty-components-table-row, tr",
    "cell_start": ".start_time, .start, td:nth-child(1)",
    "cell_end": ".end_time, .end, td:nth-child(2)",
    "cell_from": ".start_location_long_name, .from, td:nth-child(3)",
    "cell_to": ".end_location_long_name, .to, td:nth-child(4)",
    "cell_type": ".type, .component-type, td:nth-child(5)",
    "turno_tipo_header": ".duty-type, .turn-type, .badge, .tag",
    "labels_text": ".label, .chip, .badge",
    "train_number": ".trip_numbers, .train, .service-no, .numero-tren",
}

# Tiempo de espera/reintento red
HTTP_TIMEOUT = 30
HTTP_RETRIES = 3
RETRY_SLEEP = 1.5

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("get_turnos")

# -----------------------------
# Dataclasses
# -----------------------------
@dataclass
class DiaTurno:
    date: str
    status: str                        # SERVICIO | DESCANSO | LD | I | LIBRE
    tipo: Optional[str] = None         # Código de turno p.ej "F-BC04A"
    start: Optional[str] = None        # "HH:MM"
    end: Optional[str] = None          # "HH:MM"
    overnight: bool = False
    location_from: Optional[str] = None
    location_to: Optional[str] = None
    train_number: Optional[str] = None
    raw_frag_href: Optional[str] = None
    html_hash: Optional[str] = None
    notes: List[str] = None

@dataclass
class MesTurnos:
    employee_id: Optional[str]
    generated_at: str
    source: str
    year_month: str
    days: List[DiaTurno]

# -----------------------------
# Utilidades
# -----------------------------
def sha256_text(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

def norm_hhmm(s: str) -> Optional[str]:
    if not s:
        return None
    # Normaliza "8:5" -> "08:05"
    m = re.match(r"^\s*(\d{1,2}):(\d{1,2})\s*$", s)
    if not m: 
        return None
    h, mm = int(m.group(1)), int(m.group(2))
    if 0 <= h < 24 and 0 <= mm < 60:
        return f"{h:02d}:{mm:02d}"
    return None

def parse_time(text: str) -> Optional[str]:
    # Extrae el primer HH:MM que aparezca
    m = re.search(r"(\d{1,2}):(\d{2})", text or "")
    return norm_hhmm(m.group(0)) if m else None

def simplify(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "").strip())

def detect_status(texts: List[str], has_hours: bool) -> str:
    """
    Heurística de estado por día según textos visibles y si hay horas.
    """
    joined = " ".join([t.upper() for t in texts])
    if "LD" in joined:
        return "LD"
    if re.search(r"\bDESCANSO\b", joined):
        return "DESCANSO"
    if re.search(r"\bI\b", joined) and not has_hours:
        return "I"
    if has_hours:
        return "SERVICIO"
    return "LIBRE"

def is_overnight(start_hhmm: Optional[str], end_hhmm: Optional[str]) -> bool:
    if not start_hhmm or not end_hhmm:
        return False
    try:
        sh, sm = map(int, start_hhmm.split(":"))
        eh, em = map(int, end_hhmm.split(":"))
        start_minutes = sh * 60 + sm
        end_minutes = eh * 60 + em
        return end_minutes < start_minutes  # cruza medianoche
    except Exception:
        return False

def ensure_dir(path: str):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)

# -----------------------------
# Sesión y login
# -----------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; TurnosBot/1.0; +https://example.invalid)",
        "Accept-Language": "es-ES,es;q=0.9",
    })
    s.timeout = HTTP_TIMEOUT
    return s

def http_get(session: requests.Session, url: str, **kwargs) -> requests.Response:
    last_ex = None
    for i in range(HTTP_RETRIES):
        try:
            r = session.get(url, timeout=HTTP_TIMEOUT, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"status={r.status_code}")
            return r
        except Exception as ex:
            last_ex = ex
            time.sleep(RETRY_SLEEP * (i + 1))
    raise last_ex

def http_post(session: requests.Session, url: str, data=None, **kwargs) -> requests.Response:
    last_ex = None
    for i in range(HTTP_RETRIES):
        try:
            r = session.post(url, data=data, timeout=HTTP_TIMEOUT, **kwargs)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"status={r.status_code}")
            return r
        except Exception as ex:
            last_ex = ex
            time.sleep(RETRY_SLEEP * (i + 1))
    raise last_ex

def login_ivu(session: requests.Session, user: str, pwd: str) -> None:
    login_url = urljoin(IVU_BASE_URL, IVU_LOGIN_PATH)
    logger.info("Haciendo login en IVU…")
    # Algunas instalaciones requieren obtener un token CSRF/hidden antes del POST:
    r0 = http_get(session, login_url)
    soup0 = BeautifulSoup(r0.text, "html.parser")
    # Busca campos hidden (csrf, lt, execution, etc.)
    payload = {}
    for hidden in soup0.select("form input[type='hidden']"):
        name = hidden.get("name")
        val = hidden.get("value", "")
        if name:
            payload[name] = val
    # Añade credenciales (ajusta nombres si difieren en tu instancia)
    # Los nombres típicos: username / password
    payload.update({
        "username": user,
        "password": pwd,
    })
    # Envía POST
    r1 = http_post(session, login_url, data=payload)
    if "logout" not in r1.text.lower() and "salir" not in r1.text.lower():
        # Heurística: comprobar presencia de algo típico tras login
        if "duty" not in r1.text.lower() and r1.url.endswith(IVU_LOGIN_PATH):
            logger.error("Login fallido: revisa usuario/contraseña o el flujo de autenticación.")
            raise SystemExit(1)
    logger.info("Login correcto ✅")

# -----------------------------
# Lectura del calendario y días
# -----------------------------
def iter_meses_base() -> List[Tuple[int, int]]:
    """Devuelve (year, month) a leer, empezando por el actual."""
    today = dt.date.today()
    res = []
    y, m = today.year, today.month
    for i in range(MESES_A_LEER):
        yy = y + (m + i - 1) // 12
        mm = (m + i - 1) % 12 + 1
        res.append((yy, mm))
    return res

def month_overview_url(year: int, month: int) -> str:
    # Ajusta la ruta si tu instancia usa otra
    # Ejemplo típico: /_duty-calendar?year=YYYY&month=MM
    params = {"year": year, "month": f"{month:02d}"}
    return urljoin(IVU_BASE_URL, "/_duty-calendar?" + urlencode(params))

def read_month_overview(session: requests.Session, year: int, month: int) -> BeautifulSoup:
    url = month_overview_url(year, month)
    r = http_get(session, url)
    return BeautifulSoup(r.text, "html.parser")

def find_day_links(soup_calendar: BeautifulSoup) -> Dict[str, str]:
    """
    Devuelve dict {YYYY-MM-DD: href_detalle}
    """
    links = {}
    for day in soup_calendar.select(SELECTORS["calendar_days"]):
        # intenta sacar la fecha de data-* o texto
        date_attr = day.get("data-date") or day.get("data-day") or ""
        date_txt = simplify(day.text)
        date_str = None

        # 1) data-date (YYYY-MM-DD)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", date_attr)
        if m:
            date_str = m.group(1)
        else:
            # 2) intenta parsear por atributos alternativos
            m2 = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", date_attr)
            if m2:
                y, mo, d = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
                date_str = f"{y:04d}-{mo:02d}-{d:02d}"

        # fallback: si no hay atributo, puede que haya un link con beginDate
        href_a = day.select_one(SELECTORS["day_href"])
        if not href_a:
            # intenta encontrar links dentro del día:
            a = day.find("a", href=True)
            if a and ("duty" in a["href"] or "beginDate" in a["href"]):
                href_a = a

        if href_a:
            href = href_a.get("href", "")
            # extrae fecha del href si no la teníamos
            if not date_str:
                m3 = re.search(r"beginDate=([0-9]{4}-[0-9]{2}-[0-9]{2})", href)
                if m3:
                    date_str = m3.group(1)

            if date_str:
                links[date_str] = href

    return links

def read_day_details(session: requests.Session, href: str) -> Tuple[str, BeautifulSoup]:
    url = urljoin(IVU_BASE_URL, href)
    r = http_get(session, url)
    return r.text, BeautifulSoup(r.text, "html.parser")

def extract_texts(el) -> List[str]:
    if not el:
        return []
    texts = []
    for node in el.stripped_strings:
        t = simplify(node)
        if t:
            texts.append(t)
    return texts

def parse_day(date_str: str, href: str, html_text: str, soup_day: BeautifulSoup) -> DiaTurno:
    header = soup_day.select_one(SELECTORS["details_header"])
    table = soup_day.select_one(SELECTORS["table_components"])

    tipo = None
    labels = []
    start_hhmm = None
    end_hhmm = None
    loc_from = None
    loc_to = None
    train_no = None

    if header:
        # intenta detectar tipo de turno (código) en cabecera
        tipo_el = header.select_one(SELECTORS["turno_tipo_header"])
        if tipo_el:
            tipo = simplify(tipo_el.text)
        labels.extend(extract_texts(header.select_one(SELECTORS["labels_text"])))

    has_hours = False

    if table:
        # Recorre filas y toma el primer start y el último end
        rows = table.select(SELECTORS["table_row"])
        for i, row in enumerate(rows):
            row_txt = simplify(row.get_text(" ", strip=True))
            # detecta tipo de componente (Descanso, LD, etc.)
            # recoge horas y ubicaciones si están
            start_cell = row.select_one(SELECTORS["cell_start"])
            end_cell   = row.select_one(SELECTORS["cell_end"])
            from_cell  = row.select_one(SELECTORS["cell_from"])
            to_cell    = row.select_one(SELECTORS["cell_to"])
            type_cell  = row.select_one(SELECTORS["cell_type"])
            train_cell = row.select_one(SELECTORS["train_number"])

            st = parse_time(start_cell.text) if start_cell else None
            en = parse_time(end_cell.text) if end_cell else None
            if st and not start_hhmm:
                start_hhmm = st
            if en:
                end_hhmm = en  # el último 'end' se queda
            if from_cell and not loc_from:
                loc_from = simplify(from_cell.text)
            if to_cell:
                loc_to = simplify(to_cell.text)
            if type_cell:
                labels.append(simplify(type_cell.text))
            if train_cell and not train_no:
                tn = simplify(train_cell.text)
                # normaliza (solo dígitos/letras)
                tn = re.sub(r"[^A-Za-z0-9\- ]+", "", tn)
                train_no = tn if tn else None

        has_hours = bool(start_hhmm and end_hhmm)

    status = detect_status(labels, has_hours)
    overnight = is_overnight(start_hhmm, end_hhmm)

    return DiaTurno(
        date=date_str,
        status=status,
        tipo=tipo,
        start=start_hhmm,
        end=end_hhmm,
        overnight=overnight,
        location_from=loc_from,
        location_to=loc_to,
        train_number=train_no,
        raw_frag_href=href,
        html_hash=sha256_text(html_text),
        notes=[],
    )

# -----------------------------
# Flujo principal
# -----------------------------
def main():
    user = os.getenv("IVU_USER")
    pwd  = os.getenv("IVU_PASS")
    if not user or not pwd:
        logger.error("Faltan variables de entorno IVU_USER / IVU_PASS.")
        sys.exit(1)

    ensure_dir(DATA_DIR)

    session = make_session()
    login_ivu(session, user, pwd)

    months = iter_meses_base()
    logger.info(f"Meses a leer: {months}")

    all_days: Dict[str, List[DiaTurno]] = {}  # "YYYY-MM" -> list[DiaTurno]

    for (yy, mm) in months:
        soup_cal = read_month_overview(session, yy, mm)
        links = find_day_links(soup_cal)
        logger.info(f"[{yy}-{mm:02d}] Días detectados: {len(links)}")

        for date_str, href in sorted(links.items()):
            try:
                html_text, soup_day = read_day_details(session, href)
                dia = parse_day(date_str, href, html_text, soup_day)

                ym = date_str[:7]
                all_days.setdefault(ym, []).append(dia)

                # Log amigable
                if dia.status == "SERVICIO":
                    logger.info(f"[{date_str}] {dia.status} {dia.start}-{dia.end} {dia.tipo or ''} {dia.location_from or ''} → {dia.location_to or ''}")
                else:
                    logger.info(f"[{date_str}] {dia.status}")
            except Exception as ex:
                logger.warning(f"[{date_str}] Error leyendo día: {ex}")
                continue

    # Guardado por mes
    # employee_id: si la UI lo muestra en alguna parte, puedes extraerlo y setearlo aquí.
    employee_id = None
    now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")

    for ym, days in sorted(all_days.items()):
        out = MesTurnos(
            employee_id=employee_id,
            generated_at=now_iso,
            source=IVU_BASE_URL.replace("https://", "").replace("http://", ""),
            year_month=ym,
            days=days,
        )

        # dataclass -> dict -> JSON serializable
        payload = {
            "employee_id": out.employee_id,
            "generated_at": out.generated_at,
            "source": out.source,
            "year_month": out.year_month,
            "days": [asdict(d) for d in out.days],
        }

        outfile = os.path.join(DATA_DIR, f"turnos_{ym}.json")
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Guardado {outfile} ({len(days)} días)")

    if not all_days:
        logger.warning("No se detectaron días/turnos. Revisa selectores o accesos.")
    
    # --- Al final de main() ---
    if not all_days:
        logger.warning("No se detectaron días/turnos. Revisa selectores o accesos.")
        marker = os.path.join(DATA_DIR, "NO_DATA.txt")
        with open(marker, "w", encoding="utf-8") as f:
            f.write(
                "No se generaron JSON de turnos.\n"
                "Posibles causas:\n"
                "- Login correcto pero sin días detectados en el mes.\n"
                "- Selectores HTML desajustados.\n"
                "- La vista del calendario no responde/URL distinta.\n"
            )
        logger.info(f"Marcador escrito: {marker}")

if __name__ == "__main__":
    main()
