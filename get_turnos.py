#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
get_turnos.py — Scraper IVU (Playwright) → turnos_YYYY-MM.json

Requisitos:
  pip install playwright python-dateutil pytz
  python -m playwright install chromium

ENV:
  IVU_BASE_URL       # https://wcrew-ilsa.trenitalia.it
  IVU_USER
  IVU_PASS
  OUTPUT_DIR         # ./out (por defecto)
  TARGET_YEAR        # YYYY (opcional)
  TARGET_MONTH       # MM   (opcional)
  IVU_CALENDAR_URL   # URL directa a vista mensual (opcional, recomendado)
  INCLUIR_DESCANSOS  # "true"/"false" (por defecto "true")

Notas:
- Los selectores son genéricos y suelen funcionar, pero cada IVU puede variar.
- Ajusta los marcados con  [A JUSTAR]  si tu DOM difiere (te indicaré dónde).
- El script crea un JSON con esquema estable: days[] con SERVICIO/LD/I/DESCANSO/LIBRE.
"""

from __future__ import annotations
import os, re, sys, json, hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from typing import List, Optional
from dateutil import tz
import pytz

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

MADRID_TZ = tz.gettz("Europe/Madrid")

def sha256_text(t: str) -> str:
    return "sha256:" + hashlib.sha256(t.encode("utf-8")).hexdigest()

def hm_to_minutes(hm: Optional[str]) -> Optional[int]:
    if not hm:
        return None
    m = re.match(r"^([01]?\d|2[0-3]):([0-5]\d)$", hm)
    if not m:
        return None
    h, mi = int(m.group(1)), int(m.group(2))
    return h*60 + mi

@dataclass
class DayEntry:
    date: str                    # YYYY-MM-DD
    status: str                  # SERVICIO | DESCANSO | LD | I | LIBRE
    tipo: Optional[str] = None
    start: Optional[str] = None  # HH:MM
    end: Optional[str] = None    # HH:MM
    overnight: bool = False
    all_day: bool = False
    location_from: Optional[str] = None
    location_to: Optional[str] = None
    train_number: Optional[str] = None
    raw_frag_href: Optional[str] = None
    html_hash: Optional[str] = None
    notes: List[str] = None

@dataclass
class MonthDoc:
    employee_id: Optional[str]
    generated_at: str
    source: str
    year: int
    month: int
    days: List[DayEntry]

class IvuScraper:
    def __init__(self, base_url: str, user: str, password: str, incluir_descansos: bool = True):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.incluir_descansos = incluir_descansos

    # ---------- LOGIN ----------
    def login(self, page):
        page.goto(self.base_url, wait_until="domcontentloaded")
        # [A JUSTAR] Si tu portal redirige a SSO, verás inputs distintos.
        # Intento 1: formulario clásico
        try:
            if page.locator("input[name='username']").count():
                page.fill("input[name='username']", self.user)
            if page.locator("input[name='password']").count():
                page.fill("input[name='password']", self.password)

            # Botón por texto/rol/submit
            clicked = False
            candidates = [
                ("role", r"(Iniciar sesión|Entrar|Login)"),
                ("css", "button[type='submit']"),
                ("css", "input[type='submit']"),
            ]
            for mode, patt in candidates:
                try:
                    if mode == "role":
                        loc = page.get_by_role("button", name=re.compile(patt, re.I))
                        if loc.count():
                            loc.first.click()
                            clicked = True
                            break
                    else:
                        if page.locator(patt).count():
                            page.locator(patt).first.click()
                            clicked = True
                            break
                except PWTimeout:
                    pass

            if not clicked:
                # Si no hay botón, puede ser SSO inmediato
                # espera a que navegue
                pass

            page.wait_for_load_state("networkidle", timeout=15000)
        except PWTimeout:
            # Intento 2: flujo SSO (genérico)
            # [A JUSTAR] Añade aquí selectors de tu SSO (AzureAD/Google/etc.)
            # Ejemplo genérico (comentado):
            # page.fill("input[type='email']", self.user)
            # page.click("button:has-text('Siguiente')")
            # page.fill("input[type='password']", self.password)
            # page.click("button:has-text('Iniciar sesión')")
            page.wait_for_load_state("networkidle", timeout=15000)

    # ---------- ACCESO A CALENDARIO ----------
    def open_month_view(self, page, year: int, month: int):
        cal_url = os.getenv("IVU_CALENDAR_URL")
        if cal_url:
            # Ruta directa (recomendado si la tienes)
            target = cal_url
            # Si admite query de año/mes, mejor: p.ej. ?year=YYYY&month=MM
            # [A JUSTAR] tu portal concreto
            page.goto(target, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            return

        # Fallback por clicks de menú
        # [A JUSTAR] según tu portal: textos como "Calendario", "Mi planificación", etc.
        opened = False
        for label in ["Calendario", "Calendar", "Mi planificación", "Schedule"]:
            try:
                loc = page.get_by_role("link", name=re.compile(label, re.I))
                if loc.count():
                    loc.first.click()
                    page.wait_for_load_state("networkidle")
                    opened = True
                    break
            except PWTimeout:
                pass
        if not opened:
            # Último recurso: si el home ya muestra el calendario mensual, sigue
            pass

        # [Opcional] navegar al mes/año concretos (si hay selectores o flechas)
        # [A JUSTAR] según tu calendario (botones prev/next o selector de mes/año).
        # Aquí no cambiamos el mes; recorremos el mes actual visible.

    # ---------- PARSE DE UN DÍA ----------
    def parse_day(self, page, d: date) -> DayEntry:
        notes: List[str] = []

        # Abre el detalle del día (celda del calendario)
        # [A JUSTAR] Selector robusto para tu calendario
        day_selectors = [
            f"[data-day='{d.isoformat()}']",
            f"td[aria-label*='{d.day}']",
            f"td.calendar-day:has-text('{d.day}')",
            f"button[aria-label*='{d.day}']",
        ]
        opened = False
        for sel in day_selectors:
            if page.locator(sel).count():
                page.locator(sel).first.click()
                page.wait_for_timeout(250)
                opened = True
                break
        if not opened:
            # Si no hay detalle, consideramos LIBRE
            return DayEntry(date=d.isoformat(), status="LIBRE", notes=["Sin detalle de día"], html_hash=None)

        # Capturamos el HTML del panel/modal de detalle (o de la página si es inline)
        html = page.content()
        html_hash = sha256_text(html)

        # Heurísticas:
        # 1) ¿Hay tabla de componentes?
        has_table = bool(re.search(r"duty-components-table", html))

        # 2) Tiempos de inicio/fin (cabecera o cuadro)
        def find_hhmm(labels_regex: str) -> Optional[str]:
            m = re.search(labels_regex + r"[^0-9]([0-2]?\d:[0-5]\d)", html, re.I)
            return m.group(1) if m else None

        start_hm = find_hhmm(r"(Inicio|Start)")
        end_hm   = find_hhmm(r"(T[ée]rmino|Fin|End)")

        # 3) Tipo de turno
        tipo = None
        m_tipo = re.search(r"(Tipo|Type)[^A-Za-z0-9]*([A-Z0-9\-]{2,})", html, re.I)
        if m_tipo:
            tipo = m_tipo.group(2)

        # 4) Etiquetas descanso/LD/I
        tag_descanso = re.search(r"\b(Descanso|Rest)\b", html, re.I)
        tag_ld       = re.search(r"\bLD\b", html)
        tag_i        = re.search(r"(?:^|\s)I(?:\s|$|<)", html)

        # 5) Localizaciones (primera y última)
        # [A JUSTAR] Si tu DOM usa clases distintas, actualiza esta regex
        locs = re.findall(r"(start_location_long_name|end_location_long_name)[^>]*>\s*([^<]+)\s*<", html, re.I)
        loc_from = locs[0][1].strip() if locs else None
        loc_to   = locs[-1][1].strip() if locs else None

        # 6) Nº de tren
        train_number = None
        m_tn = re.search(r"(?:trip|train)_number[^>]*>\s*([^<]+)\s*<", html, re.I)
        if m_tn:
            train_number = m_tn.group(1).strip()

        # Clasificación
        status = "LIBRE"
        all_day = False
        overnight = False

        if tag_descanso and self.incluir_descansos:
            status = "DESCANSO"
            all_day = True
        elif tag_ld and self.incluir_descansos:
            status = "LD"
            all_day = True
        elif tag_i and self.incluir_descansos:
            status = "I"
            all_day = True
        elif has_table or (start_hm and end_hm):
            status = "SERVICIO"
        else:
            status = "LIBRE"

        # Dormida si cruza medianoche
        if status == "SERVICIO" and start_hm and end_hm:
            s = hm_to_minutes(start_hm)
            e = hm_to_minutes(end_hm)
            if s is not None and e is not None and e < s:
                overnight = True
                notes.append("Cruce de medianoche (dormida)")

        return DayEntry(
            date=d.isoformat(),
            status=status,
            tipo=tipo,
            start=start_hm,
            end=end_hm,
            overnight=overnight,
            all_day=all_day,
            location_from=loc_from,
            location_to=loc_to,
            train_number=train_number,
            raw_frag_href=None,
            html_hash=html_hash,
            notes=notes or []
        )

# ----------------------- MAIN -----------------------
def main():
    base_url = os.getenv("IVU_BASE_URL")
    user = os.getenv("IVU_USER")
    password = os.getenv("IVU_PASS")
    out_dir = os.getenv("OUTPUT_DIR", "./out")
    incluir_descansos = (os.getenv("INCLUIR_DESCANSOS", "true").lower() == "true")

    if not base_url or not user or not password:
        print("Faltan variables de entorno: IVU_BASE_URL, IVU_USER, IVU_PASS", file=sys.stderr)
        sys.exit(1)

    now = datetime.now(MADRID_TZ)
    year = int(os.getenv("TARGET_YEAR", now.year))
    month = int(os.getenv("TARGET_MONTH", now.month))

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"turnos_{year}-{month:02d}.json")

    # Días del mes
    first = date(year, month, 1)
    if month == 12:
        next_first = date(year + 1, 1, 1)
    else:
        next_first = date(year, month + 1, 1)
    days = [first + timedelta(days=i) for i in range((next_first - first).days)]

    entries: List[DayEntry] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(timezone_id="Europe/Madrid")
        page = context.new_page()

        scraper = IvuScraper(base_url, user, password, incluir_descansos)
        scraper.login(page)
        scraper.open_month_view(page, year, month)

        for d in days:
            try:
                e = scraper.parse_day(page, d)
                entries.append(e)
                print(f"[{e.date}] {e.status:9} {e.start or ''}–{e.end or ''} {e.tipo or ''}")
            except Exception as ex:
                print(f"ERROR día {d}: {ex}", file=sys.stderr)

        browser.close()

    doc = MonthDoc(
        employee_id=None,  # [A JUSTAR] si puedes leerlo del portal, rellénalo
        generated_at=datetime.now(MADRID_TZ).isoformat(),
        source=base_url.replace("https://", "").replace("http://", ""),
        year=year,
        month=month,
        days=entries
    )

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "employee_id": doc.employee_id,
            "generated_at": doc.generated_at,
            "source": doc.source,
            "year": doc.year,
            "month": doc.month,
            "days": [asdict(d) for d in doc.days]
        }, f, ensure_ascii=False, indent=2)

    print(f"\nWrote {out_path} ({len(entries)} días)")

if __name__ == "__main__":
    main()
