import os, re, pathlib
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

# ====== CONFIG BÁSICA ======
BASE = "https://wcrew-ilsa.trenitalia.it"
LOGIN_URL = f"{BASE}"  # nos redirige al login
TZ = pytz.timezone("Europe/Madrid")

# ====== UTILIDADES ======
def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def save_debug(page, name: str, html_override: str | None = None):
    """Guarda HTML + captura para diagnóstico en carpeta debug/."""
    ensure_dir("debug")
    try:
        html = html_override if html_override is not None else page.content()
        with open(f"debug/{name}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass
    try:
        page.screenshot(path=f"debug/{name}.png", full_page=True)
    except Exception:
        pass

# ====== PARSERS ======
def parse_table_html(table_html: str):
    """
    Extrae filas con columnas:
    Fecha, Hora Inicio, Hora Fin, Tipo, Ubicación, Tren.
    Funciona con tabla TH/TD y tiene un fallback por tarjetas.
    """
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []

    table = soup.find("table")
    if table:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        header_map = {}
        expected = {
            "fecha": ["fecha", "date", "giorno"],
            "hora_inicio": ["hora inicio", "inicio", "start", "inizio"],
            "hora_fin": ["hora fin", "fin", "end", "fine"],
            "tipo": ["tipo", "type", "servicio", "duty"],
            "ubicacion": ["ubicación", "ubicacion", "location", "luogo"],
            "tren": ["tren", "train", "n° treno", "numero tren"],
        }
        for i, h in enumerate(headers):
            for k, alias in expected.items():
                if any(a in h for a in alias):
                    header_map[k] = i

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cells = [td.get_text(strip=True) for td in tds]

            def cell(k, idx):
                return (
                    cells[header_map[k]]
                    if k in header_map and header_map[k] < len(cells)
                    else (cells[idx] if idx < len(cells) else "")
                )

            rows.append(
                {
                    "fecha": cell("fecha", 0),
                    "hora_inicio": cell("hora_inicio", 1),
                    "hora_fin": cell("hora_fin", 2),
                    "tipo": cell("tipo", 3),
                    "ubicacion": cell("ubicacion", 4),
                    "tren": cell("tren", 5),
                }
            )
        return rows

    # Fallback por tarjetas/divs
    for duty in soup.select(".duty, .duty-row, .ivu-row, .row"):
        text = duty.get_text(" | ", strip=True)
        parts = [p.strip() for p in text.split("|")]
        rows.append(
            {
                "fecha": parts[0] if len(parts) > 0 else "",
                "hora_inicio": parts[1] if len(parts) > 1 else "",
                "hora_fin": parts[2] if len(parts) > 2 else "",
                "tipo": parts[3] if len(parts) > 3 else "",
                "ubicacion": parts[4] if len(parts) > 4 else "",
                "tren": parts[5] if len(parts) > 5 else "",
            }
        )
    return rows

def parse_datetime(fecha_str: str, hora_str: str):
    fecha_str = (fecha_str or "").strip()
    hora_str = (hora_str or "").strip()
    meses = {
        "ene": "01",
        "feb": "02",
        "mar": "03",
        "abr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dic": "12",
    }
    # Formato típico "19 oct. 2025"
    m = re.search(r"(\d{1,2})\s+([A-Za-zñ]{3,})\.?\s+(\d{4})", fecha_str)
    if m:
        d, mes, y = m.group(1), m.group(2).lower()[:3], m.group(3)
        mesn = meses.get(mes, mes)
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H"):
            try:
                return TZ.localize(datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", fmt))
            except Exception:
                pass
    for ff in ("%d/%m/%Y", "%Y-%m-%d"):
        for fh in ("%H:%M", "%H"):
            try:
                return TZ.localize(datetime.strptime(f"{fecha_str} {hora_str}", f"{ff} {fh}"))
            except Exception:
                pass
    return None

def rows_to_events(rows):
    by_month = {}
    for r in rows:
        start = parse_datetime(r.get("fecha", ""), r.get("hora_inicio", ""))
        end = parse_datetime(r.get("fecha", ""), r.get("hora_fin", ""))
        if not start:
            continue
        if not end:
            end = start + timedelta(hours=8)
        ym = start.strftime("%Y-%m")
        tipo = r.get("tipo", "")
        ubic = r.get("ubicacion", "")
        tren = r.get("tren", "")
        by_month.setdefault(ym, []).append(
            {
                "summary": f"{tipo} - Tren {tren}" if tren else (tipo or "Turno"),
                "start": start,
                "end": end,
                "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}",
            }
        )
    return by_month

def create_ics(year_month: str, events: list[dict]):
    cal = Calendar()
    cal.add("prodid", "-//Turnos IVU//")
    cal.add("version", "2.0")
    for ev in events:
        e = Event()
        e.add("summary", ev["summary"])
        e.add("dtstart", ev["start"])
        e.add("dtend", ev["end"])
        e.add("description", ev["description"])
        e.add("dtstamp", datetime.now(TZ))
        cal.add_component(e)
    fname = f"turnos_{year_month}.ics"
    with open(fname, "wb") as f:
        f.write(cal.to_ical())
    return fname

# ====== FUNCIONES ESPECÍFICAS DE TU PORTAL ======
def login(context, user: str, pwd: str):
    """
    Login usando el formulario real (#j_username / #j_password).
    No navegamos a /mbweb/duties (en tu instancia no está mapeado).
    """
    page = context.new_page()
    page.goto(LOGIN_URL, timeout=60000)
    page.wait_for_load_state("domcontentloaded")
    save_debug(page, "01_login_landing")

    # Intento normal con click de botón
    try:
        page.fill("#j_username", user, timeout=8000)
        page.fill("#j_password", pwd, timeout=8000)
        page.click('input.login_button[type="submit"]')
    except Exception:
        # Fallback: enviar por JS (un solo argumento)
        page.evaluate(
            """(creds)=>{ 
                const uf=document.querySelector('#j_username'); 
                const pf=document.querySelector('#j_password'); 
                if(uf) uf.value=creds.u; if(pf) pf.value=creds.p; 
                const f=document.querySelector('form#login_form'); 
                if(f){ f.action='/mbweb/j_security_check'; f.method='POST'; f.submit(); }
            }""",
            {"u": user, "p": pwd},
        )

    # Esperar que esté en cualquier módulo post-login; si no, seguimos igual
    try:
        page.wait_for_url(re.compile(r"/mbweb/"), timeout=25000)
    except PwTimeout:
        save_debug(page, "02_after_submit_timeout")

    save_debug(page, "03_after_login")
    return page

def get_month_html_from_page(page):
    """Devuelve SOLO el HTML de #tableview de la página actual (mes)."""
    full = page.content()
    soup = BeautifulSoup(full, "html.parser")
    tv = soup.select_one("#tableview")
    return str(tv) if tv else full

def extract_dates_and_empid(month_html: str):
    """
    Extrae todas las fechas (beginDate=YYYY-MM-DD) y allocatedEmployeeId (si está).
    En tu instancia los días enlazan a: duty-details?beginDate=...&allocatedEmployeeId=NNN
    """
    dates = set(re.findall(r"beginDate=(\d{4}-\d{2}-\d{2})", month_html))
    empid = None
    m = re.search(r"allocatedEmployeeId=(\d+)", month_html)
    if m:
        empid = m.group(1)
    return sorted(dates), empid

def fetch_day_html_via_fetch(page, ymd: str, empid: str | None):
    """Usa fetch (desde la misma sesión navegador) para descargar el HTML del día."""
    url = f"/mbweb/duty-details?beginDate={ymd}"
    if empid:
        url += f"&allocatedEmployeeId={empid}"
    html = page.evaluate(
        """(u)=>fetch(u,{credentials:'same-origin'}).then(r=>r.text())""",
        url,
    )
    return html

# ====== MAIN ======
def main():
    user = os.environ.get("IVU_USER")
    pwd = os.environ.get("IVU_PASS")
    if not user or not pwd:
        raise RuntimeError("Faltan IVU_USER/IVU_PASS")

    ensure_dir("debug")
    generated = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",
            ],
        )
        context = browser.new_context(
            locale="es-ES",
            timezone_id="Europe/Madrid",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/118.0.0.0 Safari/537.36"
            ),
        )
        # Quitar navigator.webdriver
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        page = login(context, user, pwd)

        # **NO** navegamos a /mbweb/duties: ya estamos en “Turnos” con el mes cargado
        month_html = get_month_html_from_page(page)
        save_debug(page, "04_month_html", html_override=month_html)

        dates, empid = extract_dates_and_empid(month_html)
        if not dates:
            save_debug(page, "05_no_dates_in_month", html_override=month_html)
            raise RuntimeError("No se encontraron fechas en el mes")

        # Pedir todos los días usando el mismo patrón de la UI y parsear
        all_events = {}
        for ymd in dates:
            day_html = fetch_day_html_via_fetch(page, ymd, empid)
            # Si quieres debug de un día: descomenta la línea siguiente
            # save_debug(page, f"day_{ymd}", html_override=day_html)
            rows = parse_table_html(day_html)
            for ym, evs in rows_to_events(rows).items():
                all_events.setdefault(ym, []).extend(evs)

        for ym, evs in all_events.items():
            fname = create_ics(ym, evs)
            generated.append(fname)

        browser.close()

    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
