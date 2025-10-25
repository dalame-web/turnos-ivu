# scrape_and_publish.py
# Funciona con la estructura real de IVU que has pasado:
# - Login: j_username / j_password -> /mbweb/j_security_check
# - Mes:   /mbweb/_-duty-table  (render en #tableview)
# - Día:   /mbweb/_-duty-details-day?beginDate=YYYY-MM-DD (&showUserInfo=...)
# Genera turnos_YYYY-MM.ics en /calendars (publicados por gh-pages)

import os, re, pathlib
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

BASE = "https://wcrew-ilsa.trenitalia.it"
LOGIN_URL = f"{BASE}"
LOGIN_POST = f"{BASE}/mbweb/j_security_check"
DUTIES_URL = f"{BASE}/mbweb/duties"
MONTH_ENDPOINT = f"{BASE}/mbweb/_-duty-table"
DAY_ENDPOINT = f"{BASE}/mbweb/_-duty-details-day"

TZ = pytz.timezone("Europe/Madrid")

def _ensure_dir(path):
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)

def save_debug(page, name, content=None):
    _ensure_dir(f"debug/{name}.png")
    try:
        page.screenshot(path=f"debug/{name}.png", full_page=True)
    except Exception:
        pass
    if content is not None:
        with open(f"debug/{name}.html", "w", encoding="utf-8") as f:
            f.write(content)
    else:
        try:
            html = page.content()
            with open(f"debug/{name}.html", "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass

def login(context, user, pwd):
    page = context.new_page()
    page.goto(LOGIN_URL, timeout=60000)  # redirige al login
    # Rellenar exactamente los campos que muestra tu HTML (j_username / j_password)
    page.wait_for_selector('form#login_form', timeout=20000)
    page.fill('#j_username', user)
    page.fill('#j_password', pwd)
    page.click('input.login_button[type="submit"]')
    # Esperamos que aparezca el menú principal / página post-login
    try:
        page.wait_for_url(re.compile(r"/mbweb/(duties|messages|absence-overview|holiday-request)"), timeout=25000)
    except PwTimeout:
        # si no redirige aún, ve explícitamente a duties
        page.goto(DUTIES_URL, timeout=30000)
    return page

def get_storage_state(context):
    # Obtenemos cookies/estado y lo usamos en llamadas HTTP a los endpoints AJAX
    state = context.storage_state()
    return state

def new_api_context(p, storage_state):
    # Crea un contexto de peticiones con las cookies autenticadas
    return p.request.new_context(base_url=BASE, storage_state=storage_state,
                                 extra_http_headers={"Accept-Language":"es-ES"})

def fetch_month_html(api):
    # Descarga el fragmento HTML que IVU inyecta en #tableview para la vista mes
    resp = api.get("/mbweb/_-duty-table")
    resp.ok or (_ for _ in ()).throw(RuntimeError(f"GET _-duty-table status {resp.status}"))
    return resp.text()

def extract_dates_from_month(html_month):
    # En el fragmento suelen existir enlaces o JS con beginDate=YYYY-MM-DD
    dates = set()
    # 1) Busca parámetros beginDate=YYYY-MM-DD
    for m in re.finditer(r"beginDate=(\d{4}-\d{2}-\d{2})", html_month):
        dates.add(m.group(1))
    # 2) Si no hubiera, intenta encontrar celdas/días con atributos data
    soup = BeautifulSoup(html_month, "html.parser")
    for a in soup.find_all("a", href=True):
        m = re.search(r"beginDate=(\d{4}-\d{2}-\d{2})", a["href"])
        if m:
            dates.add(m.group(1))
    # Como respaldo, busca data-begin-date
    for tag in soup.find_all(attrs={"data-begin-date": True}):
        dates.add(tag["data-begin-date"])
    return sorted(dates)

def fetch_day_html(api, ymd):
    # Pide el detalle del día (lo que IVU inyecta en #tableview)
    params = f"beginDate={ymd}&showUserInfo=true"
    resp = api.get(f"/mbweb/_-duty-details-day?{params}")
    resp.ok or (_ for _ in ()).throw(RuntimeError(f"GET _-duty-details-day {ymd} status {resp.status}"))
    return resp.text()

def parse_table_html(table_html):
    """
    Extrae filas con columnas: Fecha, Hora Inicio, Hora Fin, Tipo, Ubicación, Tren
    Intenta mapear por cabecera; si no, usa posiciones.
    """
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []

    # 1) Si hay tabla con TH
    table = soup.find("table")
    if table:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        # Mapeo por nombre
        header_map = {}
        expected = {
            "fecha": ["fecha","date","giorno"],
            "hora_inicio": ["hora inicio","inicio","start","inizio"],
            "hora_fin": ["hora fin","fin","end","fine"],
            "tipo": ["tipo","type","servicio","duty"],
            "ubicacion": ["ubicación","ubicacion","location","luogo"],
            "tren": ["tren","train","n° treno","numero tren"]
        }
        for i, h in enumerate(headers):
            for key, aliases in expected.items():
                if any(a in h for a in aliases):
                    header_map[key] = i

        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds: 
                continue
            cells = [td.get_text(strip=True) for td in tds]
            def cell(key, default_idx):
                if key in header_map and header_map[key] < len(cells):
                    return cells[header_map[key]]
                return cells[default_idx] if default_idx < len(cells) else ""
            rows.append({
                "fecha":      cell("fecha", 0),
                "hora_inicio":cell("hora_inicio", 1),
                "hora_fin":   cell("hora_fin", 2),
                "tipo":       cell("tipo", 3),
                "ubicacion":  cell("ubicacion", 4),
                "tren":       cell("tren", 5),
            })
        return rows

    # 2) Si IVU renderiza tarjetas/divs
    for duty in soup.select(".duty, .duty-row, .ivu-row, .row"):
        text = duty.get_text(" | ", strip=True)
        parts = [p.strip() for p in text.split("|")]
        rows.append({
            "fecha": parts[0] if len(parts)>0 else "",
            "hora_inicio": parts[1] if len(parts)>1 else "",
            "hora_fin": parts[2] if len(parts)>2 else "",
            "tipo": parts[3] if len(parts)>3 else "",
            "ubicacion": parts[4] if len(parts)>4 else "",
            "tren": parts[5] if len(parts)>5 else ""
        })
    return rows

def parse_datetime(fecha_str, hora_str):
    fecha_str = (fecha_str or "").strip()
    hora_str  = (hora_str or "").strip()
    # Formato típico "20 oct. 2025" que aparece en cabeceras de IVU
    meses = {"ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
             "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12"}
    m = re.search(r"(\d{1,2})\s+([A-Za-zñ]{3,})\.?\s+(\d{4})", fecha_str)
    if m:
        d, mes, y = m.group(1), m.group(2).lower()[:3], m.group(3)
        mesn = meses.get(mes, mes)
        for fmt in ("%Y-%m-%d %H:%M","%Y-%m-%d %H"):
            try:
                dt = datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", fmt)
                return TZ.localize(dt)
            except:
                pass
    # Alternativas
    for ff in ("%d/%m/%Y","%Y-%m-%d"):
        for fh in ("%H:%M","%H"):
            try:
                dt = datetime.strptime(f"{fecha_str} {hora_str}", f"{ff} {fh}")
                return TZ.localize(dt)
            except:
                pass
    return None

def events_from_rows(rows):
    by_month = {}
    for r in rows:
        fecha = r.get("fecha","")
        ini   = r.get("hora_inicio","")
        fin   = r.get("hora_fin","")
        tipo  = r.get("tipo","")
        ubic  = r.get("ubicacion","")
        tren  = r.get("tren","")
        start = parse_datetime(fecha, ini) if fecha and ini else None
        end   = parse_datetime(fecha, fin) if fecha and fin else None
        if not start:
            # si no hay fecha/hora claras, ignora
            continue
        if not end:
            end = start + timedelta(hours=8)
        ym = start.strftime("%Y-%m")
        by_month.setdefault(ym, []).append({
            "summary": f"{tipo} - Tren {tren}" if tren else (tipo or "Turno"),
            "start": start,
            "end": end,
            "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}"
        })
    return by_month

def create_ics(year_month, events):
    cal = Calendar()
    cal.add('prodid','-//Turnos IVU//')
    cal.add('version','2.0')
    for ev in events:
        e = Event()
        e.add('summary', ev['summary'])
        e.add('dtstart', ev['start'])
        e.add('dtend', ev['end'])
        e.add('description', ev['description'])
        e.add('dtstamp', datetime.now(TZ))
        cal.add_component(e)
    fname = f"turnos_{year_month}.ics"
    with open(fname, "wb") as f:
        f.write(cal.to_ical())
    return fname

def main():
    user = os.environ.get("IVU_USER")
    pwd  = os.environ.get("IVU_PASS")
    if not user or not pwd:
        raise RuntimeError("Faltan IVU_USER/IVU_PASS")

    generated = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox","--disable-gpu"
        ])
        context = browser.new_context(
            locale="es-ES",
            timezone_id="Europe/Madrid",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/118.0.0.0 Safari/537.36")
        )
        # Quitar navigator.webdriver
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        # --- LOGIN ---
        page = login(context, user, pwd)

        # Crea un contexto de peticiones con las cookies del login
        api = new_api_context(p, context.storage_state())

        # --- MES: obtener fragmento HTML del mes ---
        html_month = fetch_month_html(api)
        if not html_month or "beginDate" not in html_month:
            save_debug(page, "month_empty", content=html_month or "")
            raise RuntimeError("No se pudo obtener el mes (sin beginDate)")

        dates = extract_dates_from_month(html_month)
        if not dates:
            save_debug(page, "month_no_dates", content=html_month)
            raise RuntimeError("No se encontraron fechas en el mes")

        # --- DÍA: pedir detalle por cada fecha ---
        all_events = {}
        for ymd in dates:
            html_day = fetch_day_html(api, ymd)
            rows = parse_table_html(html_day)
            bym = events_from_rows(rows)
            for ym, evs in bym.items():
                all_events.setdefault(ym, []).extend(evs)

        # --- ICS por mes ---
        for ym, evs in all_events.items():
            fname = create_ics(ym, evs)
            generated.append(fname)

        browser.close()

    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
