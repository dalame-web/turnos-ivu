# scrape_and_publish.py
# Requisitos: playwright, beautifulsoup4, icalendar, pytz
# Ejecutar en Actions: pip install playwright beautifulsoup4 icalendar pytz ; playwright install chromium

import os, re
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from playwright.sync_api import sync_playwright

TZ = pytz.timezone("Europe/Madrid")

# ---------- Ajusta selectores si el DOM del portal difiere ----------
LOGIN_URL = "https://wcrew-ilsa.trenitalia.it"
# Selectores comunes: (ajusta si hace falta)
USERNAME_SELECTOR = 'input[name="username"]'
PASSWORD_SELECTOR = 'input[name="password"]'
LOGIN_BUTTON_SELECTOR = 'button[type="submit"], input[type="submit"]'
MONTH_DAY_SELECTOR = 'table.monthview td.day, .month-day, .wk-day'  # fallback; ajustar según DOM
DAY_CLICKABLE_SELECTOR = 'a.day-link, .day-cell'  # lo que sea clicable para cargar detalle
DAY_TABLE_CONTAINER = "#tableview"  # aquí se inyecta la tabla del día según tu HTML
# --------------------------------------------------------------------

def login_and_open_month(context, user, pwd):
    page = context.new_page()
    page.goto(LOGIN_URL, timeout=60000)
    # Login - ajustar selectores si son distintos
    try:
        page.fill(USERNAME_SELECTOR, user, timeout=5000)
        page.fill(PASSWORD_SELECTOR, pwd, timeout=5000)
        page.click(LOGIN_BUTTON_SELECTOR)
    except Exception:
        # intentar otras alternativas (por si el login está en otra ruta)
        pass
    # espera a que la vista mensual esté disponible
    page.wait_for_selector(DAY_TABLE_SELECTOR if (DAY_TABLE_SELECTOR := "div.month") else "#tableview", timeout=30000)
    # aseguramos que la página con la vista mes esté cargada
    page.wait_for_timeout(1500)
    return page

def find_day_elements(page):
    # devuelve una lista de element handles clicables que abren el detalle de día
    # probamos varios selectores
    candidates = page.query_selector_all(DAY_CLICKABLE_SELECTOR)
    if not candidates:
        candidates = page.query_selector_all(MONTH_DAY_SELECTOR)
    return candidates

def extract_html_from_day(page, day_element):
    # click en el día y esperar que #tableview se llene
    try:
        day_element.click()
    except Exception:
        # si no es clicable, intentar ejecutar JS para simular click
        page.evaluate("(el)=>el.click()", day_element)
    page.wait_for_selector(DAY_TABLE_CONTAINER, timeout=8000)
    page.wait_for_timeout(800)  # esperar render
    return page.inner_html(DAY_TABLE_CONTAINER)

def parse_table_html(table_html):
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []
    table = soup.find("table")
    if table:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds: continue
            cells = [td.get_text(strip=True) for td in tds]
            # Intentar mapear por posición por defecto:
            rows.append({
                "fecha": cells[0] if len(cells)>0 else "",
                "hora_inicio": cells[1] if len(cells)>1 else "",
                "hora_fin": cells[2] if len(cells)>2 else "",
                "tipo": cells[3] if len(cells)>3 else "",
                "ubicacion": cells[4] if len(cells)>4 else "",
                "tren": cells[5] if len(cells)>5 else ""
            })
    else:
        # si no hay table, buscar tarjetas
        for duty in soup.select(".duty, .duty-row"):
            text = duty.get_text(" | ", strip=True)
            # heurística simple: parse por separadores
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
    fecha_str = fecha_str.strip()
    hora_str = hora_str.strip()
    meses = {
        "ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
        "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12"
    }
    m = re.search(r"(\d{1,2})\s+([A-Za-zñ]{3,})\.?\s+(\d{4})", fecha_str)
    if m:
        d, mes, y = m.group(1), m.group(2).lower()[:3], m.group(3)
        mesn = meses.get(mes, mes)
        for fmt in ("%Y-%m-%d %H:%M","%d-%m-%Y %H:%M","%d/%m/%Y %H:%M","%d/%m/%Y %H"):
            try:
                dt = datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", fmt)
                return TZ.localize(dt)
            except:
                pass
    # intentos alternativos
    for fmt_f in ("%d/%m/%Y","%Y-%m-%d"):
        for fmt_h in ("%H:%M","%H"):
            try:
                dt = datetime.strptime(f"{fecha_str} {hora_str}", f"{fmt_f} {fmt_h}")
                return TZ.localize(dt)
            except:
                pass
    return None

def events_from_rows(rows):
    by_month = {}
    for r in rows:
        fecha = r.get("fecha","")
        inicio = r.get("hora_inicio","")
        fin = r.get("hora_fin","")
        tipo = r.get("tipo","")
        ubic = r.get("ubicacion","")
        tren = r.get("tren","")
        dt_start = parse_datetime(fecha, inicio) if fecha and inicio else None
        dt_end = parse_datetime(fecha, fin) if fecha and fin else None
        if not dt_start:
            continue
        if not dt_end:
            dt_end = dt_start + timedelta(hours=8)  # fallback jornada completa
        monthkey = dt_start.strftime("%Y-%m")
        ev = {
            "start": dt_start,
            "end": dt_end,
            "summary": f"{tipo} - Tren {tren}" if tren else tipo,
            "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}"
        }
        by_month.setdefault(monthkey, []).append(ev)
    return by_month

def create_ics(year_month, events):
    cal = Calendar()
    cal.add('prodid', '-//Turnos IVU//')
    cal.add('version', '2.0')
    for ev in events:
        e = Event()
        e.add('summary', ev['summary'] or "Turno")
        e.add('dtstart', ev['start'])
        e.add('dtend', ev['end'])
        e.add('description', ev['description'])
        e.add('dtstamp', datetime.now(tz=TZ))
        cal.add_component(e)
    fname = f"turnos_{year_month}.ics"
    with open(fname, "wb") as f:
        f.write(cal.to_ical())
    return fname

def main():
    user = os.environ.get("IVU_USER")
    pwd = os.environ.get("IVU_PASS")
    if not user or not pwd:
        raise RuntimeError("Se requieren IVU_USER e IVU_PASS en variables de entorno")
    generated = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        # Login
        page.goto(LOGIN_URL, timeout=60000)
        # intentos robustos de login
        try:
            page.fill(USERNAME_SELECTOR, user)
            page.fill(PASSWORD_SELECTOR, pwd)
            page.click(LOGIN_BUTTON_SELECTOR)
        except Exception:
            # si fallan selectores, intenta simplemente submit del primer formulario
            try:
                page.evaluate("document.querySelector('form').submit()")
            except:
                pass
        # esperar que la vista mensual esté lista
        page.wait_for_selector(DAY_TABLE_CONTAINER, timeout=30000)
        page.wait_for_timeout(1200)
        # recolectar días en la vista mensual
        day_elements = find_day_elements(page)
        if not day_elements:
            # si no encontró elementos, intenta obtener enlaces <a> dentro de un contenedor
            day_elements = page.query_selector_all("a[href*='duty-details'], a[href*='day']")
        seen_months = {}
        for de in day_elements:
            try:
                html = extract_html_from_day(page, de)
            except Exception:
                continue
            rows = parse_table_html(html)
            evs_by_month = events_from_rows(rows)
            for ym, evs in evs_by_month.items():
                seen_months.setdefault(ym, []).extend(evs)
        # crear ICS por mes
        for ym, evs in seen_months.items():
            fname = create_ics(ym, evs)
            generated.append(fname)
        browser.close()
    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
