# scrape_and_publish.py
# Requisitos: playwright, beautifulsoup4, icalendar, pytz
# En Actions: pip install playwright beautifulsoup4 icalendar pytz ; playwright install chromium

import os, re
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from playwright.sync_api import sync_playwright

TZ = pytz.timezone("Europe/Madrid")

# ---------- Ajusta selectores si el DOM del portal difiere ----------
LOGIN_URL = "https://wcrew-ilsa.trenitalia.it"
USERNAME_SELECTOR = 'input[name="username"]'
PASSWORD_SELECTOR = 'input[name="password"]'
LOGIN_BUTTON_SELECTOR = 'button[type="submit"], input[type="submit"]'
MONTH_DAY_SELECTOR = 'table.monthview td.day, .month-day, .wk-day'
DAY_CLICKABLE_SELECTOR = 'a.day-link, .day-cell'
DAY_TABLE_CONTAINER = "#tableview"
# --------------------------------------------------------------------

def login_and_open_month(context, user, pwd):
    page = context.new_page()
    page.goto(LOGIN_URL, timeout=60000)
    try:
        page.fill(USERNAME_SELECTOR, user, timeout=5000)
        page.fill(PASSWORD_SELECTOR, pwd, timeout=5000)
        page.click(LOGIN_BUTTON_SELECTOR)
    except Exception:
        try:
            page.evaluate("document.querySelector('form').submit()")
        except:
            pass
    page.wait_for_selector(DAY_TABLE_CONTAINER, timeout=30000)
    page.wait_for_timeout(1200)
    return page

def find_day_elements(page):
    c = page.query_selector_all(DAY_CLICKABLE_SELECTOR)
    if not c:
        c = page.query_selector_all(MONTH_DAY_SELECTOR)
    return c

def extract_html_from_day(page, day_element):
    try:
        day_element.click()
    except Exception:
        page.evaluate("(el)=>el.click()", day_element)
    page.wait_for_selector(DAY_TABLE_CONTAINER, timeout=8000)
    page.wait_for_timeout(800)
    return page.inner_html(DAY_TABLE_CONTAINER)

def parse_table_html(table_html):
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []
    table = soup.find("table")
    if table:
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds: continue
            cells = [td.get_text(strip=True) for td in tds]
            rows.append({
                "fecha": cells[0] if len(cells)>0 else "",
                "hora_inicio": cells[1] if len(cells)>1 else "",
                "hora_fin": cells[2] if len(cells)>2 else "",
                "tipo": cells[3] if len(cells)>3 else "",
                "ubicacion": cells[4] if len(cells)>4 else "",
                "tren": cells[5] if len(cells)>5 else ""
            })
    else:
        for duty in soup.select(".duty, .duty-row"):
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
    fecha_str = fecha_str.strip()
    hora_str = hora_str.strip()
    meses = {"ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
             "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12"}
    m = re.search(r"(\d{1,2})\s+([A-Za-zñ]{3,})\.?\s+(\d{4})", fecha_str)
    if m:
        d, mes, y = m.group(1), m.group(2).lower()[:3], m.group(3)
        mesn = meses.get(mes, mes)
        try:
            dt = datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", "%Y-%m-%d %H:%M")
            return TZ.localize(dt)
        except:
            try:
                dt = datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", "%Y-%m-%d %H")
                return TZ.localize(dt)
            except:
                return None
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
        if not dt_start: continue
        if not dt_end: dt_end = dt_start + timedelta(hours=8)
        monthkey = dt_start.strftime("%Y-%m")
        by_month.setdefault(monthkey, []).append({
            "start": dt_start,
            "end": dt_end,
            "summary": f"{tipo} - Tren {tren}" if tren else tipo,
            "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}"
        })
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
        page = login_and_open_month(context, user, pwd)
        day_elements = find_day_elements(page)
        if not day_elements:
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
        for ym, evs in seen_months.items():
            fname = create_ics(ym, evs)
            generated.append(fname)
        browser.close()
    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
