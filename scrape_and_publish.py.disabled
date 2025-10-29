import os, re, pathlib
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

BASE = "https://wcrew-ilsa.trenitalia.it"
LOGIN_URL = f"{BASE}"
TZ = pytz.timezone("Europe/Madrid")

# ---------------- util ----------------
def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def save_debug(page, name: str, html_override: str | None = None):
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

# ---------------- parsers ----------------
HORA_RE = r"([01]?\d|2[0-3]):[0-5]\d"

def parse_day_ivu(day_html: str):
    """
    Parser específico para el detalle diario de IVU:
    lee cabecera (Fecha / Número de turno / Inicio / Término) y
    deduce ubicación (origen→destino) y número de tren si aparece.
    Devuelve una lista con UNA fila (nuestro formato estándar).
    """
    soup = BeautifulSoup(day_html, "html.parser")

    # 1) Cabecera con info de día
    header = soup.select_one("table.allocation-info, table.duty_header_attribute, table.table-header-block")
    if not header:
        return []

    def get_cont(label):
        # Busca un <div class="desc">LABEL</div> y lee el siguiente <div class="cont">
        for row in header.select("tr"):
            descs = row.select("div.desc")
            conts = row.select("div.cont")
            for i, d in enumerate(descs):
                t = d.get_text(strip=True).lower()
                if label in t and i < len(conts):
                    return conts[i].get_text(" ", strip=True)
        return ""

    fecha = get_cont("fecha")
    tipo  = get_cont("número de turno") or get_cont("numero de turno") or get_cont("turno") or ""
    hora_ini = get_cont("inicio")
    hora_fin = get_cont("término") or get_cont("termino")

    # 2) Deducir ubicación y número de tren desde la tabla de componentes (si existe)
    body = soup.select_one("table.duty-components-table")
    ubic = ""
    tren = ""

    if body:
        # origen → destino: primer start_location_long_name y último end_location_long_name
        starts = [td.get_text(" ", strip=True) for td in body.select("td.start_location_long_name")]
        ends   = [td.get_text(" ", strip=True) for td in body.select("td.end_location_long_name")]
        if starts and ends:
            ubic = f"{starts[0]} → {ends[-1]}"
        elif starts:
            ubic = starts[0]
        elif ends:
            ubic = ends[-1]

        # número de tren: celdas trip_numbers; si están vacías, heurística
        trips = [td.get_text(" ", strip=True) for td in body.select("td.trip_numbers")]
        first_trip = next((t for t in trips if t), "")
        if first_trip:
            m = re.search(r"\b([A-Z]{1,3}\d{2,5}|\d{3,5})\b", first_trip)
            if m:
                tren = m.group(1)
        if not tren:
            # heurística general sobre todo el cuerpo
            m = re.search(r"\b([A-Z]{1,3}\d{2,5}|\d{3,5})\b", body.get_text(" ", strip=True))
            if m:
                tren = m.group(1)

    # 3) Si faltan horas, intenta coger la primera y última del cuerpo
    if (not hora_ini or not hora_fin) and body:
        horas = re.findall(HORA_RE, body.get_text(" ", strip=True))
        if horas:
            if not hora_ini:
                hora_ini = horas[0]
            if not hora_fin:
                hora_fin = horas[-1]

    # Si aún faltan horas, intenta regex global
    if not hora_ini or not hora_fin:
        horas_all = re.findall(HORA_RE, soup.get_text(" ", strip=True))
        if horas_all:
            hora_ini = hora_ini or horas_all[0]
            hora_fin = hora_fin or horas_all[-1]

    # Si no hay fecha u horas, no devolvemos nada (que lo resuelva el fallback)
    if not fecha or not hora_ini or not hora_fin:
        return []

    return [{
        "fecha": fecha,
        "hora_inicio": hora_ini,
        "hora_fin": hora_fin,
        "tipo": tipo or "Turno",
        "ubicacion": ubic,
        "tren": tren,
    }]

def parse_table_html(table_html: str):
    # Parser genérico (por si alguna vista trae tabla tradicional)
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []
    table = soup.find("table")
    if not table:
        return rows

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    header_map = {}
    expected = {
        "fecha": ["fecha", "date", "giorno"],
        "hora_inicio": ["hora inicio", "inicio", "start", "inizio"],
        "hora_fin": ["hora fin", "fin", "end", "fine"],
        "tipo": ["tipo", "type", "servicio", "duty"],
        "ubicacion": ["ubicación", "ubicacion", "location", "luogo", "desde", "hacia"],
        "tren": ["tren", "train", "n° treno", "numero tren", "nº tren", "viaje"],
    }
    for i, h in enumerate(headers):
        for k, alias in expected.items():
            if any(a in h for a in alias):
                header_map[k] = i

    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        cells = [td.get_text(" ", strip=True) for td in tds]

        def cell(k, idx):
            return (
                cells[header_map[k]]
                if k in header_map and header_map[k] < len(cells)
                else (cells[idx] if idx < len(cells) else "")
            )

        rows.append({
            "fecha":       cell("fecha", 0),
            "hora_inicio": cell("hora_inicio", 1),
            "hora_fin":    cell("hora_fin", 2),
            "tipo":        cell("tipo", 3),
            "ubicacion":   cell("ubicacion", 4),
            "tren":        cell("tren", 5),
        })
    return rows

def parse_day_fallback(day_html: str, fecha_iso: str):
    # Tu fallback anterior (se mantiene)
    soup = BeautifulSoup(day_html, "html.parser")
    text = soup.get_text("\n", strip=True)

    horas = re.findall(HORA_RE, text)
    if not horas:
        return []
    hora_ini, hora_fin = horas[0], horas[-1]

    m_tipo = re.search(r"\b([A-Z]{1,3}-[A-Z0-9]{2,6})\b", text)
    tipo = m_tipo.group(1) if m_tipo else ""
    if not tipo:
        m2 = re.search(r"\b(D|LD|I|CERRO\s*T)\b", text)
        tipo = m2.group(1) if m2 else ""

    m_ubi = re.search(r"\b(MADA|MAD|ATO|BCN|VAL|SEV|ZGZ|SANTS|ATOCHA|CHAMARTIN)\b", text, re.IGNORECASE)
    ubic = m_ubi.group(1) if m_ubi else ""

    m_tren = re.search(r"(?:Tren|N[ºo]|n[ºo])\s*(\d{3,5})", text, re.IGNORECASE)
    if m_tren:
        tren = m_tren.group(1)
    else:
        m_tren2 = re.search(r"\b(\d{3,5})\b", text)
        tren = m_tren2.group(1) if m_tren2 else ""

    fecha_legible = datetime.strptime(fecha_iso, "%Y-%m-%d").strftime("%d %b %Y").lower().replace(".", "")

    return [{
        "fecha": fecha_legible,
        "hora_inicio": hora_ini,
        "hora_fin": hora_fin,
        "tipo": tipo or "Turno",
        "ubicacion": ubic,
        "tren": tren,
    }]

def parse_datetime(fecha_str: str, hora_str: str):
    fecha_str = (fecha_str or "").strip()
    hora_str = (hora_str or "").strip()
    meses = {
        "ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
        "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12",
    }
    m = re.search(r"(\d{1,2})\s+([A-Za-zñ]{3,})\.?\s+(\d{4})", fecha_str)
    if m:
        d, mes, y = m.group(1), m.group(2).lower()[:3], m.group(3)
        mesn = meses.get(mes, mes)
        for fmt in ("%Y-%m-%d %H:%M","%Y-%m-%d %H"):
            try:
                return TZ.localize(datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", fmt))
            except:
                pass
    for ff in ("%d/%m/%Y","%Y-%m-%d"):
        for fh in ("%H:%M","%H"):
            try:
                return TZ.localize(datetime.strptime(f"{fecha_str} {hora_str}", f"{ff} {fh}"))
            except:
                pass
    return None

def rows_to_events(rows):
    by_month = {}
    for r in rows:
        start = parse_datetime(r.get("fecha",""), r.get("hora_inicio",""))
        end   = parse_datetime(r.get("fecha",""), r.get("hora_fin",""))
        if not start:
            continue
        if not end:
            end = start + timedelta(hours=8)
        ym = start.strftime("%Y-%m")
        tipo = r.get("tipo","")
        ubic = r.get("ubicacion","")
        tren = r.get("tren","")
        by_month.setdefault(ym, []).append({
            "summary": f"{tipo} - Tren {tren}" if tren else (tipo or "Turno"),
            "start": start, "end": end,
            "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}"
        })
    return by_month

def create_ics(year_month: str, events):
    outdir = pathlib.Path("public/calendars")
    outdir.mkdir(parents=True, exist_ok=True)
    cal = Calendar()
    cal.add("prodid","-//Turnos IVU//")
    cal.add("version","2.0")
    for ev in events:
        e = Event()
        e.add("summary", ev["summary"])
        e.add("dtstart", ev["start"])
        e.add("dtend", ev["end"])
        e.add("description", ev["description"])
        e.add("dtstamp", datetime.now(TZ))
        cal.add_component(e)
    fname = outdir / f"turnos_{year_month}.ics"
    with open(fname,"wb") as f:
        f.write(cal.to_ical())
    return str(fname)

# ---------------- portal helpers ----------------
def login(context, user, pwd):
    page = context.new_page()
    page.goto(LOGIN_URL, timeout=60000)
    page.wait_for_load_state("domcontentloaded")
    save_debug(page, "01_login_landing")
    try:
        page.fill("#j_username", user, timeout=8000)
        page.fill("#j_password", pwd, timeout=8000)
        page.click('input.login_button[type="submit"]')
    except Exception:
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
    try:
        page.wait_for_url(re.compile(r"/mbweb/"), timeout=25000)
    except PwTimeout:
        save_debug(page, "02_after_submit_timeout")
    save_debug(page, "03_after_login")
    return page

def ensure_turnos_visible(page):
    try:
        page.wait_for_selector("#tableview", timeout=5000)
        return
    except PwTimeout:
        pass
    for sel in ['a[href*="duties"]', 'li.mainmenu-duties a', 'a:has-text("Turnos")']:
        el = page.query_selector(sel)
        if el:
            el.click()
            break
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_selector("#tableview", timeout=15000)

def extract_dates_empid_from_any(html: str):
    dates = set(re.findall(r"beginDate=(\d{4}-\d{2}-\d{2})", html))
    empid = None
    m = re.search(r"allocatedEmployeeId=(\d+)", html)
    if m:
        empid = m.group(1)
    return sorted(dates), empid

# -------- WeekView usando el directorio real de la URL ----------
def weekview_reload_and_get_html(page, frag: str, wait_ms: int = 1200):
    """
    Ejecuta WeekView.reload(frag) resolviendo la URL final como:
      <directorio_actual>/<frag>
    En tu portal el directorio actual es /mbweb/main/ivu/desktop/.
    """
    page.evaluate("""
        () => {
          if (!document.querySelector('#tableview')) {
              const d=document.createElement('div'); d.id='tableview'; document.body.appendChild(d);
          }
        }
    """)
    page.evaluate("""
        () => {
          if (typeof window.WeekView === 'undefined') {
              window.WeekView = {
                reload: function (frag) {
                  const loc = new URL(window.location.href);
                  const dir = loc.pathname.replace(/[^/]+$/, ''); // carpeta .../desktop/
                  const url = frag.startsWith('/') ? frag : (dir + frag);
                  return fetch(url, {
                      credentials: 'same-origin',
                      headers: {
                        'X-Requested-With':'XMLHttpRequest',
                        'Accept':'text/html, */*; q=0.01'
                      }
                  })
                  .then(r => r.text())
                  .then(html => {
                     const el = document.querySelector('#tableview');
                     if (el) el.innerHTML = html;
                     return html;
                  });
                }
              };
          }
        }
    """)
    page.evaluate("""(f)=>{ const c=document.querySelector('#tableview'); if(c) c.innerHTML=''; WeekView.reload(f); }""", frag)
    page.wait_for_function("""
        () => {
          const el = document.querySelector('#tableview');
          if (!el || !el.innerHTML) return false;
          const h = el.innerHTML.toLowerCase();
          return h.length > 200 && (h.includes('<table') || h.includes('<td') || h.includes('inicio') || h.includes('término') || h.includes('termino'));
        }
    """, timeout=25000)
    page.wait_for_timeout(wait_ms)
    return page.inner_html("#tableview")

def try_weekview_polyfill_and_get_month(page):
    return weekview_reload_and_get_html(page, "_-duty-table")

def fetch_day_html(context, page, ymd: str, empid: str | None):
    from urllib.parse import urljoin
    curr = page.url
    base_dir = re.sub(r'[^/]+$', '', curr)
    qs = f"beginDate={ymd}&showUserInfo=true"
    if empid:
        qs += f"&allocatedEmployeeId={empid}"
    url = urljoin(base_dir, f"_-duty-details-day?{qs}")
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html, */*; q=0.01",
        "Referer": page.url,
        "Accept-Language": "es-ES,es;q=0.9",
        "Cache-Control": "no-cache",
    }
    r = context.request.get(url, headers=headers)
    if r.ok:
        return r.text()
    return weekview_reload_and_get_html(page, f"_-duty-details-day?{qs}")

# ---------------- main ----------------
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
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-gpu"],
        )
        context = browser.new_context(
            locale="es-ES",
            timezone_id="Europe/Madrid",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/118.0.0.0 Safari/537.36"),
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        page = login(context, user, pwd)
        ensure_turnos_visible(page)

        full_html = page.content()
        soup = BeautifulSoup(full_html, "html.parser")
        tv = soup.select_one("#tableview")
        month_html = str(tv) if tv else full_html

        dates, empid = extract_dates_empid_from_any(full_html)
        if not dates:
            month_html = try_weekview_polyfill_and_get_month(page)
            dates, empid = extract_dates_empid_from_any(month_html)

        save_debug(page, "04_month_html", html_override=month_html)

        if not dates:
            save_debug(page, "05_no_dates_in_month", html_override=month_html)
            raise RuntimeError("No se encontraron fechas en el mes")

        all_events = {}
        for idx, ymd in enumerate(dates):
            html_day = fetch_day_html(context, page, ymd, empid)
            if idx < 15:
                try:
                    with open(f"debug/day_{ymd}.html", "w", encoding="utf-8") as f:
                        f.write(html_day)
                except Exception:
                    pass

            # *** NUEVO ORDEN: parser IVU -> genérico -> fallback ***
            rows = parse_day_ivu(html_day)
            if not rows:
                rows = parse_table_html(html_day)
            if not rows:
                rows = parse_day_fallback(html_day, ymd)

            month_map = rows_to_events(rows)
            print(f"[{ymd}] filas={len(rows)} evs={sum(len(v) for v in month_map.values())}")
            for ym, evs in month_map.items():
                all_events.setdefault(ym, []).extend(evs)

        for ym, evs in all_events.items():
            fname = create_ics(ym, evs)
            generated.append(fname)

        browser.close()

    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
