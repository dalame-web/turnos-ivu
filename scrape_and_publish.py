import os, re, pathlib
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
from icalendar import Calendar, Event
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

BASE = "https://wcrew-ilsa.trenitalia.it"
LOGIN_URL = f"{BASE}"
TZ = pytz.timezone("Europe/Madrid")

# =========== util ===========
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

# =========== parsers ===========
def parse_table_html(table_html: str):
    """
    Parser principal: tabla con TH/TD ("Fecha", "Hora Inicio", "Hora Fin", "Tipo", "Ubicación", "Tren").
    """
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []
    table = soup.find("table")
    if not table:
        return rows

    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
    header_map = {}
    expected = {
        "fecha": ["fecha","date","giorno"],
        "hora_inicio": ["hora inicio","inicio","start","inizio"],
        "hora_fin": ["hora fin","fin","end","fine"],
        "tipo": ["tipo","type","servicio","duty"],
        "ubicacion": ["ubicación","ubicacion","location","luogo"],
        "tren": ["tren","train","n° treno","numero tren","nº tren","num tren"],
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

HORA_RE = r"([01]?\d|2[0-3]):[0-5]\d"

def parse_day_fallback(day_html: str, fecha_iso: str):
    """
    Fallback robusto para páginas de día sin tabla formal.
    - Toma la primera hora del HTML como inicio y la última como fin.
    - Intenta extraer: Tipo (código/etiqueta), Ubicación (palabras tipo MAD/MADA),
      y Nº tren (palabra 'Tren' o número aislado de 3-5 dígitos).
    Devuelve una lista de 0..n filas con el formato estándar.
    """
    soup = BeautifulSoup(day_html, "html.parser")
    text = soup.get_text("\n", strip=True)

    horas = re.findall(HORA_RE, text)
    if not horas:
        return []  # no hay horas; sin datos no añadimos nada

    hora_ini = horas[0]
    hora_fin = horas[-1]

    # Tipo: busca textos "código" en mayúsculas con guión (p.ej. F-AG03A) o etiquetas típicas (D, I, LD, CERRO T...)
    m_tipo = re.search(r"\b([A-Z]{1,3}-[A-Z0-9]{2,6})\b", text)
    tipo = m_tipo.group(1) if m_tipo else ""
    if not tipo:
        m_tipo2 = re.search(r"\b(D|LD|I|CERRO\s*T)\b", text)
        tipo = m_tipo2.group(1) if m_tipo2 else ""

    # Ubicación: toma palabras como MAD/MADA u otras estaciones (todas mayúsculas de 3–6 letras)
    m_ubi = re.search(r"\b(MADA|MAD|ATO|BCN|VAL|SEV|ZGZ|SANTS|ATOCHA|CHAMARTIN)\b", text, re.IGNORECASE)
    ubicacion = m_ubi.group(1) if m_ubi else ""

    # Tren: busca "Tren 1234" o "nº 1234" o cualquier número de 3–5 dígitos cercano a la palabra 'Tren'
    tren = ""
    m_tren = re.search(r"(?:Tren|N[ºo]|n[ºo])\s*(\d{3,5})", text, re.IGNORECASE)
    if m_tren:
        tren = m_tren.group(1)
    else:
        # último recurso: cualquier número de 3–5 dígitos en el texto
        m_tren2 = re.search(r"\b(\d{3,5})\b", text)
        tren = m_tren2.group(1) if m_tren2 else ""

    # La fecha la tenemos como ISO (YYYY-MM-DD) desde el enlace del mes.
    fecha_legible = datetime.strptime(fecha_iso, "%Y-%m-%d").strftime("%d %b %Y").lower().replace(".", "")

    return [{
        "fecha": fecha_legible,
        "hora_inicio": hora_ini,
        "hora_fin": hora_fin,
        "tipo": tipo,
        "ubicacion": ubicacion,
        "tren": tren,
    }]

def parse_datetime(fecha_str: str, hora_str: str):
    fecha_str = (fecha_str or "").strip()
    hora_str  = (hora_str or "").strip()
    meses = {"ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
             "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12"}
    # "19 oct 2025" o "19 oct. 2025"
    m = re.search(r"(\d{1,2})\s+([A-Za-zñ]{3,})\.?\s+(\d{4})", fecha_str)
    if m:
        d, mes, y = m.group(1), m.group(2).lower()[:3], m.group(3)
        mesn = meses.get(mes, mes)
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H"):
            try:
                return TZ.localize(datetime.strptime(f"{y}-{mesn}-{d.zfill(2)} {hora_str}", fmt))
            except: pass
    for ff in ("%d/%m/%Y","%Y-%m-%d"):
        for fh in ("%H:%M","%H"):
            try:
                return TZ.localize(datetime.strptime(f"{fecha_str} {hora_str}", f"{ff} {fh}"))
            except: pass
    return None

def rows_to_events(rows):
    by_month = {}
    for r in rows:
        start = parse_datetime(r.get("fecha",""), r.get("hora_inicio",""))
        end   = parse_datetime(r.get("fecha",""), r.get("hora_fin",""))
        if not start:  # si falla el parseo de fecha, ignora
            continue
        if not end:
            end = start + timedelta(hours=8)
        ym = start.strftime("%Y-%m")
        tipo = r.get("tipo",""); ubic = r.get("ubicacion",""); tren = r.get("tren","")
        by_month.setdefault(ym, []).append({
            "summary": f"{tipo} - Tren {tren}" if tren else (tipo or "Turno"),
            "start": start, "end": end,
            "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}"
        })
    return by_month

def create_ics(year_month: str, events):
    outdir = pathlib.Path("public/calendars")
    outdir.mkdir(parents=True, exist_ok=True)
    cal = Calendar(); cal.add('prodid','-//Turnos IVU//'); cal.add('version','2.0')
    for ev in events:
        e = Event()
        e.add('summary', ev['summary']); e.add('dtstart', ev['start']); e.add('dtend', ev['end'])
        e.add('description', ev['description']); e.add('dtstamp', datetime.now(TZ))
        cal.add_component(e)
    fname = outdir / f"turnos_{year_month}.ics"
    with open(fname, "wb") as f: f.write(cal.to_ical())
    return str(fname)

# =========== helpers ===========
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
        page.evaluate("""(creds)=>{ 
            const uf=document.querySelector('#j_username'); 
            const pf=document.querySelector('#j_password'); 
            if(uf) uf.value=creds.u; if(pf) pf.value=creds.p; 
            const f=document.querySelector('form#login_form'); 
            if(f){ f.action='/mbweb/j_security_check'; f.method='POST'; f.submit(); }
        }""", {"u":user,"p":pwd})
    try:
        page.wait_for_url(re.compile(r"/mbweb/"), timeout=25000)
    except PwTimeout:
        save_debug(page, "02_after_submit_timeout")
    save_debug(page, "03_after_login")
    return page

def ensure_turnos_visible(page):
    """Si no aparece #tableview, intenta activar la pestaña 'Turnos'."""
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
    if m: empid = m.group(1)
    return sorted(dates), empid

def try_weekview_polyfill_and_get_month(page):
    page.wait_for_selector("#tableview", state="attached", timeout=15000)
    page.evaluate("""
        () => {
          if (typeof window.WeekView === 'undefined') {
              window.WeekView = {
                  reload: function (frag) {
                      const url = (frag.startsWith('/mbweb/')) ? frag : ('/mbweb/' + frag);
                      return fetch(url, { credentials: 'same-origin' })
                        .then(r => r.text())
                        .then(html => {
                           const el = document.querySelector('#tableview');
                           if (el) el.innerHTML = html;
                        }).catch(()=>{});
                  }
              };
          }
        }
    """)
    page.evaluate("""(frag)=>{ 
        const el=document.querySelector('#tableview'); if(el) el.innerHTML='';
        WeekView.reload(frag); 
    }""", "_-duty-table")
    page.wait_for_function("""()=>{const el=document.querySelector('#tableview'); return el && el.innerHTML && el.innerHTML.length>20;}""", timeout=20000)
    return page.inner_html("#tableview")

def fetch_day_html(page, ymd: str, empid: str | None):
    """
    Devuelve el HTML del día {ymd} probando rutas en este orden:
    1) /duty-details?beginDate=...
    2) /mbweb/duty-details?beginDate=...    (tu instancia lo rechaza; se mantiene por compatibilidad)
    3) _-duty-details-day?beginDate=...      (fragmento AJAX mediante polyfill WeekView)
    """
    def _has_unmapped_error(txt: str) -> bool:
        return "There is no Action mapped for namespace" in (txt or "")

    # 1) /duty-details (SIN /mbweb)
    url1 = f"/duty-details?beginDate={ymd}"
    if empid: url1 += f"&allocatedEmployeeId={empid}"
    html = page.evaluate("""(u)=>fetch(u,{credentials:'same-origin'}).then(r=>r.text())""", url1)
    if html and not _has_unmapped_error(html):
        return html

    # 2) /mbweb/duty-details (mantener por compatibilidad)
    url2 = f"/mbweb/duty-details?beginDate={ymd}"
    if empid: url2 += f"&allocatedEmployeeId={empid}"
    html = page.evaluate("""(u)=>fetch(u,{credentials:'same-origin'}).then(r=>r.text())""", url2)
    if html and not _has_unmapped_error(html):
        return html

    # 3) Fragmento AJAX con WeekView (inyectamos polyfill si hace falta)
    page.evaluate("""
        () => {
          if (typeof window.WeekView === 'undefined') {
              window.WeekView = {
                  reload: function (frag) {
                      const url = frag.startsWith('/mbweb/') ? frag : '/mbweb/' + frag;
                      return fetch(url, { credentials: 'same-origin' })
                        .then(r => r.text())
                        .then(html => {
                           let el = document.querySelector('#tableview');
                           if (!el) { el = document.createElement('div'); el.id='tableview'; document.body.appendChild(el); }
                           el.innerHTML = html;
                           return html;
                        });
                  }
              };
          }
        }
    """)
    frag = f"_-duty-details-day?beginDate={ymd}&showUserInfo=true"
    if empid: frag += f"&allocatedEmployeeId={empid}"
    html = page.evaluate("""(f)=>WeekView.reload(f)""", frag)
    return html

# =========== main ===========
def main():
    user = os.environ.get("IVU_USER"); pwd = os.environ.get("IVU_PASS")
    if not user or not pwd: raise RuntimeError("Faltan IVU_USER/IVU_PASS")

    ensure_dir("debug")
    generated = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=[
            "--disable-blink-features=AutomationControlled","--no-sandbox","--disable-gpu"
        ])
        context = browser.new_context(
            locale="es-ES", timezone_id="Europe/Madrid",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/118.0.0.0 Safari/537.36")
        )
        context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")

        page = login(context, user, pwd)

        # 1) Asegurar cuadrícula "Turnos"
        ensure_turnos_visible(page)

        # 2) Extraer fechas/empid (del HTML completo; si no, WeekView polyfill)
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

        # 3) Pedir cada día y parsear (con fallback). Guardar los 10 primeros HTML para debug.
        all_events = {}
        for idx, ymd in enumerate(dates):
            html_day = fetch_day_html(page, ymd, empid)
            if idx < 10:
                # guarda muestra para depurar si hiciera falta
                try:
                    with open(f"debug/day_{ymd}.html","w",encoding="utf-8") as f:
                        f.write(html_day)
                except Exception:
                    pass

            rows = parse_table_html(html_day)
            if not rows:
                rows = parse_day_fallback(html_day, ymd)

            month_map = rows_to_events(rows)
            # trazas
            total_evs = sum(len(v) for v in month_map.values())
            print(f"[{ymd}] eventos detectados: {total_evs}")

            for ym, evs in month_map.items():
                all_events.setdefault(ym, []).extend(evs)

        # 4) Generar ICS por mes
        for ym, evs in all_events.items():
            fname = create_ics(ym, evs)
            generated.append(fname)

        browser.close()

    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
