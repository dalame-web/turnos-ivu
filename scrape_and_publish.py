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
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def save_debug(page, name, html_override=None):
    _ensure_dir("debug")
    # HTML
    try:
        html = html_override if html_override is not None else page.content()
        with open(f"debug/{name}.html", "w", encoding="utf-8") as f:
            f.write(html)
    except Exception:
        pass
    # Screenshot
    try:
        page.screenshot(path=f"debug/{name}.png", full_page=True)
    except Exception:
        pass

def parse_table_html(table_html):
    soup = BeautifulSoup(table_html, "html.parser")
    rows = []
    table = soup.find("table")
    if table:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        header_map = {}
        expected = {
            "fecha": ["fecha","date","giorno"],
            "hora_inicio": ["hora inicio","inicio","start","inizio"],
            "hora_fin": ["hora fin","fin","end","fine"],
            "tipo": ["tipo","type","servicio","duty"],
            "ubicacion": ["ubicación","ubicacion","location","luogo"],
            "tren": ["tren","train","n° treno","numero tren"]
        }
        for i,h in enumerate(headers):
            for k, alias in expected.items():
                if any(a in h for a in alias):
                    header_map[k] = i
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds: continue
            cells = [td.get_text(strip=True) for td in tds]
            def cell(k, idx):
                return cells[header_map[k]] if k in header_map and header_map[k] < len(cells) else (cells[idx] if idx < len(cells) else "")
            rows.append({
                "fecha": cell("fecha",0),
                "hora_inicio": cell("hora_inicio",1),
                "hora_fin": cell("hora_fin",2),
                "tipo": cell("tipo",3),
                "ubicacion": cell("ubicacion",4),
                "tren": cell("tren",5)
            })
        return rows
    # fallback tarjetas
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
    hora_str = (hora_str or "").strip()
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
            except: pass
    for ff in ("%d/%m/%Y","%Y-%m-%d"):
        for fh in ("%H:%M","%H"):
            try:
                dt = datetime.strptime(f"{fecha_str} {hora_str}", f"{ff} {fh}")
                return TZ.localize(dt)
            except: pass
    return None

def events_from_rows(rows):
    by_month={}
    for r in rows:
        fecha=r.get("fecha",""); ini=r.get("hora_inicio",""); fin=r.get("hora_fin","")
        tipo=r.get("tipo","");   ubic=r.get("ubicacion","");  tren=r.get("tren","")
        start = parse_datetime(fecha, ini) if fecha and ini else None
        end   = parse_datetime(fecha, fin) if fecha and fin else None
        if not start: continue
        if not end: end = start + timedelta(hours=8)
        ym = start.strftime("%Y-%m")
        by_month.setdefault(ym, []).append({
            "summary": f"{tipo} - Tren {tren}" if tren else (tipo or "Turno"),
            "start": start, "end": end,
            "description": f"Tipo: {tipo}\\nUbicación: {ubic}\\nTren: {tren}"
        })
    return by_month

def create_ics(year_month, events):
    cal = Calendar(); cal.add('prodid','-//Turnos IVU//'); cal.add('version','2.0')
    for ev in events:
        e=Event()
        e.add('summary', ev['summary']); e.add('dtstart', ev['start']); e.add('dtend', ev['end'])
        e.add('description', ev['description']); e.add('dtstamp', datetime.now(TZ))
        cal.add_component(e)
    fname=f"turnos_{year_month}.ics"
    with open(fname,"wb") as f: f.write(cal.to_ical())
    return fname

def login(context, user, pwd):
    page = context.new_page()
    page.goto(LOGIN_URL, timeout=60000)
    page.wait_for_load_state("domcontentloaded")
    # Guardar estado inicial
    save_debug(page, "01_login_landing")

    # Esperar a que el formulario exista (aunque esté hidden)
    page.wait_for_selector("form#login_form", state="attached", timeout=20000)

    # Rellenar y enviar por JS aunque esté oculto
    page.evaluate("""
        (u,p)=>{
          const uf=document.querySelector('#j_username');
          const pf=document.querySelector('#j_password');
          if(uf) uf.value=u;
          if(pf) pf.value=p;
          const form=document.querySelector('form#login_form');
          if(form){
            form.action='/mbweb/j_security_check';
            form.method='POST';
            form.submit();
          }
        }
    """, user, pwd)

    # Esperar navegación post-login (cualquier módulo)
    try:
        page.wait_for_url(re.compile(r"/mbweb/(duties|messages|absence-overview|holiday-request)"), timeout=25000)
    except PwTimeout:
        save_debug(page, "02_after_submit_timeout")
        # probar acceso directo a duties
        page.goto(DUTIES_URL, timeout=30000)

    save_debug(page, "03_after_login")
    return page

def main():
    user = os.environ.get("IVU_USER"); pwd = os.environ.get("IVU_PASS")
    if not user or not pwd:
        raise RuntimeError("Faltan IVU_USER/IVU_PASS")

    generated=[]
    _ensure_dir("debug")  # asegurar carpeta desde el inicio

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

        # API ctx con cookies autenticadas
        api = p.request.new_context(base_url=BASE, storage_state=context.storage_state(),
                                    extra_http_headers={"Accept-Language":"es-ES"})

        # Mes
        resp = api.get("/mbweb/_-duty-table")
        if not resp.ok:
            save_debug(page, "04_month_request_fail", html_override=resp.text())
            raise RuntimeError(f"_ -duty-table status {resp.status}")
        html_month = resp.text()
        save_debug(page, "04_month_html", html_override=html_month)

        # Fechas (beginDate=YYYY-MM-DD)
        dates = set(re.findall(r"beginDate=(\d{4}-\d{2}-\d{2})", html_month))
        if not dates:
            # intenta buscar en atributos data
            soup = BeautifulSoup(html_month, "html.parser")
            for a in soup.find_all("a", href=True):
                m = re.search(r"beginDate=(\d{4}-\d{2}-\d{2})", a["href"])
                if m: dates.add(m.group(1))
            for tag in soup.find_all(attrs={"data-begin-date": True}):
                dates.add(tag["data-begin-date"])
        dates = sorted(dates)
        if not dates:
            save_debug(page, "05_no_dates", html_override=html_month)
            raise RuntimeError("No se encontraron fechas en el mes")

        all_events={}
        for ymd in dates:
            r = api.get(f"/mbweb/_-duty-details-day?beginDate={ymd}&showUserInfo=true")
            if not r.ok:
                save_debug(page, f"day_{ymd}_req_fail", html_override=r.text())
                continue
            html_day = r.text()
            # Guardar uno de ejemplo para diagnóstico
            # save_debug(page, f"day_{ymd}", html_override=html_day)

            rows = parse_table_html(html_day)
            bym = events_from_rows(rows)
            for ym, evs in bym.items():
                all_events.setdefault(ym, []).extend(evs)

        for ym, evs in all_events.items():
            fname = create_ics(ym, evs); generated.append(fname)

        browser.close()

    print("GENERATED_FILES=" + ",".join(generated))

if __name__ == "__main__":
    main()
