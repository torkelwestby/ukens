# app.py
# Streamlit-app: Match HubSpot (data/hubspot.csv) ↔ Brønnøysund (data/brreg.csv)
# Fire steg: 1) orgnr  2) eksakt navn  3) navn uten juridiske  4) navn uten juridiske + ekstra
# API-berikelse til slutt: ansatte (Enhetsregisteret) og omsetning (Regnskapsregisteret)

import re
import time
import math
import requests
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import streamlit as st
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="HubSpot ↔ Brreg matcher", layout="wide")

# --- Passord ---
def check_password() -> bool:
    authed = st.session_state.get("authed", False)
    if authed:
        return True

    pw = st.text_input("Skriv passord", type="password")
    if st.button("Logg inn"):
        if pw == st.secrets.get("APP_PASSWORD", ""):
            st.session_state["authed"] = True
            st.rerun()
        else:
            st.error("Feil passord")
    return False

if not check_password():
    st.stop()

with st.sidebar:
    if st.button("Logg ut"):
        st.session_state["authed"] = False
        st.rerun()


# -------------------- Konfig --------------------
ENHETS_BASE = "https://data.brreg.no/enhetsregisteret/api/enheter"
REGN_BASE   = "https://data.brreg.no/regnskapsregisteret/regnskap"
UA          = "hs-brreg-streamlit/1.0 (kontakt: you@example.com)"
TIMEOUT     = 8
RETRIES     = 1
MAX_WORKERS = min(32, (os.cpu_count() or 8) * 4)


# NACE-hovedkategorier (2-siffer) basert på SSB-standarden
NACE_CATEGORIES = {
    "A - Jordbruk, skogbruk og fiske": {
        "01": "Jordbruk og tjenester tilknyttet jordbruk, jakt og viltstell",
        "02": "Skogbruk og tjenester tilknyttet skogbruk",
        "03": "Fiske, fangst og akvakultur",
    },
    "B - Bergverksdrift og utvinning": {
        "05": "Bryting av steinkull og brunkull",
        "06": "Utvinning av råolje og naturgass",
        "07": "Bryting av metallholdig malm",
        "08": "Bryting og bergverksdrift ellers",
        "09": "Tjenester tilknyttet bergverksdrift og utvinning",
    },
    "C - Industri": {
        "10": "Produksjon av nærings- og nytelsesmidler",
        "11": "Produksjon av drikkevarer",
        "12": "Produksjon av tobakksvarer",
        "13": "Produksjon av tekstiler",
        "14": "Produksjon av klær",
        "15": "Produksjon av lær og relaterte produkter",
        "16": "Produksjon av trelast og varer av tre",
        "17": "Produksjon av papir og papirvarer",
        "18": "Trykking og reproduksjon av innspilte opptak",
        "19": "Produksjon av kullprodukter og raffinerte petroleumsprodukter",
        "20": "Produksjon av kjemikalier og kjemiske produkter",
        "21": "Produksjon av farmasøytiske råvarer og preparater",
        "22": "Produksjon av gummi- og plastprodukter",
        "23": "Produksjon av andre ikke-metalliske mineralprodukter",
        "24": "Produksjon av metaller",
        "25": "Produksjon av metallvarer, unntatt maskiner og utstyr",
        "26": "Produksjon av datamaskiner og elektroniske og optiske produkter",
        "27": "Produksjon av elektrisk utstyr",
        "28": "Produksjon av maskiner og utstyr ikke nevnt annet sted",
        "29": "Produksjon av motorvogner og tilhengere",
        "30": "Produksjon av andre transportmidler",
        "31": "Produksjon av møbler",
        "32": "Annen industriproduksjon",
        "33": "Reparasjon, vedlikehold og installasjon av maskiner og utstyr",
    },
    "D - Forsyning av elektrisitet, gass, damp og kjøleluft": {
        "35": "Forsyning av elektrisitet, gass, damp og kjøleluft",
    },
    "E - Vannforsyning, avløp, renovasjon og opprydding": {
        "36": "Uttak fra kilde, rensing og distribusjon av vann",
        "37": "Oppsamling og behandling av avløpsvann",
        "38": "Innsamling, gjenvinning og behandling av avfall",
        "39": "Miljøutbedring, opprydding og lignende aktivitet",
    },
    "F - Bygge- og anleggsvirksomhet": {
        "41": "Oppføring av bygninger",
        "42": "Anleggsvirksomhet",
        "43": "Spesialisert bygge- og anleggsvirksomhet",
    },
    "G - Varehandel": {
        "46": "Engroshandel",
        "47": "Detaljhandel",
    },
    "H - Transport og lagring": {
        "49": "Landtransport og rørtransport",
        "50": "Sjøfart",
        "51": "Lufttransport",
        "52": "Lagring og andre tjenester tilknyttet transport",
        "53": "Post- og budtjenester",
    },
    "I - Overnattings- og serveringsvirksomhet": {
        "55": "Overnattingsvirksomhet",
        "56": "Serveringsvirksomhet",
    },
    "J - Utgivelse, kringkasting og innholdsproduksjon": {
        "58": "Utgivelsesvirksomhet",
        "59": "Film-, video- og fjernsynsprogramproduksjon",
        "60": "Radio og fjernsyn, kringkasting og nyhetsbyråer",
    },
    "K - IT og telekommunikasjon": {
        "61": "Telekommunikasjon",
        "62": "Dataprogrammering og konsulentvirksomhet",
        "63": "Datainfrastruktur, -behandling og -lagring",
    },
    "L - Finansiell tjenesteyting": {
        "64": "Finansieringsvirksomhet og kollektive investeringsfond",
        "65": "Forsikringsvirksomhet",
        "66": "Tjenester tilknyttet finansiell virksomhet",
    },
    "M - Eiendomsvirksomhet": {
        "68": "Eiendomsvirksomhet",
    },
    "N - Faglig, vitenskapelig og teknisk tjenesteyting": {
        "69": "Juridisk og regnskapsmessig tjenesteyting",
        "70": "Hovedkontortjenester og administrativ rådgivning",
        "71": "Arkitektvirksomhet og teknisk konsulentvirksomhet",
        "72": "Forskning og eksperimentell utvikling",
        "73": "Annonse- og reklamevirksomhet, markedsundersøkelser",
        "74": "Annen faglig, vitenskapelig og teknisk virksomhet",
        "75": "Veterinærtjenester",
    },
    "O - Forretningsmessig tjenesteyting": {
        "77": "Utleie- og leasingvirksomhet",
        "78": "Arbeidskrafttjenester",
        "79": "Reisebyrå- og reisearrangørvirksomhet",
        "80": "Etterforsknings- og vakttjenester",
        "81": "Tjenester tilknyttet eiendomsdrift",
        "82": "Annen forretningsmessig tjenesteyting",
    },
    "P - Offentlig administrasjon og forsvar": {
        "84": "Offentlig administrasjon og forsvar",
    },
    "Q - Undervisning": {
        "85": "Undervisning",
    },
    "R - Helse- og sosialtjenester": {
        "86": "Helsetjenester",
        "87": "Helse- og omsorgstjenester i institusjoner",
        "88": "Omsorgs- og sosialtjenester uten botilbud",
    },
    "S - Kulturell virksomhet, idrett og fritid": {
        "90": "Kunstnerisk virksomhet og underholdningsvirksomhet",
        "91": "Drift av biblioteker, arkiver, museer",
        "92": "Lotteri- og gamblingvirksomhet",
        "93": "Sports-, fornøyelses- og fritidsaktiviteter",
    },
    "T - Annen tjenesteyting": {
        "94": "Aktiviteter i medlemsorganisasjoner",
        "95": "Reparasjon av datamaskiner og husholdningsvarer",
        "96": "Personlig tjenesteyting",
    },
}

SOURCE_LABEL = {
    "orgnr": "Org.nr",
    "name_eq": "Eksakt navn",
    "name_no_legal_eq": "Uten juridiske",
    "name_no_qual_eq": "Uten juridiske + ekstra",
}

LEGAL_WORDS = {"as","a/s","asa","ab","oy","inc","ltd","llc","gmbh","sa","sarl","bv","nv","plc","k/s","aps","oyj","ag","spa"}
EXTRA_WORDS = {"group","holding","konsern","international","int","co","company","solutions","solution","technology","technologies","systems","system","norge","norway"}

# -------------------- Hjelpere --------------------
MONTH_ABBR_NO = ["jan","feb","mar","apr","mai","jun","jul","aug","sep","okt","nov","des"]

def fmt_full_date(dt) -> str:
    if pd.isna(dt):
        return ""
    return f"{dt.day}.{MONTH_ABBR_NO[dt.month-1]} {dt.year}"



def only_digits(x: str) -> str:
    return re.sub(r"\D","", x or "")

def norm_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def raw_name(s: str) -> str:
    return norm_spaces(s).casefold() if isinstance(s, str) else ""

def strip_words(s: str, words: set) -> str:
    if not isinstance(s, str): return ""
    s = s.replace("&", " og ")
    s = re.sub(r"[^a-z0-9 æøå\-]", " ", s, flags=re.IGNORECASE)
    toks = [t for t in re.split(r"\s+", s.casefold()) if t and t not in words]
    return norm_spaces(" ".join(toks))

def ensure_str_col(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].fillna("").astype(str) if col in df.columns else pd.Series([""]*len(df), index=df.index, dtype=str)

def get_selected_nace_codes(selected_categories: dict) -> list[str]:
    """Hent alle valgte NACE-koder fra checkboxes"""
    codes = []
    for cat_name, subcodes in selected_categories.items():
        codes.extend(subcodes)
    return sorted(set(codes))

def get_json(url, params=None, retries=RETRIES, timeout=TIMEOUT, session: requests.Session | None = None):
    sess = session or requests
    for i in range(retries + 1):
        try:
            r = sess.get(url, params=params, timeout=timeout, headers={"User-Agent": UA, "Accept": "application/json"})
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.4 * (2 ** i))
                continue
            return None
        except requests.RequestException:
            if i < retries:
                time.sleep(0.4 * (2 ** i))
            else:
                return None
    return None

def pick_revenue_from_obj(obj):
    if not isinstance(obj, dict):
        return None
    pri = ["sumDriftsinntekter","driftsinntekter","salgsinntekter","salgsinntekt","nettoDriftsinntekter","omsetning"]
    for k in pri:
        if k in obj and obj[k] is not None:
            try: return float(str(obj[k]).replace(" ", "").replace(",", "."))
            except: pass
    for k, v in obj.items():
        if isinstance(k, str) and any(w in k for w in ["inntekt", "omset"]):
            try: return float(str(v).replace(" ", "").replace(",", "."))
            except: pass
    return None

def deep_find_revenue(j):
    stack = [j]
    while stack:
        x = stack.pop()
        if isinstance(x, dict):
            rev = pick_revenue_from_obj(x)
            if rev is not None:
                return rev
            for v in x.values():
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(x, list):
            for v in x:
                if isinstance(v, (dict, list)):
                    stack.append(v)
    return None

@st.cache_resource(show_spinner=False)
def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    retry = Retry(total=RETRIES, backoff_factor=0.4, status_forcelist=[429,500,502,503,504])
    adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


@st.cache_data(show_spinner=False, ttl=86400)
def api_employees(orgnr_list: list[str]) -> dict:
    session = get_session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    out = {}
    def fetch(org):
        j = get_json(f"{ENHETS_BASE}/{org}", session=session)
        if not j: return None
        v = j.get("antallAnsatte")
        try: return int(v) if v is not None and str(v).strip() != "" else None
        except: return None
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch, o): o for o in orgnr_list}
        for fut in as_completed(futs):
            o = futs[fut]
            try: out[o] = fut.result()
            except: out[o] = None
    return out

@st.cache_data(show_spinner=False, ttl=86400)
def api_revenue(orgnr_list: list[str]) -> dict:
    session = get_session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    out = {}
    def fetch(org):
        j = get_json(f"{REGN_BASE}/{org}", session=session)
        if not j: return None
        return deep_find_revenue(j)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch, o): o for o in orgnr_list}
        for fut in as_completed(futs):
            o = futs[fut]
            try: out[o] = fut.result()
            except: out[o] = None
    return out

def label_count_or_no(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "ikke"
    try:
        return str(int(val))
    except:
        return "ikke"

def label_amount_or_no(val):
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return "ikke"
    try:
        return f"{int(val):,}".replace(",", " ")
    except:
        return "ikke"

def mnok_to_nok(x):
    return None if x == 0 else int(x * 1_000_000)

def in_range(v, lo, hi):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return False
    if lo is not None and v < lo: return False
    if hi is not None and hi != 0 and v > hi: return False
    return True

# -------------------- UI --------------------
st.title("🔍 HubSpot ↔ Brønnøysund matcher")
st.markdown("Match bedrifter fra HubSpot med Brønnøysundregisteret. Filtrer på bransje, ansatte og omsetning.")

# Hovedfiltre i toppen
st.markdown("### ⚙️ Filtreringskriterier")

# NACE-velger med ekspanderbar seksjon
# NACE-velger med ekspanderbar seksjon
with st.expander("📊 Velg næringskoder (NACE)", expanded=True):
    st.markdown("**Velg én eller flere næringskoder fra kategoriene nedenfor:**")

    # Hurtigvalg-knapper
    col_quick1, col_quick2, col_quick3, col_quick4 = st.columns(4)
    with col_quick1:
        if st.button("✓ Velg alle"):
            st.session_state["__quick"] = "all"
    with col_quick2:
        if st.button("✗ Fjern alle"):
            st.session_state["__quick"] = "none"
    with col_quick3:
        if st.button("🏗️ Target bygg og anlegg (41–43)"):
            st.session_state["__quick"] = "construction"
    with col_quick4:
        if st.button("🛍️ Target retail (46–47)"):
            st.session_state["__quick"] = "retail"

    # Anvend hurtigvalg ved denne kjøringen
    quick = st.session_state.get("__quick", None)
    if quick:
        for category, subcodes in NACE_CATEGORIES.items():
            for code in subcodes.keys():
                key = f"nace_{category}_{code}"
                if quick == "all":
                    st.session_state[key] = True
                elif quick == "none":
                    st.session_state[key] = False
                elif quick == "construction":
                    st.session_state[key] = (category == "F - Bygge- og anleggsvirksomhet")
                elif quick == "retail":
                    st.session_state[key] = (category == "G - Varehandel")
        st.session_state["__quick"] = None  # nullstill

    st.markdown("---")

    # Tabs og avkrysningsbokser
    tabs = st.tabs([cat.split(" - ")[0] for cat in NACE_CATEGORIES.keys()])
    selected_codes = {}

    for idx, (category, subcodes) in enumerate(NACE_CATEGORIES.items()):
        with tabs[idx]:
            st.markdown(f"**{category}**")
            selected_in_category = []
            cols = st.columns(2)
            for i, (code, desc) in enumerate(subcodes.items()):
                key = f"nace_{category}_{code}"
                current = st.session_state.get(key, False)
                with cols[i % 2]:
                    checked = st.checkbox(f"**{code}** - {desc}", value=current, key=key)
                if checked:
                    selected_in_category.append(code)
            if selected_in_category:
                selected_codes[category] = selected_in_category

    # Vis valgte koder
    all_selected = get_selected_nace_codes(selected_codes)
    if all_selected:
        st.success(f"✓ **{len(all_selected)} næringskoder valgt:** {', '.join(all_selected)}")
    else:
        st.warning("⚠️ Ingen næringskoder valgt. Alle bransjer vil bli inkludert.")

# Filtre for ansatte og omsetning
col1, col2 = st.columns(2)

with col1:
    st.markdown("**👥 Antall ansatte**")
    min_emp = st.number_input("Minimum ansatte", min_value=0, value=0, step=1)
    max_emp = st.number_input("Maksimum ansatte", min_value=0, value=0, step=1, help="0 = ingen grense")

with col2:
    st.markdown("**💰 Omsetning (MNOK)**")
    min_rev_mnok = st.number_input("Minimum omsetning", min_value=0.0, value=0.0, step=1.0)
    max_rev_mnok = st.number_input("Maksimum omsetning", min_value=0.0, value=0.0, step=1.0, help="0 = ingen grense")

# Avanserte innstillinger
with st.expander("🎯 Avanserte innstillinger", expanded=False):
    source_sel = st.multiselect(
        "Match-metoder å inkludere",
        options=list(SOURCE_LABEL.values()),
        default=list(SOURCE_LABEL.values()),
        help="Velg hvilke matchingsmetoder som skal brukes"
    )

st.markdown("---")

run = st.button("▶️ Kjør matching", type="primary", use_container_width=True)

import io
import boto3

@st.cache_resource(show_spinner=False)
def s3_client():
    s = st.secrets["s3"]
    return boto3.client(
        "s3",
        aws_access_key_id=s["aws_access_key_id"],
        aws_secret_access_key=s["aws_secret_access_key"],
        region_name=s["region_name"],
    )

def read_csv_secure(obj_key, **kwargs):
    # bruk S3 i prod, lokal fallback ved utvikling
    local_path = f"data/{obj_key.split('/')[-1]}"
    if os.path.exists(local_path):
        return pd.read_csv(local_path, **kwargs)
    c = s3_client()
    b = st.secrets["s3"]["bucket"]
    p = st.secrets["s3"]["prefix"]
    key = f"{p}/{obj_key}".strip("/")
    obj = c.get_object(Bucket=b, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()), **kwargs)

# -------------------- Matching --------------------
if run:
    with st.spinner("Leser filer og matcher bedrifter..."):
        # Les filer
        hs_path = "data/hubspot.csv"
        brreg_path = "data/brreg.csv"
        
        read_kwargs = dict(sep=",", dtype=str, engine="python", on_bad_lines="skip", encoding="utf-8")
        try:
            # med dette:
            df_h = read_csv_secure("hubspot.csv", **read_kwargs)
            df_b = read_csv_secure("brreg.csv", **read_kwargs)

        except Exception as e:
            st.error(f"❌ Feil ved lesing av CSV: {e}")
            st.stop()

        HS_NAME_COL = "Company name"
        # Les og formater siste aktivitet fra HubSpot
        df_h["last_activity_raw"] = ensure_str_col(df_h, "Last Activity Date")
        df_h["last_activity_dt"]  = pd.to_datetime(df_h["last_activity_raw"], errors="coerce", utc=False)
        df_h["last_activity"]     = df_h["last_activity_dt"].apply(fmt_full_date)

        HS_ORGNR_COL = "Organisasjonsnummer"
        hs_key = "Record ID" if "Record ID" in df_h.columns else HS_NAME_COL

        df_h = df_h.drop_duplicates(subset=[hs_key])
        df_b = df_b.drop_duplicates(subset=["organisasjonsnummer"])

        # Filter Brreg på NACE
        df_b["naeringskode1.kode"] = ensure_str_col(df_b, "naeringskode1.kode").str.strip()
        
        prefixes = all_selected if all_selected else []
        if prefixes:
            mask = df_b["naeringskode1.kode"].str.startswith(tuple(prefixes))
        else:
            mask = pd.Series([True]*len(df_b))
        df_b = df_b.loc[mask].copy()

        # Rens
        df_b["organisasjonsnummer"] = ensure_str_col(df_b, "organisasjonsnummer").map(only_digits)
        df_b["navn"] = ensure_str_col(df_b, "navn")
        df_b["raw_name"] = df_b["navn"].map(raw_name)
        df_b["name_no_legal"] = df_b["navn"].map(lambda x: strip_words(x, LEGAL_WORDS))
        df_b["name_no_qual"]  = df_b["navn"].map(lambda x: strip_words(strip_words(x, LEGAL_WORDS), EXTRA_WORDS))

        df_h["company_name"] = ensure_str_col(df_h, HS_NAME_COL)
        df_h["organisasjonsnummer"] = ensure_str_col(df_h, HS_ORGNR_COL).map(only_digits)
        df_h["raw_name"] = df_h["company_name"].map(raw_name)
        df_h["name_no_legal"] = df_h["company_name"].map(lambda x: strip_words(x, LEGAL_WORDS))
        df_h["name_no_qual"]  = df_h["company_name"].map(lambda x: strip_words(strip_words(x, LEGAL_WORDS), EXTRA_WORDS))

        # Steg 1–4
        m_org = df_h.merge(
            df_b[["organisasjonsnummer","navn","raw_name","name_no_legal","name_no_qual","naeringskode1.kode","naeringskode1.beskrivelse"]],
            on="organisasjonsnummer", how="inner", suffixes=("_hub","_brreg")
        ); m_org["source"]="orgnr"; m_org["prio"]=1

        m_name_eq = df_h.merge(
            df_b[["raw_name","navn","organisasjonsnummer","naeringskode1.kode","naeringskode1.beskrivelse"]],
            on="raw_name", how="inner", suffixes=("_hub","_brreg")
        ); m_name_eq["source"]="name_eq"; m_name_eq["prio"]=2

        m_name_no_legal = df_h.merge(
            df_b[["name_no_legal","navn","organisasjonsnummer","naeringskode1.kode","naeringskode1.beskrivelse"]],
            on="name_no_legal", how="inner", suffixes=("_hub","_brreg")
        ); m_name_no_legal["source"]="name_no_legal_eq"; m_name_no_legal["prio"]=3

        m_name_no_qual = df_h.merge(
            df_b[["name_no_qual","navn","organisasjonsnummer","naeringskode1.kode","naeringskode1.beskrivelse"]],
            on="name_no_qual", how="inner", suffixes=("_hub","_brreg")
        ); m_name_no_qual["source"]="name_no_qual_eq"; m_name_no_qual["prio"]=4

        # 1–1 grådig per steg
        def prep_step(df_step: pd.DataFrame) -> pd.DataFrame:
            if df_step.empty: return df_step
            s = df_step.copy()
            s["hs_id"] = s[hs_key]
            if "organisasjonsnummer" in s.columns:
                org = s["organisasjonsnummer"].fillna("")
            elif "organisasjonsnummer_brreg" in s.columns:
                org = s["organisasjonsnummer_brreg"].fillna("")
            else:
                org = pd.Series([""]*len(s), index=s.index)
            s["brreg_id"] = org.where(org!="", s["navn"])
            return s

        steps = [prep_step(m_org), prep_step(m_name_eq), prep_step(m_name_no_legal), prep_step(m_name_no_qual)]
        used_hs, used_brreg, rows = set(), set(), []
        for s in steps:
            if s.empty: continue
            for _, r in s.sort_values(["prio"]).iterrows():
                if r["hs_id"] in used_hs: continue
                if r["brreg_id"] in used_brreg: continue
                rows.append(r); used_hs.add(r["hs_id"]); used_brreg.add(r["brreg_id"])

        best = pd.DataFrame(rows)
        if best.empty:
            st.warning("⚠️ Ingen matcher funnet med valgte filtre.")
            st.stop()

        best["source_label"] = best["source"].map(SOURCE_LABEL).fillna(best["source"])
        best = best[best["source_label"].isin(source_sel)].copy()

    # -------------------- API-berikelse (etter match) --------------------
    with st.spinner("Henter data fra Brønnøysund APIs..."):
        pair = best[["organisasjonsnummer_brreg", "organisasjonsnummer_hub"]].astype(str).applymap(only_digits)

        # Lag én kolonne med samme lengde som best
        best["orgnr"] = pair["organisasjonsnummer_brreg"].where(
            pair["organisasjonsnummer_brreg"] != "",
            pair["organisasjonsnummer_hub"]
        ).replace("", pd.NA)

        # Finn unike orgnr (uten NaN) til API-kall
        uniq_org = sorted(set(best["orgnr"].dropna().tolist()))

        emp_map = api_employees(uniq_org) if uniq_org else {}
        rev_map = api_revenue(uniq_org) if uniq_org else {}

        best["ansatte_api"] = best["orgnr"].map(emp_map)
        best["omsetning_api"] = best["orgnr"].map(rev_map)

        # Filtre ansatte og omsetning (indeks-aligned)
        min_rev = mnok_to_nok(min_rev_mnok)
        max_rev = mnok_to_nok(max_rev_mnok)

        emp = best["ansatte_api"]
        rev = best["omsetning_api"]

        mask_emp = pd.Series(True, index=best.index)
        if min_emp and min_emp > 0:
            mask_emp &= emp.ge(min_emp).fillna(False)
        if max_emp and max_emp > 0:
            mask_emp &= emp.le(max_emp).fillna(False)

        mask_rev = pd.Series(True, index=best.index)
        if min_rev is not None:
            mask_rev &= rev.ge(min_rev).fillna(False)
        if max_rev is not None and max_rev != 0:
            mask_rev &= rev.le(max_rev).fillna(False)

        best_f = best.loc[mask_emp & mask_rev].copy()


        # Enkle labels
        best_f["ansatte"] = best_f["ansatte_api"].apply(label_count_or_no)
        best_f["omsetning"] = best_f["omsetning_api"].apply(label_amount_or_no)

        # Visning
        show_cols = [
            "company_name","navn","orgnr","last_activity",
            "naeringskode1.kode","naeringskode1.beskrivelse",
            "ansatte","omsetning","source_label"
        ]
        for c in show_cols:
            if c not in best_f.columns: best_f[c] = ""

    # Resultater
    st.markdown("---")
    st.markdown("### 📊 Resultater")
    
    col_metric1, col_metric2, col_metric3 = st.columns(3)
    with col_metric1:
        st.metric("Totalt antall matcher", len(best_f))
    with col_metric2:
        avg_emp = best_f["ansatte_api"].dropna().mean()
        st.metric("Gjennomsnitt ansatte", f"{int(avg_emp)}" if not math.isnan(avg_emp) else "N/A")
    with col_metric3:
        avg_rev = best_f["omsetning_api"].dropna().mean()
        st.metric("Gjennomsnitt omsetning", f"{int(avg_rev/1_000_000)} MNOK" if not math.isnan(avg_rev) else "N/A")

    st.dataframe(
        best_f.sort_values(["omsetning_api"], ascending=False)[show_cols],
        use_container_width=True,
        height=500,
        column_config={
            "company_name": "HubSpot navn",
            "navn": "Brreg navn",
            "orgnr": "Org.nr",
            "last_activity": "Sist aktivitet",
            "naeringskode1.kode": "NACE",
            "naeringskode1.beskrivelse": "Bransje",
            "ansatte": "Ansatte",
            "omsetning": "Omsetning (NOK)",
            "source_label": "Match-metode"
        }
    )

    # --- Eksport ---
    csv = best_f.sort_values("omsetning_api", ascending=False)[show_cols].to_csv(index=False).encode("utf-8-sig")

    excel_buffer = pd.ExcelWriter("temp.xlsx", engine="openpyxl")
    best_f.sort_values("omsetning_api", ascending=False)[show_cols].to_excel(excel_buffer, index=False, sheet_name="Matcher")
    excel_buffer.close()
    with open("temp.xlsx", "rb") as f:
        excel_data = f.read()

    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button(
            "⬇️ Last ned som CSV",
            data=csv,
            file_name="hubspot_brreg_matches.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_dl2:
        st.download_button(
            "📘 Last ned som Excel",
            data=excel_data,
            file_name="hubspot_brreg_matches.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# -------------------- Dokumentasjon --------------------
st.markdown("---")
with st.expander("📖 Hvordan dette virker (under panseret)", expanded=False):
    st.markdown("""
    
    ### Matching-prosess (4 steg)
    
    Vi prøver å matche bedrifter fra HubSpot med Brønnøysundregisteret i prioritert rekkefølge på:
    
    1. **Organisasjonsnummer** – Direkte match på orgnr dersom det fins i HubSpot (sjeldent :/)
    2. **Eksakt navn** – Case-insensitiv likhet på normalisert bedriftsnavn
    3. **Navn uten juridiske endelser** – Fjerner AS, ASA, AB, LLC, etc.
    4. **Navn uten juridiske + ekstra ord** – Fjerner også "holding", "group", "konsern", "norge", etc.
    
    Hver bedrift fra HubSpot matches kun med én bedrift fra Brreg (1-til-1 matching). Ved flere kandidater velges den med høyest prioritet.
    
    ---
    
    ### API-berikelse
    
    Etter matching henter systemet tilleggsdata fra offentlige API-er:
    
    - **Antall ansatte**: Enhetsregisteret API
    - **Omsetning**: Regnskapsregisteret API (siste tilgjengelige regnskap)
    
    Manglende data vises som "ikke".
    
    ---
    
    ### NACE-koder (Standard for næringsgruppering)
    
    NACE er den offisielle standarden for næringsgrupperinndeling i Norge, basert på EUs NACE-standard. Kodene er organisert hierarkisk:
    
    - **1 bokstav** (A-U): Hovedområde (f.eks. F = Bygge- og anleggsvirksomhet)
    - **2 tall** (01-99): Næringsgruppe (f.eks. 41 = Oppføring av bygninger)
    - **3-5 tall**: Mer detaljert inndeling (ikke brukt i denne appen)
    
    **Eksempler på vanlige koder:**
    - **41-43**: Bygge- og anleggsvirksomhet
    - **62**: IT-konsulentvirksomhet
    - **68**: Eiendomsvirksomhet
    - **70**: Hovedkontortjenester
    - **46-47**: Varehandel
    
    **Nyttige ressurser:**
    - [SSB Klass - Søk i NACE-koder](https://www.ssb.no/klass/klassifikasjoner/6)
    - [Enhetsregisteret - Søk på bedrift](https://www.brreg.no/bedrift/)
    
    ---
    
    ### Filtrering
    
    - **Næringskoder**: Velg én eller flere NACE-koder fra kategoriene. Systemet bruker prefix-matching (f.eks. "41" matcher 41.100, 41.200, etc.)
    - **Ansatte**: Begrens til bedrifter med gitt antall ansatte
    - **Omsetning**: Filtrer på årlig omsetning i millioner NOK
    - **Match-metoder**: Velg hvilke matchingsmetoder som skal inkluderes i resultatet
    
    Sett verdi til 0 for å ikke bruke filteret.
    """)

st.markdown("---")
st.caption("Laget med Streamlit 🎈 | Data fra Brønnøysundregistrene")