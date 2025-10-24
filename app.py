# app.py
# Streamlit-søk i Brønnøysundregistrene for lead-liste
# Brukervennlige bransjepakker (NACE), ansattfilter og økonomifilter
# Viser treffliste og lar deg eksportere til CSV

import time
import math
import requests
import pandas as pd
import streamlit as st

ENHETSREG_BASE = "https://data.brreg.no/enhetsregisteret/api/enheter"
REGNSKAP_BASE = "https://data.brreg.no/regnskapsregisteret/regnskap"

# --------- Bransjepakker (NACE-prefix) ---------
BRANSJE_PRESETS = {
    "Detaljhandel (Retail)": ["47"],
    "Engroshandel": ["46"],
    "Bilhandel og verksteder": ["45"],
    "Kraft: produksjon": ["35.11"],
    "Kraft: overføring": ["35.12"],
    "Kraft: distribusjon": ["35.13"],
    "Kraft: handel": ["35.14"],
    "IT-konsulent og programmering": ["62.01", "62.02", "62.03", "62.09"],
    "Programvareutgivelse": ["58.29"],
    "Telekom": ["61"],
    "Bygg og anlegg": ["41", "42", "43"],
    "Industri": [
        "10","11","12","13","14","15","16","17","18","20","21","22","23","24",
        "25","26","27","28","29","30","31","32","33"
    ],
    "Transport og lagring": ["49","50","51","52","53"],
    "Overnatting og servering": ["55","56"],
    "Helse og sosial": ["86","87","88"],
    "Undervisning": ["85"],
    "Finans og forsikring": ["64","65","66"],
    "Eiendom": ["68"],
    "Profesjonelle tjenester": ["69","70","71","72","73","74","75"],  # 70.22 = management consulting
    "Vann, avløp og renovasjon": ["36","37","38","39"],
}

# ---------- Hjelpefunksjoner ----------

def _req_json(url, params=None, retries=3, timeout=20):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=timeout, headers={"Accept": "application/json"})
        if r.status_code == 200:
            return r.json()
        time.sleep(1.5 * (i + 1))
    r.raise_for_status()

def fetch_enheter(nace_prefixes=None, min_ansatte=None, max_ansatte=None, kommune=None, fylke=None, max_hits=2000):
    results = []
    size = 100
    page = 0
    pulled = 0

    base_params = {
        "size": size,
        "page": page,
        "hovedenhet": "true",
        "konkurs": "false",
        "underAvvikling": "false",
        "organisasjonsform": "AS,ASA,SÆR,FKF,IKS,SA"
    }
    if kommune:
        base_params["kommunenummer"] = kommune
    if fylke:
        base_params["fylkesnummer"] = fylke

    while True:
        params = dict(base_params)
        params["page"] = page
        data = _req_json(ENHETSREG_BASE, params=params)
        embedded = data.get("_embedded", {}) or {}
        enheter = embedded.get("enheter", []) or []
        if not enheter:
            break

        for e in enheter:
            ansatte = e.get("antallAnsatte")
            if min_ansatte is not None and (ansatte is None or ansatte < min_ansatte):
                continue
            if max_ansatte is not None and (ansatte is not None and ansatte > max_ansatte):
                continue

            if nace_prefixes:
                nk = (((e.get("naeringskode1") or {}).get("kode")) or "")
                if not any(nk.startswith(p) for p in nace_prefixes):
                    continue

            results.append(e)
            pulled += 1
            if pulled >= max_hits:
                break

        if pulled >= max_hits:
            break

        page += 1
        time.sleep(0.2)

    return results

def fetch_regnskap(orgnr):
    url = f"{REGNSKAP_BASE}/{orgnr}"
    try:
        data = _req_json(url)
    except Exception:
        return None
    if not data:
        return None

    if isinstance(data, dict) and isinstance(data.get("regnskap"), list) and data["regnskap"]:
        def _year(x):
            tp = x.get("periode", {}) or x.get("tidsperiode", {}) or {}
            return int(tp.get("regnskapsår") or tp.get("regnskapsaar") or tp.get("ar") or 0)
        data["regnskap"].sort(key=_year, reverse=True)
        latest = data["regnskap"][0]
    else:
        latest = data

    def pick(d, keys):
        for k in keys:
            if k in d and isinstance(d[k], (int, float)):
                return d[k]
        return None

    driftsinntekter_blokk = latest.get("driftsinntekter") or latest.get("Driftsinntekter") or {}
    resultat_blokk = latest.get("resultatregnskapResultat") or latest.get("ResultatregnskapResultat") or {}
    finans_blokk = latest.get("finansresultat") or latest.get("Finansresultat") or {}

    sum_driftsinntekter = (
        pick(latest, ["sumDriftsinntekter"]) or
        pick(driftsinntekter_blokk, ["sumDriftsinntekter", "SumDriftsinntekter"])
    )
    resultat_for_skatt = (
        pick(latest, ["resultatForSkatt", "ordinærtResultatFørSkatt", "ordinaertResultatForSkatt"]) or
        pick(resultat_blokk, ["resultatForSkatt", "ordinærtResultatFørSkatt", "ordinaertResultatForSkatt"]) or
        pick(finans_blokk, ["resultatForSkatt"])
    )

    periode = latest.get("periode") or latest.get("tidsperiode") or {}
    aar = (
        periode.get("regnskapsår") or
        periode.get("regnskapsaar") or
        periode.get("ar")
    )

    return {"aar": aar, "sumDriftsinntekter": sum_driftsinntekter, "resultatForSkatt": resultat_for_skatt}

def apply_financial_filters(df, min_rev, max_rev, min_profit, max_profit):
    def in_range(val, lo, hi):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return False
        if lo is not None and val < lo:
            return False
        if hi is not None and val > hi:
            return False
        return True

    mask = pd.Series([True] * len(df))
    if min_rev is not None or max_rev is not None:
        mask = mask & df["sumDriftsinntekter"].apply(lambda v: in_range(v, min_rev, max_rev))
    if min_profit is not None or max_profit is not None:
        mask = mask & df["resultatForSkatt"].apply(lambda v: in_range(v, min_profit, max_profit))
    return df[mask].reset_index(drop=True)

def merge_prefixes(selected_labels, free_text):
    prefixes = []
    for label in selected_labels:
        prefixes.extend(BRANSJE_PRESETS.get(label, []))
    if free_text:
        for part in free_text.split(","):
            p = part.strip()
            if p:
                prefixes.append(p)
    # fjern duplikater, behold rekkefølge
    seen = set()
    out = []
    for p in prefixes:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out

# ---------- UI ----------

st.set_page_config(page_title="Selskapsfinner • Brønnøysund + Regnskap", layout="wide")

st.title("Selskapsfinner for konsulentsalg")
st.caption("Kilder: Enhetsregisteret og Regnskapsregisteret (åpen del).")

with st.expander("Filter"):
    col1, col2, col3 = st.columns(3)

    with col1:
        presets = st.multiselect(
            "Bransjepakker",
            options=list(BRANSJE_PRESETS.keys()),
            default=["Detaljhandel (Retail)", "Kraft: produksjon", "IT-konsulent og programmering"],
            help="Velg én eller flere pakker. Systemet matcher på NACE-prefix."
        )
        free_prefix = st.text_input(
            "Egne NACE-prefix (kommaseparert)",
            placeholder="47, 35.11, 70.22"
        )
        min_ansatte = st.number_input("Min ansatte", min_value=0, value=10, step=1)
        max_ansatte = st.number_input("Maks ansatte", min_value=0, value=1000, step=1)

    with col2:
        min_rev_mnok = st.number_input("Min driftsinntekter (MNOK)", min_value=0.0, value=0.0, step=1.0)
        max_rev_mnok = st.number_input("Maks driftsinntekter (MNOK)", min_value=0.0, value=0.0, step=1.0, help="0 betyr ingen grense")
        min_profit_mnok = st.number_input("Min resultat før skatt (MNOK)", min_value=-1000.0, value=0.0, step=1.0)
        max_profit_mnok = st.number_input("Maks resultat før skatt (MNOK)", min_value=0.0, value=0.0, step=1.0, help="0 betyr ingen grense")

    with col3:
        kommune_nr = st.text_input("Kommunenummer", placeholder="0301 for Oslo", help="Valgfritt")
        fylke_nr = st.text_input("Fylkesnummer", placeholder="03 for Oslo", help="Valgfritt")
        max_hits = st.number_input("Maks enheter å hente", min_value=100, max_value=10000, value=2000, step=100)

    st.info("Velg bransjepakker eller legg til egne prefix. Økonomitall hentes fra siste innsendte regnskap.")

run = st.button("Søk")

# ---------- Kjøring ----------

if run:
    st.write("Henter kandidater fra Enhetsregisteret...")
    nace_prefixes = merge_prefixes(presets, free_prefix)
    enheter = fetch_enheter(
        nace_prefixes=nace_prefixes or None,
        min_ansatte=min_ansatte if min_ansatte > 0 else None,
        max_ansatte=max_ansatte if max_ansatte > 0 else None,
        kommune=kommune_nr or None,
        fylke=fylke_nr or None,
        max_hits=int(max_hits),
    )

    if not enheter:
        st.warning("Ingen kandidater funnet med disse filtrene.")
        st.stop()

    st.write(f"Fant {len(enheter)} kandidater. Henter nøkkeltall fra Regnskapsregisteret...")

    rows = []
    progress = st.progress(0)
    for i, e in enumerate(enheter, 1):
        orgnr = e.get("organisasjonsnummer")
        reg = fetch_regnskap(orgnr)
        progress.progress(i / len(enheter))

        navn = e.get("navn")
        nk = (e.get("naeringskode1") or {}).get("kode")
        nk_beskr = (e.get("naeringskode1") or {}).get("beskrivelse")
        ansatte = e.get("antallAnsatte")
        kommune = (e.get("forretningsadresse") or {}).get("kommune")
        fylke = (e.get("forretningsadresse") or {}).get("fylke")

        if reg:
            sum_driftsinntekter = reg.get("sumDriftsinntekter")
            resultat_for_skatt = reg.get("resultatForSkatt")
            aar = reg.get("aar")
        else:
            sum_driftsinntekter = None
            resultat_for_skatt = None
            aar = None

        rows.append({
            "orgnr": orgnr,
            "navn": navn,
            "naeringskode": nk,
            "naeringskode_beskrivelse": nk_beskr,
            "ansatte": ansatte,
            "kommune": kommune,
            "fylke": fylke,
            "regnskapsår": aar,
            "sumDriftsinntekter": sum_driftsinntekter,
            "resultatForSkatt": resultat_for_skatt,
        })
        time.sleep(0.05)

    df = pd.DataFrame(rows)

    def mnok_to_nok(x):
        return None if x == 0 else int(x * 1_000_000)

    min_rev = mnok_to_nok(min_rev_mnok)
    max_rev = mnok_to_nok(max_rev_mnok)
    min_profit = mnok_to_nok(min_profit_mnok)
    max_profit = mnok_to_nok(max_profit_mnok)

    df_filtered = apply_financial_filters(df, min_rev, max_rev, min_profit, max_profit)
    df_filtered = df_filtered.sort_values(
        by=["sumDriftsinntekter", "resultatForSkatt"],
        ascending=[False, False],
        na_position="last"
    ).reset_index(drop=True)

    st.success(f"Treff etter økonomifilter: {len(df_filtered)}")

    show_cols = [
        "navn", "orgnr", "naeringskode", "naeringskode_beskrivelse",
        "ansatte", "kommune", "fylke",
        "regnskapsår", "sumDriftsinntekter", "resultatForSkatt"
    ]
    st.dataframe(df_filtered[show_cols], use_container_width=True, height=520)

    csv = df_filtered.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Last ned som CSV", data=csv, file_name="selskapsfinner_treff.csv", mime="text/csv")

    with st.expander("Oppsummering"):
        def fmt_mnok(v):
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return None
            return round(v / 1_000_000, 2)
        st.write("Medianer i MNOK for treffene:")
        med_rev = fmt_mnok(df_filtered["sumDriftsinntekter"].median() if not df_filtered.empty else None)
        med_prof = fmt_mnok(df_filtered["resultatForSkatt"].median() if not df_filtered.empty else None)
        st.metric("Median driftsinntekter", med_rev)
        st.metric("Median resultat før skatt", med_prof)
