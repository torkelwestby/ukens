# app.py
# Streamlit-søk i Brønnøysundregistrene for lead-liste
# Filtrerer på NACE (naeringskode), antall ansatte, driftsinntekter og resultat før skatt
# Viser treffliste og lar deg eksportere til CSV

import time
import math
import requests
import pandas as pd
import streamlit as st

ENHETSREG_BASE = "https://data.brreg.no/enhetsregisteret/api/enheter"
REGNSKAP_BASE = "https://data.brreg.no/regnskapsregisteret/regnskap"

# ---------- Hjelpefunksjoner ----------

def _req_json(url, params=None, retries=3, timeout=20):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=timeout, headers={"Accept": "application/json"})
        if r.status_code == 200:
            return r.json()
        # 429 eller 5xx kan forekomme. Backoff.
        time.sleep(1.5 * (i + 1))
    r.raise_for_status()

def fetch_enheter(nace_prefixes=None, min_ansatte=None, max_ansatte=None, kommune=None, fylke=None, max_hits=2000):
    """
    Henter kandidater fra Enhetsregisteret. Bruker paginering.
    Filtrerer mest mulig server-side og resten client-side.
    """
    results = []
    size = 100
    page = 0
    pulled = 0

    # Bygg query
    # Merk: API-et har begrenset server-side filtrering. Vi bruker tilgjengelige parametre der det gir effekt.
    base_params = {
        "size": size,
        "page": page,
        "hovedenhet": "true",
        "konkurs": "false",
        "underAvvikling": "false",
        "organisasjonsform": "AS,ASA,SÆR,FKF,IKS,SA"  # typiske målgrupper. Juster ved behov.
    }

    if kommune:
        base_params["kommunenummer"] = kommune
    if fylke:
        base_params["fylkesnummer"] = fylke
    # antall ansatte har ikke eksakt serverfilter. Vi henter bredt og filtrerer lokalt.

    while True:
        params = dict(base_params)
        params["page"] = page
        data = _req_json(ENHETSREG_BASE, params=params)
        embedded = data.get("_embedded", {}) or {}
        enheter = embedded.get("enheter", []) or []
        if not enheter:
            break

        for e in enheter:
            # Lokal filtrering
            ansatte = e.get("antallAnsatte")
            if min_ansatte is not None and (ansatte is None or ansatte < min_ansatte):
                continue
            if max_ansatte is not None and (ansatte is not None and ansatte > max_ansatte):
                continue

            if nace_prefixes:
                # sjekk naeringskode1.kode starter med ett av prefixene
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
        # vennlig fartsgrense
        time.sleep(0.2)

    return results

def fetch_regnskap(orgnr):
    """
    Henter siste åpne regnskap. Åpen del gir nøkkeltall for sist innsendte år.
    Struktur i respons kan variere litt mellom foretak.
    """
    url = f"{REGNSKAP_BASE}/{orgnr}"
    try:
        data = _req_json(url)
    except Exception:
        return None

    # Finn siste år
    if not data:
        return None

    # Typisk struktur: {"regnskap": [{...}, {...}] } eller direkte felter
    # Vi forsøker å plukke siste element hvis liste finnes
    if isinstance(data, dict) and "regnskap" in data and isinstance(data["regnskap"], list) and data["regnskap"]:
        # sortér på år hvis mulig
        def _year(x):
            tp = x.get("periode", {}) or x.get("tidsperiode", {}) or {}
            return int(tp.get("regnskapsår") or tp.get("regnskapsaar") or tp.get("ar") or 0)
        data["regnskap"].sort(key=_year, reverse=True)
        latest = data["regnskap"][0]
    else:
        latest = data

    # Forsøk å lese driftsinntekter og resultat før skatt
    # Feltnavn varierer. Vi sjekker flere muligheter.
    def pick(d, keys):
        for k in keys:
            if k in d and isinstance(d[k], (int, float)):
                return d[k]
        return None

    # Noen ganger ligger tallene under delobjekter
    driftsinntekter_blokk = latest.get("driftsinntekter") or latest.get("Driftsinntekter") or {}
    resultat_blokk = latest.get("resultatregnskapResultat") or latest.get("ResultatregnskapResultat") or {}
    finans_blokk = latest.get("finansresultat") or latest.get("Finansresultat") or {}

    sum_driftsinntekter = (
        pick(latest, ["sumDriftsinntekter"]) or
        pick(driftsinntekter_blokk, ["sumDriftsinntekter", "SumDriftsinntekter"])
    )

    # Resultat før skatt kan hete resultatForSkatt, ordinærtResultatFørSkatt, resultatForSkattSum
    resultat_for_skatt = (
        pick(latest, ["resultatForSkatt", "ordinærtResultatFørSkatt", "ordinaertResultatForSkatt"]) or
        pick(resultat_blokk, ["resultatForSkatt", "ordinærtResultatFørSkatt", "ordinaertResultatForSkatt"]) or
        pick(finans_blokk, ["resultatForSkatt"])
    )

    # Hent årstall hvis mulig
    periode = latest.get("periode") or latest.get("tidsperiode") or {}
    aar = (
        periode.get("regnskapsår") or
        periode.get("regnskapsaar") or
        periode.get("ar")
    )

    return {
        "aar": aar,
        "sumDriftsinntekter": sum_driftsinntekter,
        "resultatForSkatt": resultat_for_skatt
    }

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

# ---------- UI ----------

st.set_page_config(page_title="Selskapsfinner • Brønnøysund + Regnskap", layout="wide")

st.title("Selskapsfinner for konsulentsalg")
st.caption("Kilde: Enhetsregisteret og Regnskapsregisteret (åpen del).")

with st.expander("Filter"):
    col1, col2, col3 = st.columns(3)

    with col1:
        nace_help = "Bruk NACE-prefix for bransje. Eksempler: 47 for detaljhandel. 35.1 for kraftforsyning. 62 for IT-tjenester."
        nace_input = st.tags_input("NACE-prefix", suggestions=["47", "35.11", "35.12", "35.13", "62", "70.22"], help=nace_help)
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

    st.info("Tip: Start bredt på Enhetsregisteret, snevre inn med økonomifilter. Økonomitall er fra siste innsendte regnskap.")

run = st.button("Søk")

# ---------- Kjøring ----------

if run:
    st.write("Henter kandidater fra Enhetsregisteret...")
    nace_prefixes = [p.strip() for p in (nace_input or []) if p.strip()]
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
        # Oppdater fremdrift
        progress.progress(i / len(enheter))

        # Bygg rad
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

        # vennlig fartsgrense
        time.sleep(0.05)

    df = pd.DataFrame(rows)

    # Konverter MNOK-input til NOK
    def mnok_to_nok(x):
        return None if x == 0 else int(x * 1_000_000)

    min_rev = mnok_to_nok(min_rev_mnok)
    max_rev = mnok_to_nok(max_rev_mnok)
    min_profit = mnok_to_nok(min_profit_mnok)
    max_profit = mnok_to_nok(max_profit_mnok)

    # Filtrer på økonomi
    df_filtered = apply_financial_filters(df, min_rev, max_rev, min_profit, max_profit)

    # Sorter på driftsinntekter synkende
    df_filtered = df_filtered.sort_values(by=["sumDriftsinntekter", "resultatForSkatt"], ascending=[False, False], na_position="last").reset_index(drop=True)

    st.success(f"Treff etter økonomifilter: {len(df_filtered)}")

    # Vis tabell
    show_cols = [
        "navn", "orgnr", "naeringskode", "naeringskode_beskrivelse",
        "ansatte", "kommune", "fylke",
        "regnskapsår", "sumDriftsinntekter", "resultatForSkatt"
    ]
    st.dataframe(df_filtered[show_cols], use_container_width=True, height=520)

    # Last ned CSV
    csv = df_filtered.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Last ned som CSV", data=csv, file_name="selskapsfinner_treff.csv", mime="text/csv")

    # Hurtigoppsummering
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

