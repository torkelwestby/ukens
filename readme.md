.\.venv\Scripts\activate
python -m pip install streamlit
python -m streamlit run app.py


# ⚡ HubSpot ↔ Brønnøysund Matcher

En enkel og fleksibel **Streamlit-app** for å matche selskaper fra **HubSpot CRM** mot **Brønnøysundregistrene**, og hente ut nøkkeltall som **ansatte** og **omsetning** fra offentlige API-er.  
Perfekt for salgsanalyse, verdivurdering og lead-filtrering basert på næringskode, størrelse og aktivitet.  

---

## 🚀 Funksjonalitet

- 🔹 Matcher HubSpot-selskaper mot Brønnøysund via fire steg:
  1. **Organisasjonsnummer**
  2. **Eksakt navn**
  3. **Navn uten juridiske endelser** (AS, ASA, AB, osv.)
  4. **Navn uten juridiske + ekstra ord** (Holding, Group, Norge, osv.)
- ⚙️ Henter **ansatte** og **omsetning** via åpne Brønnøysund API-er.
- 🔎 Filtrer på:
  - NACE-/næringskoder (fra ferdigpakker eller egendefinert input)
  - Antall ansatte (min–maks)
  - Omsetning (min–maks, i MNOK)
  - Valgte matchkilder (orgnr, navn osv.)
- 📊 Viser resultater i interaktiv tabell og lar deg **laste ned CSV**.
- 🧩 Kan utvides med egne filtreringslogikker eller visualiseringer.

---

## 📁 Mappestruktur

📦 prosjektmappe
┣ 📂 data
┃ ┣ 📄 hubspot.csv
┃ ┗ 📄 brreg.csv
┣ 📄 app.py
┗ 📄 README.md


**hubspot.csv** må minst inneholde kolonnene:
- `Company name`
- `Organisasjonsnummer`
- (valgfritt) `Record ID`

**brreg.csv** må minst inneholde:
- `organisasjonsnummer`
- `navn`
- `naeringskode1.kode`
- `naeringskode1.beskrivelse`

---

## 💻 Installasjon og kjøring

### 1. Lag og aktiver virtuelt miljø
```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
