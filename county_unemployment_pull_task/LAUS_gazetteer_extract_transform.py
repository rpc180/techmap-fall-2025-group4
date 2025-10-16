# %% COLAB — LAUS County (2019–2024) → Fact + Gazetteer lat/long → Dim
# Outputs:
#   - fact_unemployment_county_monthly.csv
#   - dim_geography_county_expanded.csv
#
# Warehouse key: county FIPS GEOID (5-digit) → geography_key
# Designed to run in google collab

import os, io, re, requests, zipfile
import pandas as pd
from tqdm.auto import tqdm
from google.colab import files

# ---------------- Config ----------------
LAUS_BASE = "https://download.bls.gov/pub/time.series/la/"
LAUS_FILES = {
    "series": "la.series",               # meta: series_id, seasonal, area_code, measure_code...
    "measure": "la.measure",             # measure_code -> measure_text
    "area": "la.area",                   # area_code -> area_text/type
    "data_county": "la.data.64.County",  # monthly county data
}
START_YEAR, END_YEAR = 2019, 2024

# Gazetteer (counties) — ZIP contains the TXT with INTPTLAT/LONG
GAZ_ZIP_URL = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2023_Gazetteer/2023_Gaz_counties_national.zip"
GAZ_ZIP_PATH = "2023_Gaz_counties_national.zip"

# Regions/Divisions per USPS (optional enrich)
STATE_TO_REGION = {
    # Northeast
    "CT":"Northeast","ME":"Northeast","MA":"Northeast","NH":"Northeast","RI":"Northeast","VT":"Northeast",
    "NJ":"Northeast","NY":"Northeast","PA":"Northeast",
    # Midwest
    "IL":"Midwest","IN":"Midwest","MI":"Midwest","OH":"Midwest","WI":"Midwest",
    "IA":"Midwest","KS":"Midwest","MN":"Midwest","MO":"Midwest","NE":"Midwest","ND":"Midwest","SD":"Midwest",
    # South
    "DE":"South","FL":"South","GA":"South","MD":"South","NC":"South","SC":"South","VA":"South","DC":"South","WV":"South",
    "AL":"South","KY":"South","MS":"South","TN":"South","AR":"South","LA":"South","OK":"South","TX":"South",
    # West
    "AZ":"West","CO":"West","ID":"West","MT":"West","NV":"West","NM":"West","UT":"West","WY":"West",
    "AK":"West","CA":"West","HI":"West","OR":"West","WA":"West",
}
STATE_TO_DIVISION = {
    "CT":"New England","ME":"New England","MA":"New England","NH":"New England","RI":"New England","VT":"New England",
    "NJ":"Middle Atlantic","NY":"Middle Atlantic","PA":"Middle Atlantic",
    "IL":"East North Central","IN":"East North Central","MI":"East North Central","OH":"East North Central","WI":"East North Central",
    "IA":"West North Central","KS":"West North Central","MN":"West North Central","MO":"West North Central","NE":"West North Central","ND":"West North Central","SD":"West North Central",
    "DE":"South Atlantic","FL":"South Atlantic","GA":"South Atlantic","MD":"South Atlantic","NC":"South Atlantic","SC":"South Atlantic","VA":"South Atlantic","DC":"South Atlantic","WV":"South Atlantic",
    "AL":"East South Central","KY":"East South Central","MS":"East South Central","TN":"East South Central",
    "AR":"West South Central","LA":"West South Central","OK":"West South Central","TX":"West South Central",
    "AZ":"Mountain","CO":"Mountain","ID":"Mountain","MT":"Mountain","NV":"Mountain","NM":"Mountain","UT":"Mountain","WY":"Mountain",
    "AK":"Pacific","CA":"Pacific","HI":"Pacific","OR":"Pacific","WA":"Pacific",
}
STATE_NAME = {
 'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California','CO':'Colorado','CT':'Connecticut','DE':'Delaware','DC':'District of Columbia',
 'FL':'Florida','GA':'Georgia','HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa','KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine',
 'MD':'Maryland','MA':'Massachusetts','MI':'Michigan','MN':'Minnesota','MS':'Mississippi','MO':'Missouri','MT':'Montana','NE':'Nebraska','NV':'Nevada',
 'NH':'New Hampshire','NJ':'New Jersey','NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio','OK':'Oklahoma','OR':'Oregon',
 'PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina','SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont','VA':'Virginia',
 'WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming'
}

# ---------------- Helpers ----------------
def download_file(url: str, out_path: str, desc: str):
    if os.path.exists(out_path):
        return
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    with requests.get(url, stream=True, timeout=60, headers=headers) as r:
        if r.status_code != 200:
            raise RuntimeError(f"Failed to download {url} — HTTP {r.status_code}")
        total = int(r.headers.get('content-length', 0))
        with open(out_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, desc=desc) as pbar:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))

def read_laus_tsv(path):
    try:
        df = pd.read_csv(path, sep="\t", dtype=str, na_filter=False, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep="\t", dtype=str, na_filter=False, encoding="latin-1")
    df.columns = [c.strip().lower() for c in df.columns]
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].str.strip()
    return df

def extract_geoid_from_area_code(area_code: str):
    if not isinstance(area_code, str):
        return None
    m = re.search(r'(\d{5})', area_code)
    return m.group(1) if m else None

def extract_geoid_from_series_id(series_id: str):
    if not isinstance(series_id, str):
        return None
    m = re.search(r'(\d{5})', series_id)
    return m.group(1) if m else None

# ---------------- Step 1: Download LAUS files ----------------
print("Downloading LAUS county files…")
for key, fname in LAUS_FILES.items():
    download_file(LAUS_BASE + fname, fname, desc=fname)

# ---------------- Step 2: Load LAUS metadata ----------------
print("Loading LAUS metadata…")
series  = read_laus_tsv("la.series")       # series_id, seasonal, area_code, measure_code, ...
measure = read_laus_tsv("la.measure")      # measure_code, measure_text
area    = read_laus_tsv("la.area")         # area_code, area_text, area_type_code, ...

# ---- Robust measure mapping by keywords (no exact-text dependency) ----
m = measure.copy()
m["mtxt"] = m["measure_text"].str.lower()

def canon_from_text(t: str):
    t = t.lower()
    if "unemploy" in t and "rate" in t:
        return "unemployment_rate"
    if "labor" in t and "force" in t:
        return "labor_force"
    if "unemploy" in t and "rate" not in t:
        return "unemployed"
    if "employ" in t and "unemploy" not in t and "rate" not in t:
        return "employed"
    return None

m["canon_name"] = m["mtxt"].apply(canon_from_text)
m = m.dropna(subset=["canon_name"])
m = m[m["canon_name"].isin(["labor_force","employed","unemployed","unemployment_rate"])]

code_to_name = dict(zip(m["measure_code"], m["canon_name"]))
keep_codes = set(code_to_name.keys())

# ---- County series: include S & U; prefer S when both exist for same (county, measure) ----
series["is_county_series"] = series["series_id"].str.startswith("LAUCN")
ser_raw = series[
    (series["is_county_series"]) &
    (series["seasonal"].str.upper().isin(["S","U"])) &
    (series["measure_code"].isin(keep_codes))
][["series_id","area_code","measure_code","seasonal"]].copy()

ser_raw = ser_raw.merge(area[["area_code","area_text","area_type_code"]], on="area_code", how="left")

ser_raw["geoid"] = ser_raw["area_code"].apply(extract_geoid_from_area_code)
ser_raw.loc[ser_raw["geoid"].isna(), "geoid"] = ser_raw["series_id"].apply(extract_geoid_from_series_id)
ser_raw = ser_raw.dropna(subset=["geoid"])

pref = {"S":1,"U":2}
ser_raw["season_rank"] = ser_raw["seasonal"].str.upper().map(pref).fillna(99)
ser = (ser_raw.sort_values(["geoid","measure_code","season_rank"])
              .groupby(["geoid","measure_code"], as_index=False)
              .first())

print("Seasonal mix kept (S preferred):", ser["seasonal"].value_counts(dropna=False).to_dict())

# ---------------- Step 3: Load County data table ----------------
print("Loading LAUS county monthly data…")
data = read_laus_tsv("la.data.64.County")  # series_id, year, period, value, footnote_codes...

# Filter to our series, years, monthly; coerce numeric
data = data[data["series_id"].isin(ser["series_id"])]
data["year"] = pd.to_numeric(data["year"], errors="coerce").astype("Int64")
data = data[(data["year"] >= START_YEAR) & (data["year"] <= END_YEAR)]
data = data[data["period"].str.match(r"^M\d{2}$")]
data["value"] = pd.to_numeric(data["value"], errors="coerce")

# Attach metadata + build date_id
df = (data.merge(ser, on="series_id", how="left")
          .merge(m[["measure_code","canon_name"]], on="measure_code", how="left"))
df["month"]   = df["period"].str[1:].astype(int)
df["date_id"] = df["year"]*100 + df["month"]

# Pivot by measure_code → canonical names (stable)
pv = (df.pivot_table(index=["geoid","date_id"],
                     columns="canon_name",
                     values="value",
                     aggfunc="first")
        .reset_index())

# Ensure all four columns exist
for col in ["labor_force","employed","unemployed","unemployment_rate"]:
    if col not in pv.columns:
        pv[col] = pd.NA

# ---------------- Step 4: Download & load Gazetteer (county centroids) ----------------
print("Downloading Census Gazetteer (counties)…")
download_file(GAZ_ZIP_URL, GAZ_ZIP_PATH, desc="Gazetteer (zip)")

with zipfile.ZipFile(GAZ_ZIP_PATH, 'r') as zf:
    txt_name = [m for m in zf.namelist() if m.lower().endswith(".txt")][0]
    with zf.open(txt_name) as f:
        gaz = pd.read_csv(io.TextIOWrapper(f, encoding="utf-8"), sep="\t", dtype=str)

gaz.columns = [c.strip() for c in gaz.columns]
for c in gaz.select_dtypes(include="object").columns:
    gaz[c] = gaz[c].str.strip()
for col in ["INTPTLAT","INTPTLONG"]:
    if col in gaz.columns:
        gaz[col] = pd.to_numeric(gaz[col], errors="coerce")

dim_geo = gaz[["GEOID","USPS","NAME","INTPTLAT","INTPTLONG","ALAND","AWATER"]].copy()
dim_geo = dim_geo.rename(columns={
    "GEOID":"geography_key",
    "USPS":"state_code",
    "NAME":"county_name",
    "INTPTLAT":"latitude",
    "INTPTLONG":"longitude",
    "ALAND":"aland",
    "AWATER":"awater",
})
dim_geo["region"] = dim_geo["state_code"].map(STATE_TO_REGION)
dim_geo["division"] = dim_geo["state_code"].map(STATE_TO_DIVISION)
dim_geo["state_name"] = dim_geo["state_code"].map(STATE_NAME)

# ---------------- Step 5: Build Fact table with GEOID as geography_key ----------------
with tqdm(total=1, desc="Merging fact with Gazetteer") as pbar:
    fact = pv.merge(
        dim_geo[["geography_key","state_code","state_name"]],
        left_on="geoid", right_on="geography_key", how="inner"
    )
    pbar.update(1)

fact_out = (fact[["geography_key","date_id","labor_force","employed","unemployed","unemployment_rate"]]
            .sort_values(["geography_key","date_id"])
            .reset_index(drop=True))

# ---------------- Step 6: Write CSVs + downloads ----------------
fact_path = "fact_unemployment_county_monthly.csv"
dim_path  = "dim_geography_county_expanded.csv"
fact_out.to_csv(fact_path, index=False)
dim_geo.to_csv(dim_path, index=False)

print(f"\n✅ Wrote {len(fact_out):,} rows → {fact_path}")
print(f"   Unique counties: {fact_out['geography_key'].nunique():,}")
print(f"   Date span: {fact_out['date_id'].min()} .. {fact_out['date_id'].max()}")
print(f"✅ Wrote {len(dim_geo):,} rows → {dim_path}")

print("\nSample output (fact):")
display(fact_out.head(10))
print("\nSample output (dim):")
display(dim_geo.head(10))

# Offer downloads
files.download(fact_path)
files.download(dim_path)
