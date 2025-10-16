# %% COLAB CELL — QCEW → Monthly NAICS (2-digit), Statewide, 2019–2024
# Single pass: throttled downloads, local cache, resume, progress bar
# Designed to work in google collab
#
import os, io, time, math, json, calendar, hashlib, requests
import pandas as pd
from tqdm.auto import tqdm

START_YEAR = 2019
END_YEAR   = 2024
QUARTERS   = [1,2,3,4]
SLEEP_BETWEEN_REQUESTS = 0.4     # gentle throttle
RETRIES = 3
RETRY_SLEEP = 1.5

# Two-letter -> FIPS (2-digit string)
STATE_FIPS = {'AL':'01','AK':'02','AZ':'04','AR':'05','CA':'06','CO':'08','CT':'09','DE':'10','DC':'11','FL':'12','GA':'13',
'HI':'15','ID':'16','IL':'17','IN':'18','IA':'19','KS':'20','KY':'21','LA':'22','ME':'23','MD':'24','MA':'25','MI':'26',
'MN':'27','MS':'28','MO':'29','MT':'30','NE':'31','NV':'32','NH':'33','NJ':'34','NM':'35','NY':'36','NC':'37','ND':'38',
'OH':'39','OK':'40','OR':'41','PA':'42','RI':'44','SC':'45','SD':'46','TN':'47','TX':'48','UT':'49','VT':'50','VA':'51',
'WA':'53','WV':'54','WI':'55','WY':'56'}

# ALL states (default); change to a subset if you want a quick test
STATES = list(STATE_FIPS.keys())

# Your 15 NAICS 2-digit
NAICS_LIST = {54,62,44,31,72,23,48,61,52,51,92,71,11,22,81}

# QCEW area slice URL (CSV): https://data.bls.gov/cew/data/api/{year}/{quarter}/area/{area_fips}.csv
BASE = "https://data.bls.gov/cew/data/api/{year}/{q}/area/{area}.csv"

CACHE_DIR = "/content/qcew_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
MANIFEST_PATH = os.path.join(CACHE_DIR, "manifest.json")
manifest = json.load(open(MANIFEST_PATH)) if os.path.exists(MANIFEST_PATH) else {}

def to_date_id(y, m):
    last = calendar.monthrange(y, m)[1]
    return y*10000 + m*100 + last

def normalize_sector_code(industry_code: str):
    """Map QCEW sector 'industry_code' to our target 2-digit NAICS.
       Handles '31-33','44-45','48-49' by taking the first 2 digits."""
    if not isinstance(industry_code, str) or industry_code == "":
        return None
    if "-" in industry_code:
        return int(industry_code.split("-")[0][:2])
    if industry_code.isdigit() and len(industry_code) == 2:
        return int(industry_code)
    try:
        return int(industry_code[:2])
    except Exception:
        return None

def cache_key(state, year, q):
    return f"{state}_{year}_Q{q}.csv"

def cached_path(key):
    return os.path.join(CACHE_DIR, key)

def fetch_state_quarter_csv(state_abbr: str, year: int, q: int) -> pd.DataFrame:
    """Download (or read cached) QCEW area slice CSV for (state,year,quarter)."""
    key = cache_key(state_abbr, year, q)
    path = cached_path(key)

    # Use cache if present
    if os.path.exists(path):
        df = pd.read_csv(path)
        return df

    # Otherwise download with retries
    fips = STATE_FIPS[state_abbr]
    area = fips + "000"   # statewide area_fips
    url = BASE.format(year=year, q=q, area=area)
    last_err = None
    for attempt in range(1, RETRIES+1):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            df = pd.read_csv(io.StringIO(r.text))
            # Save to cache for resume
            df.to_csv(path, index=False)
            manifest[key] = {"state": state_abbr, "year": year, "quarter": q, "cached": True}
            json.dump(manifest, open(MANIFEST_PATH, "w"))
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            return df
        except Exception as e:
            last_err = e
            time.sleep(RETRY_SLEEP)
    raise last_err

def coerce_numeric(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

def process_one(st, y, q) -> pd.DataFrame:
    df = fetch_state_quarter_csv(st, y, q)

    # Coerce dtypes used in filters
    coerce_numeric(df, ["agglvl_code", "own_code", "qtr", "year",
                        "month1_emplvl","month2_emplvl","month3_emplvl"])

    df["state_code"] = st

    # Keep statewide × NAICS sector (agglvl_code=54); ownerships may be split (1,2,3,5)
    subset = df[df["agglvl_code"] == 54].copy()
    if subset.empty:
        return pd.DataFrame(columns=["date_id","state_code","naics_code","employment_count","source_system"])

    subset["naics_code"] = subset["industry_code"].apply(normalize_sector_code)
    subset = subset[subset["naics_code"].isin(NAICS_LIST)].copy()
    if subset.empty:
        return pd.DataFrame(columns=["date_id","state_code","naics_code","employment_count","source_system"])

    # Build total ownership: use own_code==0 if present else sum 1+2+3+5
    if (subset["own_code"] == 0).any():
        tot = subset[subset["own_code"] == 0].copy()
    else:
        owned = subset[subset["own_code"].isin([1,2,3,5])].copy()
        if owned.empty:
            return pd.DataFrame(columns=["date_id","state_code","naics_code","employment_count","source_system"])
        tot = (owned.groupby(["state_code","year","qtr","naics_code"], as_index=False)
                    [["month1_emplvl","month2_emplvl","month3_emplvl"]].sum())

    # Melt to month rows inside the quarter
    melt = tot.melt(
        id_vars=["state_code","year","qtr","naics_code"],
        value_vars=["month1_emplvl","month2_emplvl","month3_emplvl"],
        var_name="month_in_q",
        value_name="employment"
    )
    if melt.empty:
        return pd.DataFrame(columns=["date_id","state_code","naics_code","employment_count","source_system"])

    # Map month_in_q -> absolute month 1..12 for that quarter
    def abs_month(qtr, month_in_q):
        base = (int(qtr)-1)*3
        idx = {"month1_emplvl":1, "month2_emplvl":2, "month3_emplvl":3}[month_in_q]
        return base + idx
    melt["month"] = [abs_month(qtr, miq) for qtr, miq in zip(melt["qtr"], melt["month_in_q"])]
    melt.drop(columns=["month_in_q"], inplace=True)

    melt["date_id"] = [to_date_id(int(yy), int(mm)) for yy, mm in zip(melt["year"], melt["month"])]
    melt["employment_count"] = pd.to_numeric(melt["employment"], errors="coerce").round().astype("Int64")
    melt["source_system"] = "QCEW sector (summed ownerships)"

    return melt[["date_id","state_code","naics_code","employment_count","source_system"]]

# -------- Run single pass over all states/years/quarters with resume & progress --------
tasks = [(st, y, q) for st in STATES for y in range(START_YEAR, END_YEAR+1) for q in QUARTERS]
rows = []
pbar = tqdm(total=len(tasks), desc="QCEW slices")

for st, y, q in tasks:
    try:
        part = process_one(st, y, q)
        if not part.empty:
            rows.append(part)
    except Exception as e:
        # Keep going on errors; you can log or inspect if needed
        # print(f"Skip {st} {y} Q{q}: {e}")
        pass
    pbar.update(1)

pbar.close()

if rows:
    out = pd.concat(rows, ignore_index=True)
    out.drop_duplicates(subset=["date_id","state_code","naics_code"], inplace=True)
    out.sort_values(["date_id","state_code","naics_code"], inplace=True)
else:
    out = pd.DataFrame(columns=["date_id","state_code","naics_code","employment_count","source_system"])

print(f"Rows written: {len(out):,}")
out.to_csv("fact_industry_employment_monthly_naics.csv", index=False)
print("Wrote fact_industry_employment_monthly_naics.csv")
