#Loading the data and pre processing it

# src/data_prep.py
from fastf1 import get_session
import fastf1
import pandas as pd
import os

# src/data_prep.py
from fastf1 import get_session
import fastf1, pandas as pd, os, time, logging

# Keep FastF1 logs quieter (optional)
#logging.getLogger("fastf1").setLevel(logging.ERROR)

# Local cache (avoid OneDrive)
fastf1.Cache.enable_cache("cache")

def _load_with_retries(year, ident, kind, retries=2, wait=2):
    last = None
    for _ in range(retries + 1):
        try:
            s = get_session(year, ident, kind)
            # Fewer endpoints -> fewer ways to fail
            s.load(telemetry=False, weather=False, messages=False)
            return s
        except Exception as e:
            last = e
            time.sleep(wait)
    raise last
years=(2024, 2025)
races=races = [
    'Australian Grand Prix', 'Chinese Grand Prix', 'Japanese Grand Prix', 'Bahrain Grand Prix',
    'Saudi Arabian Grand Prix', 'Miami Grand Prix', 'Emilia Romagna Grand Prix', 'Monaco Grand Prix',
    'Spanish Grand Prix', 'Canadian Grand Prix', 'Austrian Grand Prix', 'British Grand Prix',
    'Belgian Grand Prix', 'Hungarian Grand Prix', 'Dutch Grand Prix', 'Italian Grand Prix',
    'Azerbaijan Grand Prix', 'Singapore Grand Prix', 'United States Grand Prix',
    'Mexico City Grand Prix', 'São Paulo Grand Prix', 'Las Vegas Grand Prix',
    'Qatar Grand Prix', 'Abu Dhabi Grand Prix'
]
def collect_final_results(years=years, max_rounds=30):
    rows = []

    for year in years:
        for race in races:
            # ---- Race ----
            try:
                racedata = _load_with_retries(year, race, "R")
            except Exception:
                continue

            rdf = racedata.results.copy()
            if rdf is None or rdf.empty:
                continue
            rdf = rdf.set_index("DriverNumber", drop=False)

            # Safe race name without touching schedule APIs
            race_name = (getattr(race, "name", None) or f"Round {race}").split(" - ")[0]
            round_no = race
    

            # ---- Qualifying (optional) ----
            qdf, pole_time = pd.DataFrame(), pd.NaT
            try:
                quali = _load_with_retries(year, race, "Q")
                qdf = quali.results.copy()
                if not qdf.empty:
                    qdf = qdf.set_index("DriverNumber", drop=False)
                    qdf["BestQ"] = qdf["Q3"].combine_first(qdf["Q2"]).combine_first(qdf["Q1"])
                    pole_time = qdf["BestQ"].min() if qdf["BestQ"].notna().any() else pd.NaT
            except Exception:
                pass  # proceed without quali

            # ---- Build rows ----
            for drv_num, r in rdf.iterrows():
                abbr   = r.get("Abbreviation")
                team   = r.get("TeamName")
                grid   = r.get("GridPosition")
                finish = r.get("Position")
                points = r.get("Points")
                status = r.get("Status")

                q_pos, q_time, q_gap = None, pd.NaT, pd.NaT
                if not qdf.empty and drv_num in qdf.index:
                    qrow  = qdf.loc[drv_num]
                    q_pos = qrow.get("Position")
                    q_time = qrow.get("BestQ")
                    if pd.notna(q_time) and pd.notna(pole_time):
                        q_gap = q_time - pole_time

                rows.append({
                    "Season": year,
                    "RaceName": race_name,
                    "DriverNumber": int(drv_num) if pd.notna(drv_num) else None,
                    "Driver": abbr,
                    "Team": team,
                    "GridPos": grid,
                    "FinishPos": finish,
                    "Points": points,
                    "Status": status,
                    "QPos": q_pos,
                    "QTime": q_time,
                    "QGapToPole": q_gap
                })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = collect_final_results(years=(2024, 2025))
    print(df.head(12))
    os.makedirs("data", exist_ok=True)
    df.to_parquet("data/f1_final_results.parquet", index=False)
    df.to_csv("data/f1_final_results.csv", index=False)
    print(f"✅ Saved {len(df)} rows to data/f1_final_results.parquet and .csv")
























































'''ff1.Cache.enable_cache('cache')  # Enable caching to speed up data loading

def build_dataset(years=[2022, 2023]):
    all_data = []

    for year in years:
        yearsched = ff1.get_event_schedule(year) # Get the event schedule for the year

        for _,event in yearsched.iterrows():
            race_name= event['EventName']
            round_no = event['RoundNumber']
            
            try:
                race= ff1.get_session(year, race_name, 'R')
                race.load()

                quali= ff1.get_session(year, race_name, 'Q')
                quali.load()

            except Exception as e:
                print(f"Skipping {race_name} {year} due to error: {e}")
                continue

            quali_results = quali.results.set_index('DriverNumber')
            
            for _,driver in race.results.iterrows():
                driver_number = driver.name
                driver_name = driver['Driver']
                team = driver['Team']
            
            
            
            if driver_number in quali_results.iterrows():
                q_row = quali_results.loc[driver_number]
                q_pos = driver['Position']
                q_time = driver['Q3'] if pd.notna(driver['Q3']) else (driver['Q2'] if pd.notna(driver['Q2']) else driver['Q1'])
                q_gap_to_pole = q_row["Time"] - quali_results.loc[1]["Time"] if q_row["Position"] != 1 else pd.Timedelta(0)
                
            all_data.append({
                    "Season": year,
                    "Round": round_no,
                    "RaceName": race_name,
                    "Driver": driver,
                    "Team": team,
                    "GridPos": driver["GridPosition"],
                    "FinishPos": driver["Position"],
                    "Points": driver["Points"],
                    "Status": driver["Status"],   # e.g. Finished, DNF
                    "QPos": q_pos,
                    "QTime": q_time,
                    "QGapToPole": q_gap_to_pole
                })

if __name__ == "__main__":
    df = build_dataset([2022, 2023])   # start small, test first
    print(df.head())
    df.to_csv("data/f1_dataset.csv", index=False)
    print("Dataset saved to data/f1_dataset.csv")            
'''
