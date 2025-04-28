import requests
import zipfile
import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

### Step 1: Download and extract WCA rankings data (if needed) ###
### data source: https://www.worldcubeassociation.org/export/results

WCA_RANKINGS_URL = "https://www.worldcubeassociation.org/export/results/WCA_export.tsv"
                    
WCA_FILE = "WCA_export_RanksSingle.tsv"
ZIP_FILE = "WCA_export.zip"

def download_wca_data():
    if os.path.exists(WCA_FILE):
        print(f"Using existing file: {WCA_FILE}")
        return
    print("Downloading WCA rankings data...")
    r = requests.get(WCA_RANKINGS_URL)
    with open(ZIP_FILE, "wb") as f:
        f.write(r.content)
    with zipfile.ZipFile(ZIP_FILE, 'r') as z:
        z.extract(WCA_FILE)
    os.remove(ZIP_FILE)
    print("WCA rankings data downloaded and extracted.")

def load_wca_data():
    return pd.read_csv(WCA_FILE, sep='\t')

### Step 2: Scrape registrations ###
COMPETITION_ID = "MidAtlanticChampionship2025"
REGISTRATIONS_URL = f"https://www.worldcubeassociation.org/competitions/{COMPETITION_ID}/registrations"

def get_registrations():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    try:
        driver.get(REGISTRATIONS_URL)
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        results = []
        for row in table.find_elements(By.TAG_NAME, "tr")[1:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells: continue
            link = cells[0].find_elements(By.TAG_NAME, "a")
            if link:
                name = link[0].text.strip()
                pid  = link[0].get_attribute("href").split("/")[-1]
            else:
                name = cells[0].text.strip()
                pid  = None
            results.append({"Name": name, "personId": pid})
        return results
    finally:
        driver.quit()

### Step 3: Match & pivot ###
def process_competitor_data(comps, wca_df):
    rows = []
    for c in comps:
        pid = c["personId"]; name = c["Name"]
        if not pid: continue
        pdata = wca_df[wca_df["personId"] == pid]
        if pdata.empty: continue
        pivot = pdata.pivot(index="personId", columns="eventId", values=["best","worldRank"])
        pivot.columns = [f"{ev}_{typ}" for typ,ev in pivot.columns]
        pivot.reset_index(inplace=True)
        pivot.insert(0, "Name", name)
        rows.append(pivot)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["Name","personId"])

### Step 4: Format best columns ###
def format_best_columns(df):
    bests = [c for c in df if c.endswith("_best")]
    df[bests] = df[bests].div(100).round(1)
    return df

### Step 5: Sort columns with Name, personId first ###
def sort_columns_custom(df):
    fixed = ["Name","personId"]
    others = sorted(c for c in df if c not in fixed)
    return df[fixed+others]

### Step 6: Sort rows by 333_worldRank ###
def sort_rows_by_333(df):
    if "333_worldRank" in df:
        return df.sort_values("333_worldRank", na_position="last")
    return df

### Step 7: Run pipeline & save main CSV ###
def main():
    t0 = time.time()
    download_wca_data()
    wca_df = load_wca_data()

    print("Scraping registrations…")
    comps = get_registrations()

    print("Processing rankings…")
    df = process_competitor_data(comps, wca_df)
    df = format_best_columns(df)
    df = sort_columns_custom(df)
    df = sort_rows_by_333(df)

    # Step 8: Build "WR Top 100" strings for each row
    wr_list = []
    for _, row in df.iterrows():
        entries = []
        for col in df.columns:
            if col.endswith("_worldRank") and pd.notna(row[col]) and row[col] <= 100:
                event = col.split("_")[0]
                rank  = int(row[col])
                entries.append(f"{event} (#{rank})")
        wr_list.append(", ".join(entries))

    # Insert as the 3rd column (index=2)
    df.insert(2, "WR Top 100", wr_list)

    # Save the combined file
    out_main = f"{COMPETITION_ID}_competitor_rankings.csv"
    df.to_csv(out_main, index=False)
    print(f"Saved combined rankings to {out_main}")

    print(f"Done in {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
