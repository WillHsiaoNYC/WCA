import requests
import zipfile
import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

### **Step 1: Download and Extract WCA Rankings Data (If Needed)** ###
### data source: https://www.worldcubeassociation.org/export/results

WCA_RANKINGS_URL = "https://www.worldcubeassociation.org/export/results/WCA_export.tsv"
WCA_FILE = "WCA_export_RanksSingle.tsv"
ZIP_FILE = "WCA_export.zip"

def download_wca_data():
    """Check if WCA rankings file exists. If not, download and extract it."""
    if os.path.exists(WCA_FILE):
        print(f"Using existing file: {WCA_FILE}")
        return  # Skip download if file exists

    print("Downloading WCA rankings data...")
    response = requests.get(WCA_RANKINGS_URL)
    with open(ZIP_FILE, "wb") as file:
        file.write(response.content)

    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extract(WCA_FILE)

    os.remove(ZIP_FILE)  # Delete zip file after extraction
    print("WCA rankings data downloaded and extracted.")

def load_wca_data():
    """Load WCA rankings TSV file into a DataFrame."""
    df = pd.read_csv(WCA_FILE, sep='\t')
    return df

### **Step 2: Web Scraping for WCA ID and Names** ###

# COMPETITION_ID = "TroyStory2023"
COMPETITION_ID = "TippingPointBloomsburg2025"
REGISTRATIONS_URL = f"https://www.worldcubeassociation.org/competitions/{COMPETITION_ID}/registrations"

def get_registrations():
    """Scrape WCA competition page for Names & WCA IDs (stored in memory only)."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run browser in the background
    driver = webdriver.Chrome(options=options)

    try:
        driver.get(REGISTRATIONS_URL)
        wait = WebDriverWait(driver, 10)
        table = wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

        results = []
        rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # Skip header row

        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue

            # Extract competitor Name & WCA ID (if available)
            name_link = cells[0].find_element(By.TAG_NAME, "a") if cells[0].find_elements(By.TAG_NAME, "a") else None
            name = name_link.text if name_link else cells[0].text.strip()
            wca_id = name_link.get_attribute("href").split("/")[-1] if name_link else None

            results.append({"Name": name, "personId": wca_id})  # Use 'personId' for consistency

        return results  # Keep in memory, do NOT save to disk

    finally:
        driver.quit()  # Close browser

### **Step 3: Match WCA IDs and Pivot Data** ###

def process_competitor_data(competitors, wca_df):
    """
    Matches competitors' WCA IDs with the WCA rankings dataset and pivots eventId data.
    """
    results = []
    for competitor in competitors:
        wca_id = competitor["personId"]
        name = competitor["Name"]

        if not wca_id:
            continue  # Skip competitors without a WCA ID

        # Filter WCA rankings data for this competitor
        person_data = wca_df[wca_df["personId"] == wca_id]

        if person_data.empty:
            continue  # Skip if no ranking data found

        # Pivot eventId to columns with 'best' and 'worldRank' as values
        person_pivoted = person_data.pivot(index="personId", columns="eventId", values=["best", "worldRank"])
        
        # Flatten multi-index columns
        person_pivoted.columns = [f"{col[1]}_{col[0]}" for col in person_pivoted.columns]

        # Reset index and add competitor name
        person_pivoted.reset_index(inplace=True)
        person_pivoted.insert(0, "Name", name)

        results.append(person_pivoted)

    # Combine all competitor data
    if results:
        final_df = pd.concat(results, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=["Name", "personId"])  # Empty DataFrame

    return final_df

### **Step 4: Format 'best' Columns (Divide by 100 & Round)** ###

def format_best_columns(df):
    """
    Modify 'best' columns by dividing by 100 and rounding to 1 decimal place.
    """
    best_cols = [col for col in df.columns if "best" in col]  # Identify 'best' columns
    df[best_cols] = df[best_cols].div(100).round(1)  # Divide by 100 and round
    return df

### **Step 5: Sort Columns Alphabetically with 'Name' and 'personId' First** ###

def sort_columns_custom(df):
    """
    Ensures 'Name' is first, 'personId' is second, and the rest sorted alphabetically.
    """
    fixed_cols = ["Name", "personId"]  # Columns to keep at the start
    other_cols = sorted([col for col in df.columns if col not in fixed_cols])
    return df[fixed_cols + other_cols]

### **Step 6: Sort Rows by '333_worldRank'** ###

def sort_rows_by_333_worldRank(df):
    """
    Sorts the DataFrame rows by the column '333_worldRank' (missing values at the end).
    """
    if "333_worldRank" in df.columns:
        df = df.sort_values(by="333_worldRank", na_position="last")
    return df

### **Step 7: Run the Full Pipeline and Save Results** ###

def main():
    """Main function to run the full pipeline."""
    start_time = time.time()

    # Step 1: Download & process WCA rankings dataset FIRST (skip if exists)
    download_wca_data()
    wca_df = load_wca_data()

    # Step 2: Scrape competitor data (stored in memory)
    print("Scraping competitor registrations...")
    competitors = get_registrations()

    # Step 3: Match WCA IDs and pivot event data
    print("Processing rankings data...")
    final_df = process_competitor_data(competitors, wca_df)

    # Step 4: Format 'best' columns
    final_df = format_best_columns(final_df)

    # Step 5: Sort columns with 'Name' first, 'personId' second, rest alphabetically
    final_df = sort_columns_custom(final_df)

    # Step 6: Sort rows by '333_worldRank'
    final_df = sort_rows_by_333_worldRank(final_df)

    # Step 7: Save final output
    final_df.to_csv(f"{COMPETITION_ID} competitor_rankings.csv", index=False)
    print(f"Final rankings data saved to {COMPETITION_ID} competitor_rankings.csv")

    # Print execution time
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
