import os
import pandas as pd

# SET UP
RAW_DATA_DIR    = '3. raw_data'
MATCHUPS_DIR    = os.path.join(RAW_DATA_DIR, 'matchups')

# Read all CSV files in the MATCHUPS_DIR that start with "matchups_"
csv_files = [f for f in os.listdir(MATCHUPS_DIR) if f.startswith("matchups_") and f.endswith(".csv")]

# Combine them into one DataFrame
master_df = pd.concat([pd.read_csv(os.path.join(MATCHUPS_DIR, file)) for file in csv_files], ignore_index=True)