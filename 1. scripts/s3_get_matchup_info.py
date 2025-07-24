import requests, time, os
import pandas as pd
import ast

# ─── SETUP ────────────────────────────────────────────────────────────
SLEEP_SEC       = 5
LEAGUE_IDS_DIR  = '2. league_ids'
RAW_DATA_DIR    = '3. raw_data'
MATCHUPS_DIR    = os.path.join(RAW_DATA_DIR, 'matchups')
# ──────────────────────────────────────────────────────────────────────

# load your inputs
master_league_ids = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, 'master_league_ids.csv'))
out_of_filter     = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, 'out_of_filter.csv'))
already_done_df   = pd.read_csv(os.path.join(MATCHUPS_DIR, 'already_done.csv'))

# compute which leagues to process
looping_league_ids = master_league_ids[
    ~master_league_ids['league_id'].isin(out_of_filter['league_id']) &
    ~master_league_ids['league_id'].isin(already_done_df['league_id'])
]

season = 2024
weeks  = list(range(1, 18))  # weeks 1–17

# ─── PROCESS ──────────────────────────────────────────────────────────
for league_id in looping_league_ids['league_id']:
    print(f"▶ Processing league {league_id}")
    all_matchups = []

    # fetch each week
    for week in weeks:
        url    = f"https://api.sleeper.app/v1/league/{league_id}/matchups/{week}"
        params = {"season": season}
        resp   = requests.get(url, params=params)
        resp.raise_for_status()
        data   = resp.json() or []

        # annotate & collect
        for entry in data:
            entry["week"] = week
            all_matchups.append(entry)

        time.sleep(0.5)  # per‐week backoff

    # write out this league's raw matchups
    df       = pd.DataFrame(all_matchups)
    out_file = os.path.join(MATCHUPS_DIR, f"matchups_{league_id}.csv")
    df.to_csv(out_file, index=False)
    print(f"  • Wrote {len(df)} rows to {out_file}")

    # mark this league as done
    already_done_df = pd.concat([
        already_done_df,
        pd.DataFrame({'league_id': [league_id]})
    ], ignore_index=True).drop_duplicates()
    already_done_df.to_csv(os.path.join(MATCHUPS_DIR, 'already_done.csv'), index=False)

    time.sleep(SLEEP_SEC)
# ──────────────────────────────────────────────────────────────────────