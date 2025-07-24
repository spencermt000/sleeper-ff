# LIBRARIES
import os
import requests
import pandas as pd
import time
import glob
from datetime import datetime

# SET VARS
SLEEP_SEC = 5
RAW_DATA_DIR = '3. raw_data'

# load previously recorded already-done league IDs
existing_done_path = os.path.join(RAW_DATA_DIR, 'info', 'already_done.csv')
if os.path.exists(existing_done_path):
    existing_done = pd.read_csv(existing_done_path)['league_id'].tolist()
else:
    existing_done = []
# collect newly scraped league IDs
newly_done = []

# LEAGUE FILTERING
league_filters = {
    "total_teams": [10, 12],
    "slots_bn": [5, 6, 7, 8, 9, 10, 11, 12]
}

# CREATE DIRECTORIES
LEAGUES_DIR = os.path.join(RAW_DATA_DIR, 'info')
DRAFTS_DIR  = os.path.join(RAW_DATA_DIR, 'drafts')

# LOAD ALL IDS
LEAGUE_IDS_DIR = '2. league_ids'
paths = {
    'df1': 'crawled_leagues.csv',
    'df2': 'crawled_leagues2.csv',
    'df3': 'crawled_leagues3.csv',
    'df4': 'crawled_leagues4.csv',
    'df5': 'old_leagues.csv',
}
df1 = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, paths['df1']))
df2 = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, paths['df2']))
df3 = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, paths['df3']))
df4 = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, paths['df4']))
df5 = (
    pd.read_csv(os.path.join(LEAGUE_IDS_DIR, paths['df5']))
      .loc[
          lambda d: d['total_rosters'].isin(league_filters['total_teams']) 
                   & (d['season'] == 2024),
          ['league_id']
      ]
)
master_league_ids = pd.concat([df1, df2, df3, df4, df5], ignore_index=True)
master_league_ids = master_league_ids.drop_duplicates(subset='league_id')

out_of_filter = pd.read_csv(os.path.join(LEAGUE_IDS_DIR, 'out_of_filter.csv'))
master_league_ids = master_league_ids[
    ~master_league_ids['league_id'].isin(out_of_filter['league_id'])
]

output_path = os.path.join(LEAGUE_IDS_DIR, 'master_league_ids.csv')
master_league_ids.to_csv(output_path, index=False)
print(f"Saved master_league_ids.csv to: {output_path}")

# MERGE PREVIOUSLY SCRAPED
INFO_DIR = os.path.join(RAW_DATA_DIR, 'info')
info_csv = glob.glob(os.path.join(INFO_DIR, "*.csv"))
info_dfs = [pd.read_csv(f) for f in info_csv]
master_info_df = pd.concat(info_dfs, ignore_index=True)

# FILTER OUT PREVIOUSLY SCRAPED
already_done_df = pd.read_csv(os.path.join(RAW_DATA_DIR, 'info', 'already_done.csv'))
info_league_ids = master_info_df['league_id']
already_done_df = pd.concat(
    [already_done_df, info_league_ids.to_frame(name='league_id')],
    ignore_index=True).drop_duplicates(subset='league_id')
loop_league_ids = master_league_ids[
    ~master_league_ids['league_id'].isin(already_done_df['league_id'])]

# DEF FUNCTIONS
def get_json(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"[ERROR] Failed request for {url}: {e}")
    return {}

def get_draft_settings(draft_id):
    return get_json(f"https://api.sleeper.app/v1/draft/{draft_id}")

def get_draft_picks(draft_id):
    return get_json(f"https://api.sleeper.app/v1/draft/{draft_id}/picks")

def get_league_info(league_id):
    return get_json(f"https://api.sleeper.app/v1/league/{league_id}")

# MAIN LOOP
out_of_filter = []

for i, league_id in enumerate(loop_league_ids['league_id'], start=1):
    print(f"▶ [{i}/{len(loop_league_ids)}] Processing league {league_id}")
    time.sleep(SLEEP_SEC)

    # fetch league & draft JSON
    li = get_league_info(league_id)
    if not li:
        print(f"⚠️  Empty league info for {league_id}")
        continue

    draft_id = li.get('draft_id')
    if not draft_id:
        print(f"⚠️  No draft_id for league {league_id}")
        continue

    ds = get_draft_settings(draft_id)
    if not ds:
        print(f"⚠️  No draft settings for league {league_id}")
        continue

    # apply your filters
    total_teams = int(li.get('settings', {}).get('num_teams', 0))
    slots_bn    = ds.get('settings', {}).get('slots_bn')
    if total_teams not in league_filters["total_teams"] or slots_bn not in league_filters["slots_bn"]:
        out_of_filter.append(league_id)
        print(f"⏩ Out-of-filter: teams={total_teams}, slots_bn={slots_bn}")
        continue

    # build league_df
    scoring = pd.DataFrame([li.get('scoring_settings', {})])
    roster  = pd.DataFrame([{'roster_positions': li.get('roster_positions', [])}])
    settings= pd.DataFrame([li.get('settings', {})])
    for df in (scoring, roster, settings):
        df['league_id'] = league_id
    league_df = scoring.merge(roster, on='league_id')\
                       .merge(settings, on='league_id')

    # build draft_df
    picks = get_draft_picks(draft_id) or []
    rows = []
    for p in picks:
        m = p.get('metadata', {})
        rows.append({
            'league_id':   league_id,
            'draft_id':    draft_id,
            'draft_slot':  p.get('draft_slot'),
            'pick_no':     p.get('pick_no'),
            'is_keeper':   p.get('is_keeper'),
            'player_first':m.get('first_name'),
            'player_last': m.get('last_name'),
            'player_id':   m.get('player_id'),
            'position':    m.get('position'),
            'roster_id':   p.get('draft_slot'),
            'picked_by':   p.get('picked_by'),
        })
    picks_df = pd.DataFrame(rows)

    meta = {
        'league_id':     league_id,
        'draft_id':      draft_id,
        'draft_order':   ds.get('draft_order'),
        'scoring_type':  ds.get('metadata', {}).get('scoring_type'),
        'season':        ds.get('season'),
        'type':          ds.get('type'),
        'status':        ds.get('status'),
        'rounds':        ds.get('settings', {}).get('rounds'),
        **{f"slots_{k}": ds.get('settings', {}).get(f"slots_{k}") for k in ['qb','rb','wr','te','flex','super_flex','bn']}
    }
    settings_df = pd.DataFrame([meta])
    draft_df    = settings_df.merge(picks_df, on='draft_id', how='outer')

    # save
    league_df.to_csv(os.path.join(LEAGUES_DIR, f"{league_id}.csv"), index=False)
    draft_df.to_csv (os.path.join(DRAFTS_DIR,  f"{league_id}.csv"), index=False)
    newly_done.append(league_id)

# ─── SAVE LEAGUES OUTSIDE FILTER ───────────────────────────────────────────────
pd.DataFrame({'league_id': out_of_filter})\
  .to_csv(os.path.join(LEAGUE_IDS_DIR, 'out_of_filter.csv'), index=False)

print(f"Saved {len(out_of_filter)} out-of-filter IDs to out_of_filter.csv")

# combine previous and newly scraped IDs and save
all_done = set(existing_done) | set(newly_done)
pd.DataFrame({'league_id': sorted(all_done)})\
    .to_csv(existing_done_path, index=False)
print(f"Updated already_done.csv with {len(newly_done)} new entries; total now {len(all_done)}")


