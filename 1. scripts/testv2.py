import os
import time
import requests
import pandas as pd
from collections import deque

# ─── CONFIG ────────────────────────────────────────────────────────────────
PRE_SAVED_CSV        = "2. league_ids/master_league_ids.csv"

# master CSV paths
MASTER_INFO_CSV      = "3. raw_data/master_info.csv"
MASTER_DRAFTS_CSV    = "3. raw_data/master_drafts.csv"
MASTER_MATCHUPS_CSV  = "3. raw_data/master_matchups.csv"
MATCHUPS_DONE_CSV    = "3. raw_data/already_done.csv"

# Filter criteria
SEASON          = 2024
WEEKS           = list(range(1, 18))
LEAGUE_FILTERS  = {
    "total_teams": [10, 12],
    "slots_bn":    list(range(5,13))
}

# Spider params
MAX_DEPTH       = 5
REQUEST_PAUSE   = 5  # seconds between API calls

# ─── SETUP ──────────────────────────────────────────────────────────────────
# ensure directories exist
for d in ["3. raw_data", "3. raw_data/matchups"]:
    os.makedirs(d, exist_ok=True)

# load or initialize master DataFrames
def load_master(path, cols):
    if os.path.exists(path):
        return pd.read_csv(path)
    else:
        return pd.DataFrame(columns=cols)

master_info_cols      = ["league_id"] + list(pd.DataFrame([{}]).columns)
master_drafts_cols    = ["league_id","draft_id","draft_slot","pick_no","is_keeper","player_id","position","picked_by"]
master_matchups_cols  = None  # will infer later

master_info      = load_master(MASTER_INFO_CSV, master_info_cols)
master_drafts    = load_master(MASTER_DRAFTS_CSV, master_drafts_cols)
master_matchups  = load_master(MASTER_MATCHUPS_CSV, [])  # empty; will set on first append

# trackers
visited_leagues  = set()
visited_users    = set()
out_of_filter    = set()
already_done     = set(
    pd.read_csv(MATCHUPS_DONE_CSV)["league_id"].astype(str).tolist()
) if os.path.exists(MATCHUPS_DONE_CSV) else set()

# initialize queue
league_queue = deque(pd.read_csv(PRE_SAVED_CSV)["league_id"].astype(str).tolist())
user_queue   = deque()

attempts, successes = 0, 0

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_get_json(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] GET {url} → {e}")
        return None

def explore_league_for_users(league_id):
    data = safe_get_json(f"https://api.sleeper.app/v1/league/{league_id}/rosters")
    time.sleep(REQUEST_PAUSE)
    return [r["owner_id"] for r in data or [] if r.get("owner_id")]

def get_user_leagues(user_id):
    data = safe_get_json(
        f"https://api.sleeper.app/v1/user/{user_id}/leagues/nfl/{SEASON}"
    )
    time.sleep(REQUEST_PAUSE)
    return [l["league_id"] for l in data or []]

def fetch_and_append_league_data(league_id):
    """Fetch league & draft settings, apply filters, append to masters. Return True if passes."""
    li = safe_get_json(f"https://api.sleeper.app/v1/league/{league_id}")
    time.sleep(REQUEST_PAUSE)
    draft_id = li.get("draft_id") if li else None
    if not li or not draft_id:
        return False

    ds = safe_get_json(f"https://api.sleeper.app/v1/draft/{draft_id}")
    time.sleep(REQUEST_PAUSE)
    if not ds:
        return False

    teams = int(li["settings"].get("num_teams", 0))
    bn    = ds["settings"].get("slots_bn")
    if teams not in LEAGUE_FILTERS["total_teams"] or bn not in LEAGUE_FILTERS["slots_bn"]:
        out_of_filter.add(league_id)
        return False

    # --- INFO ---
    scoring = pd.DataFrame([li.get("scoring_settings", {})])
    roster  = pd.DataFrame([{"roster_positions": li.get("roster_positions", [])}])
    settings= pd.DataFrame([li.get("settings", {})])
    info_df = pd.concat([scoring, roster, settings], axis=1)
    info_df["league_id"] = league_id

    # avoid dupes
    if league_id not in master_info["league_id"].astype(str).values:
        master_info.loc[len(master_info)] = info_df.iloc[0]
        master_info.to_csv(MASTER_INFO_CSV, index=False)

    # --- DRAFT ---
    picks = ds.get("picks") or safe_get_json(f"https://api.sleeper.app/v1/draft/{draft_id}/picks")
    rows = []
    for p in picks or []:
        m = p.get("metadata", {})
        rows.append({
            "league_id":  league_id,
            "draft_id":   draft_id,
            "draft_slot": p.get("draft_slot"),
            "pick_no":    p.get("pick_no"),
            "is_keeper":  p.get("is_keeper"),
            "player_id":  m.get("player_id"),
            "position":   m.get("position"),
            "picked_by":  p.get("picked_by")
        })
    draft_df = pd.DataFrame(rows)
    new_picks = draft_df[~draft_df[["league_id","pick_no"]]
                        .isin(master_drafts[["league_id","pick_no"]]).all(axis=1)]
    if not new_picks.empty:
        master_drafts = pd.concat([master_drafts, new_picks], ignore_index=True)
        master_drafts.to_csv(MASTER_DRAFTS_CSV, index=False)

    return True

def fetch_and_append_matchups(league_id):
    all_rows = []
    for wk in WEEKS:
        data = safe_get_json(
            f"https://api.sleeper.app/v1/league/{league_id}/matchups/{wk}",
            params={"season": SEASON}
        ) or []
        for rec in data:
            rec["league_id"] = league_id
            rec["week"]      = wk
            all_rows.append(rec)
        time.sleep(REQUEST_PAUSE/2)

    df = pd.DataFrame(all_rows)
    global master_matchups
    if master_matchups.empty:
        master_matchups = df
    else:
        master_matchups = pd.concat([master_matchups, df], ignore_index=True)

    master_matchups.drop_duplicates(
        subset=["league_id","week","roster_id"], inplace=True
    )
    master_matchups.to_csv(MASTER_MATCHUPS_CSV, index=False)

    # update done file
    already_done.add(league_id)
    pd.DataFrame({"league_id": sorted(already_done)})\
        .to_csv(MATCHUPS_DONE_CSV, index=False)

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
depth = 0
while depth <= MAX_DEPTH:
    if not league_queue and depth < MAX_DEPTH:
        depth += 1
        for uid in list(user_queue):
            if uid not in visited_users:
                visited_users.add(uid)
                for new_lid in get_user_leagues(uid):
                    if new_lid not in visited_leagues:
                        league_queue.append(new_lid)
        continue

    if not league_queue:
        break

    lid = league_queue.popleft()
    attempts += 1
    if lid in visited_leagues:
        continue
    visited_leagues.add(lid)

    # spider rosters
    for owner in explore_league_for_users(lid):
        if owner not in visited_users:
            user_queue.append(owner)

    # fetch & filter
    passed = fetch_and_append_league_data(lid)
    if passed and lid not in already_done:
        successes += 1
        fetch_and_append_matchups(lid)
        status = "✔"
    else:
        status = "✖"

    print(f"[{successes}/{attempts}] {status} League {lid}")

# ─── WRAP UP ─────────────────────────────────────────────────────────────────
pd.DataFrame({"league_id": sorted(out_of_filter)})\
    .to_csv(os.path.join(os.path.dirname(PRE_SAVED_CSV), "out_of_filter.csv"), index=False)

print("\n🎉 All done.")
print(f"🏁 Completed: {successes}/{attempts} leagues scraped successfully.")