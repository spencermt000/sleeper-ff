import os
import time
import requests
import pandas as pd
from collections import deque

# ─── CONFIG ────────────────────────────────────────────────────────────────
# Presaved league IDs (your “99k”)
PRE_SAVED_CSV   = "2. league_ids/master_league_ids.csv"

# Where to write per‐league settings & draft info
INFO_DIR        = "3. raw_data/info"
DRAFTS_DIR      = "3. raw_data/drafts"

# Where to write per‐league matchup files & track done
MATCHUPS_DIR    = "3. raw_data/matchups"
MATCHUPS_DONE   = os.path.join(MATCHUPS_DIR, "already_done.csv")

# Filter criteria
SEASON          = 2024
WEEKS           = list(range(1, 18))
LEAGUE_FILTERS  = {
    "total_teams": [10, 12],
    "slots_bn":    [5, 6, 7, 8, 9, 10, 11, 12]
}

# Spider params
MAX_DEPTH       = 5
REQUEST_PAUSE   = 5  # seconds between API calls

# ─── SETUP ──────────────────────────────────────────────────────────────────
os.makedirs(INFO_DIR,    exist_ok=True)
os.makedirs(DRAFTS_DIR,  exist_ok=True)
os.makedirs(MATCHUPS_DIR, exist_ok=True)

# load trackers
visited_leagues = set()
visited_users   = set()
out_of_filter   = set()
already_done    = set(
    pd.read_csv(MATCHUPS_DONE)["league_id"].astype(str).tolist()
) if os.path.exists(MATCHUPS_DONE) else set()

# initialize queues
league_queue = deque(pd.read_csv(PRE_SAVED_CSV)["league_id"].astype(str).tolist())
user_queue   = deque()

# counters
attempts = 0
successes = 0

# ─── HELPERS ──────────────────────────────────────────────────────────────────
def safe_get_json(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] GET {url} → {e}")
        return None

def explore_league_for_users(league_id):
    """Call roster endpoint to harvest owner_ids for spider expansion."""
    data = safe_get_json(f"https://api.sleeper.app/v1/league/{league_id}/rosters")
    time.sleep(REQUEST_PAUSE)
    if not data: 
        return []
    return [r.get("owner_id") for r in data if r.get("owner_id")]

def get_user_leagues(user_id):
    data = safe_get_json(
        f"https://api.sleeper.app/v1/user/{user_id}/leagues/nfl/{SEASON}"
    )
    time.sleep(REQUEST_PAUSE)
    return [l["league_id"] for l in data] if data else []

def fetch_and_save_league_data(league_id):
    """Fetch league & draft settings, apply filters, save CSVs. Returns True if passes filter."""
    # league info
    li = safe_get_json(f"https://api.sleeper.app/v1/league/{league_id}")
    time.sleep(REQUEST_PAUSE)
    if not li or not (draft_id := li.get("draft_id")):
        return False

    ds = safe_get_json(f"https://api.sleeper.app/v1/draft/{draft_id}")
    time.sleep(REQUEST_PAUSE)
    if not ds:
        return False

    # check filters
    teams = int(li.get("settings", {}).get("num_teams", 0))
    bn    = ds.get("settings", {}).get("slots_bn")
    if teams not in LEAGUE_FILTERS["total_teams"] or bn not in LEAGUE_FILTERS["slots_bn"]:
        out_of_filter.add(league_id)
        return False

    # build & save league CSV
    scoring = pd.DataFrame([li.get("scoring_settings", {})])
    roster  = pd.DataFrame([{"roster_positions": li.get("roster_positions", [])}])
    settings= pd.DataFrame([li.get("settings", {})])
    for df in (scoring, roster, settings):
        df["league_id"] = league_id
    pd.concat([scoring, roster, settings], axis=1).to_csv(
        os.path.join(INFO_DIR, f"{league_id}.csv"),
        index=False
    )

    # build & save draft CSV
    picks = ds.get("picks", []) or safe_get_json(
        f"https://api.sleeper.app/v1/draft/{draft_id}/picks"
    )
    rows = []
    for p in picks or []:
        m = p.get("metadata", {})
        rows.append({
            "league_id":   league_id,
            "draft_id":    draft_id,
            "draft_slot":  p.get("draft_slot"),
            "pick_no":     p.get("pick_no"),
            "is_keeper":   p.get("is_keeper"),
            "player_id":   m.get("player_id"),
            "position":    m.get("position"),
            "picked_by":   p.get("picked_by")
        })
    pd.DataFrame(rows).to_csv(
        os.path.join(DRAFTS_DIR, f"{league_id}.csv"),
        index=False
    )

    return True

def fetch_and_save_matchups(league_id):
    all_rows = []
    for wk in WEEKS:
        url = f"https://api.sleeper.app/v1/league/{league_id}/matchups/{wk}"
        params = {"season": SEASON}
        data = requests.get(url, params=params).json() or []
        for rec in data:
            rec["league_id"] = league_id
            rec["week"]      = wk
            all_rows.append(rec)
        time.sleep(REQUEST_PAUSE / 2)
    df = pd.DataFrame(all_rows)
    df.to_csv(os.path.join(MATCHUPS_DIR, f"matchups_{league_id}.csv"), index=False)

    # mark done
    already_done.add(league_id)
    pd.DataFrame({"league_id": sorted(already_done)}).to_csv(
        MATCHUPS_DONE, index=False
    )

# ─── MAIN LOOP ────────────────────────────────────────────────────────────────
depth = 0
while depth <= MAX_DEPTH:
    # When presaved queue empties, begin spider expansion
    if not league_queue and depth < MAX_DEPTH:
        depth += 1
        print(f"\n🌐 Spider depth {depth}/{MAX_DEPTH} | users to expand: {len(user_queue)}")
        next_users = list(user_queue)
        user_queue.clear()
        for uid in next_users:
            if uid in visited_users:
                continue
            visited_users.add(uid)
            for new_lid in get_user_leagues(uid):
                if new_lid not in visited_leagues:
                    league_queue.append(str(new_lid))

        continue

    if not league_queue:
        break

    lid = league_queue.popleft()
    attempts += 1
    if lid in visited_leagues:
        continue
    visited_leagues.add(lid)

    # step 1: spider rosters → user_queue
    for owner in explore_league_for_users(lid):
        if owner not in visited_users:
            user_queue.append(owner)

    # step 2: fetch & filter league/draft
    passed = fetch_and_save_league_data(lid)

    # step 3: if passed & not already done, fetch matchups
    if not passed:
        print(f"✖ League {lid} skipped (filter or already done)")
    elif lid in already_done:
        print(f"✖ League {lid} skipped (filter or already done)")
    else:
        successes += 1
        print(f"✔ League {lid} passed filters; fetching matchups…")
        fetch_and_save_matchups(lid)

# ─── WRAP UP ─────────────────────────────────────────────────────────────────
# write out out_of_filter list
pd.DataFrame({"league_id": sorted(out_of_filter)}).to_csv(
    os.path.join(os.path.dirname(PRE_SAVED_CSV), "out_of_filter.csv"),
    index=False
)
print("\n🎉 All done.")
print(f"\n🏁 Completed: {successes}/{attempts} leagues scraped successfully.")