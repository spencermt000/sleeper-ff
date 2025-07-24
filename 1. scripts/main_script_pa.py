#!/usr/bin/env python3
import os
import time
import requests
import pandas as pd
from collections import deque

# ─── PROJECT BASE ────────────────────────────────────────────────────────────
# assume this script lives in ~/yomp/1. scripts/
BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# e.g. BASE_DIR == "/home/yourusername/yomp"

# ─── PATHS ───────────────────────────────────────────────────────────────────
PRE_SAVED_CSV       = os.path.join(BASE_DIR, "2. league_ids", "master_league_ids.csv")

MASTER_INFO_CSV     = os.path.join(BASE_DIR, "3. raw_data", "master_info.csv")
MASTER_DRAFTS_CSV   = os.path.join(BASE_DIR, "3. raw_data", "master_drafts.csv")
MASTER_MATCHUPS_CSV = os.path.join(BASE_DIR, "3. raw_data", "master_matchups.csv")

ALREADY_DONE_CSV    = os.path.join(BASE_DIR, "3. raw_data", "already_done.csv")
OUT_OF_FILTER_CSV   = os.path.join(BASE_DIR, "2. league_ids", "out_of_filter.csv")

# ─── FILTER CRITERIA ──────────────────────────────────────────────────────────
SEASON         = 2024
WEEKS          = list(range(1, 18))
LEAGUE_FILTERS = {
    "total_teams": [10, 12],
    "slots_bn":    list(range(5, 13))
}

# ─── SPIDER PARAMS ────────────────────────────────────────────────────────────
MAX_DEPTH      = 5
REQUEST_PAUSE  = 5   # seconds between API calls

# ─── PREPARE DIRECTORIES ──────────────────────────────────────────────────────
for d in [
    os.path.join(BASE_DIR, "3. raw_data"),
    os.path.join(BASE_DIR, "3. raw_data", "matchups")
]:
    os.makedirs(d, exist_ok=True)

# ─── LOAD OR INITIALIZE MASTER DATAFRAMES ────────────────────────────────────
def load_master(path, cols):
    return pd.read_csv(path) if os.path.exists(path) else pd.DataFrame(columns=cols)

master_info_cols    = ["league_id"]  # plus whatever scoring/settings columns you expect
master_drafts_cols  = ["league_id","draft_id","draft_slot","pick_no","is_keeper","player_id","position","picked_by"]
master_matchups_cols= []  # will infer on first write

master_info     = load_master(MASTER_INFO_CSV,    master_info_cols)
master_drafts   = load_master(MASTER_DRAFTS_CSV,  master_drafts_cols)
master_matchups = load_master(MASTER_MATCHUPS_CSV, master_matchups_cols)

# Ensure league_id columns are string type
if "league_id" in master_info.columns:
    master_info["league_id"] = master_info["league_id"].astype(str)
if "league_id" in master_drafts.columns:
    master_drafts["league_id"] = master_drafts["league_id"].astype(str)
if "league_id" in master_matchups.columns:
    master_matchups["league_id"] = master_matchups["league_id"].astype(str)

# ─── TRACKERS ─────────────────────────────────────────────────────────────────
visited_leagues = set()
visited_users   = set()
out_of_filter   = set()

already_done = (
    set(pd.read_csv(ALREADY_DONE_CSV)["league_id"].astype(str))
    if os.path.exists(ALREADY_DONE_CSV)
    else set()
)

# ─── QUEUES & COUNTERS ─────────────────────────────────────────────────────────
league_queue = deque(pd.read_csv(PRE_SAVED_CSV)["league_id"].astype(str))
user_queue   = deque()
attempts = successes = 0

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
    data = safe_get_json(f"https://api.sleeper.app/v1/user/{user_id}/leagues/nfl/{SEASON}")
    time.sleep(REQUEST_PAUSE)
    return [l["league_id"] for l in data or []]

def fetch_and_append_league_data(league_id):
    global master_info, master_drafts

    # ── fetch league + draft JSON
    li = safe_get_json(f"https://api.sleeper.app/v1/league/{league_id}")
    time.sleep(REQUEST_PAUSE)
    if not li or not (draft_id := li.get("draft_id")):
        return False

    ds = safe_get_json(f"https://api.sleeper.app/v1/draft/{draft_id}")
    time.sleep(REQUEST_PAUSE)
    if not ds:
        return False

    # ── apply filters
    teams = int(li["settings"].get("num_teams", 0))
    bn    = ds["settings"].get("slots_bn")
    if teams not in LEAGUE_FILTERS["total_teams"] or bn not in LEAGUE_FILTERS["slots_bn"]:
        out_of_filter.add(league_id)
        return False

    # ── INFO
    info_df = pd.concat([
        pd.DataFrame([li.get("scoring_settings", {})]),
        pd.DataFrame([{"roster_positions": li.get("roster_positions", [])}]),
        pd.DataFrame([li.get("settings", {})]),
    ], axis=1)
    info_df["league_id"] = league_id

    if league_id not in master_info["league_id"].astype(str).values:
        master_info = pd.concat([master_info, info_df], ignore_index=True)
        master_info.to_csv(MASTER_INFO_CSV, index=False)

    # ── DRAFT
    raw_picks = ds.get("picks") or safe_get_json(f"https://api.sleeper.app/v1/draft/{draft_id}/picks")
    picks = []
    for p in raw_picks or []:
        m = p.get("metadata", {})
        picks.append({
            "league_id":  league_id,
            "draft_id":   draft_id,
            "draft_slot": p["draft_slot"],
            "pick_no":    p["pick_no"],
            "is_keeper":  p["is_keeper"],
            "player_id":  m.get("player_id"),
            "position":   m.get("position"),
            "picked_by":  p.get("picked_by")
        })
    draft_df = pd.DataFrame(picks)
    draft_df["league_id"] = draft_df["league_id"].astype(str)

    # append new rows and dedupe by key
    combined = pd.concat([master_drafts, draft_df], ignore_index=True)
    combined.drop_duplicates(subset=["league_id","pick_no"], keep="first", inplace=True)
    master_drafts = combined
    master_drafts.to_csv(MASTER_DRAFTS_CSV, index=False)

    return True

def fetch_and_append_matchups(league_id):
    global master_matchups, already_done

    rows = []
    for wk in WEEKS:
        data = safe_get_json(
            f"https://api.sleeper.app/v1/league/{league_id}/matchups/{wk}",
            params={"season": SEASON}
        ) or []
        for rec in data:
            rec["league_id"], rec["week"] = league_id, wk
            rows.append(rec)
        time.sleep(REQUEST_PAUSE/2)

    df = pd.DataFrame(rows)
    master_matchups = pd.concat([master_matchups, df], ignore_index=True)
    master_matchups.drop_duplicates(subset=["league_id","week","roster_id"], inplace=True)
    master_matchups.to_csv(MASTER_MATCHUPS_CSV, index=False)

    already_done.add(league_id)
    pd.DataFrame({"league_id": sorted(already_done)}).to_csv(ALREADY_DONE_CSV, index=False)

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
  .to_csv(OUT_OF_FILTER_CSV, index=False)

print(f"\n🎉 Done: {successes}/{attempts} leagues passed filters.")