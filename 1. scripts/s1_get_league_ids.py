import requests, time, os
import pandas as pd

# ─── CONFIG ─────────────────────────────────────────
MASTER_CSV = "crawled_leagues2.csv"
OUTPUT_CSV = "crawled_leagues3.csv"
SEASON = 2024
MAX_DEPTH = 5
SLEEP_TIME = 2


visited_leagues = set()
visited_users = set()
user_queue = []

# Load seeds from input CSV
def load_seeds():
    df = pd.read_csv(MASTER_CSV)
    seeds = df["league_id"].dropna().unique().tolist()
    print(f"🧪 Loaded {len(seeds)} seed leagues")
    return seeds

# Robust request with error handling
def safe_request(url):
    try:
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"[ERROR] {url} → {e}")
        return None

# Explore a single league and get its owners
def explore_league(league_id, df):
    if league_id in visited_leagues:
        return [], df
    print(f"▶ Exploring league: {league_id}")
    visited_leagues.add(league_id)

    # Append new row and save CSV
    df = pd.concat([df, pd.DataFrame([{"league_id": league_id}])], ignore_index=True)
    df.to_csv(OUTPUT_CSV, index=False)

    data = safe_request(f"https://api.sleeper.app/v1/league/{league_id}/rosters")
    time.sleep(SLEEP_TIME)
    if not data:
        return [], df
    return [r.get("owner_id") for r in data if r.get("owner_id")], df

# Get all leagues for a user
def get_user_leagues(user_id):
    data = safe_request(f"https://api.sleeper.app/v1/user/{user_id}/leagues/nfl/{SEASON}")
    time.sleep(SLEEP_TIME)
    return [l["league_id"] for l in data] if data else []

# Main crawler
def spider():
    seeds = load_seeds()
    print(f"🔁 Starting spider with {len(seeds)} seeds")

    # Initialize or load the DataFrame
    if os.path.exists(OUTPUT_CSV):
        df = pd.read_csv(OUTPUT_CSV)
    else:
        df = pd.DataFrame(columns=["league_id"])

    # Start from seeds
    for lid in seeds:
        uids, df = explore_league(lid, df)
        for uid in uids:
            if uid not in visited_users:
                visited_users.add(uid)
                user_queue.append(uid)

    # Depth-based crawl
    for depth in range(1, MAX_DEPTH + 1):
        print(f"\n🌐 Depth {depth}/{MAX_DEPTH} | Users in queue: {len(user_queue)}")
        next_queue = []
        for uid in user_queue:
            for lid in get_user_leagues(uid):
                if lid not in visited_leagues:
                    new_uids, df = explore_league(lid, df)
                    for new_uid in new_uids:
                        if new_uid not in visited_users:
                            visited_users.add(new_uid)
                            next_queue.append(new_uid)
        if not next_queue:
            print("✅ No new users found—stopping early.")
            break
        user_queue[:] = next_queue

if __name__ == "__main__":
    try:
        spider()
        print("🎉 Spidering complete!")
    except KeyboardInterrupt:
        print("⚠️ Interrupted — progress saved live to CSV.")