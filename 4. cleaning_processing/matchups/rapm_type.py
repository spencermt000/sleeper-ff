import os
import pandas as pd
import numpy as np
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import MultiLabelBinarizer
import ast
from collections import Counter

def safe_literal_eval(val):
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError, TypeError):
        return []

raw_matchups = pd.read_csv('3. raw_data/master_matchups.csv')
raw_players = pd.read_csv('3. raw_data/players_sleeper.csv')
# Keep only relevant player info
players = raw_players[['player_id', 'position', 'full_name']].copy()

raw_matchups['starters'] = raw_matchups['starters'].fillna('[]').apply(safe_literal_eval)
raw_matchups['starters_points'] = raw_matchups['starters_points'].fillna('[]').apply(safe_literal_eval)

# --- Compute binary win/loss target ---
# For each matchup, assign 1 to the roster with the higher points, 0 otherwise
raw_matchups['win'] = (
    raw_matchups
    .groupby('matchup_id')['points']
    .transform(lambda pts: (pts == pts.max()).astype(int))
)

# --- Ridge Regression with Time Decay on Game-Level Data ---
# Build X and binary y (win/loss) for game-level Ridge
mlb = MultiLabelBinarizer(sparse_output=True)
X = mlb.fit_transform(raw_matchups['starters'])
y = raw_matchups['win'].values

# Compute time-decay sample weights based on week numbers
# half-life of 4 weeks
half_life_weeks = 4
lam = np.log(2) / half_life_weeks
max_week = raw_matchups['week'].max()
age_weeks = (max_week - raw_matchups['week']).clip(lower=0)
w = np.exp(-lam * age_weeks)

ridge = RidgeCV(alphas=np.logspace(-2, 3, 20), fit_intercept=False, cv=5)
# --- Filter out any rows with missing targets ---
mask = ~np.isnan(y)
X = X[mask]
y = y[mask]
w = w[mask]
ridge.fit(X, y, sample_weight=w)

effects = pd.Series(ridge.coef_, index=mlb.classes_).sort_values(ascending=False)
# --- Join player info to effects ---
effects_df = effects.rename_axis('player_id').reset_index(name='effect')
# Ensure player_id type matches
effects_df['player_id'] = effects_df['player_id'].astype(players['player_id'].dtype)
# Merge to get names and positions
effects_with_info = effects_df.merge(players, on='player_id', how='left')
# --- Compute sample size per player ---
counts = Counter()
for starters in raw_matchups['starters']:
    counts.update(starters)
# Turn into DataFrame for merging
sample_size_df = (
    pd.Series(counts, name='sample_size')
      .rename_axis('player_id')
      .reset_index()
)
# Ensure same dtype
sample_size_df['player_id'] = sample_size_df['player_id'].astype(players['player_id'].dtype)
# Merge sample size into effects_with_info
effects_with_info = effects_with_info.merge(sample_size_df, on='player_id', how='left')

# Filter to core offensive positions and display top 10
effects_with_info = effects_with_info[effects_with_info['position'].isin(["QB","WR","RB","TE"])]
print("Top 10 player effects on win probability:")
print(effects_with_info.head(10)[['player_id', 'full_name', 'position', 'effect', 'sample_size']])

print("Bottom 10 player effects on win probability:")
print(effects_with_info.tail(10).sort_values(by='effect', ascending=True)[['player_id', 'full_name', 'position', 'effect', 'sample_size']])

# Display top 10 players by sample size
top_10_sample_size = effects_with_info.sort_values(by='sample_size', ascending=False).head(10)
print("Top 10 players by sample size:")
print(top_10_sample_size[['player_id', 'full_name', 'position', 'effect', 'sample_size']])
# ----------------------------------------------------------

# 2. Explode into one row per starter
raw_matchups_long = (
    raw_matchups
    .explode(['starters', 'starters_points'])
    .rename(columns={
        'starters': 'starter_id',
        'starters_points': 'starter_points'
    })[
        ['league_id', 'week', 'roster_id', 'matchup_id', 'points', 'starter_id', 'starter_points']
    ]
)