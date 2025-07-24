# FANTASY FOOTBALL

## SCRIPT STRUCTURE
### main.py
- load all the league_ids. essentially run the merge_league_ids.py script
- load all the league_ids that were already done
- go through each league_id and extract the relevant information
    - filter out the ids that don't fit the criteria into bad_league_ids
    - get 

## IDEAS
### Price Elasticity
#### PE of Supply
- number of TEs available to draft
- amount of projected points from TEs available to draft
- compare backup picks to starter picks
- compare PES across different formats (TE premium, 2QB, dynasty, etc.)

#### PE of Demand
- number of teams without a starting TE
- compare PED across different formats (TE premium, 2QB, dynasty, etc.)
- compare PED across positions and picks

### Advanced Stats
- Value Over Replacement
- Win Shares
- Wins Above Replacement
- *DARKO-like Performance Rating*
    - maybe use each starting player as a player on the court and the result of the matchup as the points per possession
    - can add a time decay aspect as well

### Drafting
- idea 1: WAR, WS, VORP by pick
    - essentially making a draft value chart for fantasy football positions
- idea 2: draft startegy by sequence of positions taken 
    - accounting for quality of players/positions available.
-

### Fantasy Calc Value Data
- idea 1: can use it to evalute drafting success, PES/PED, etc. 
- idea 2: can use it along with WAR and other metrics to gauge value and the result of value


### Positional Importance
- idea 1: using SHAP (SHapley Additive exPlanations) values to get average marginal contribution
    - train a model on matchups data to predict the winner of a game 
    - analyze how a point at each roster spot affects the predicted outcome as a proxy for positional/roster spot importance
- idea 2: using Z-scores
    - convert each player's performance for that matchup into Z-score of all the player weeks at that position that season.
    - outcome: "for every 1 standard deviation above the mean... {blank} happens"
- idea 3: using Z-scores as the input for SHAP

### Association Analysis
- idea 1: are certain players more likely to be drafted together
- idea 2: are certain players 
- idea 3: what positions/players are more likely to be reached for and when

### Drafting Prediction Model
- predicting what position/player an owner will draft when given 

