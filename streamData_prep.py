import json
import pandas as pd
import datetime as dt

"""
Run post_Leaderboard_Endpoint() for TournamentIDs in [18, 72].
Run post_TournamentAnalytics_Pipeline for
  TournamentName == 'The Masters Tournament'.
Run post_Leaderboard_Endpoint() and post_Projections_Endpoint() for
  TournamentID in [575].

aws s3 cp s3://personal-pgatour-raw-useast1-dev/sportsdataio/tournament_data/tournament=575/player_holes_fact.json ./streamData
aws s3 rm s3://personal-pgatour-raw-useast1-dev/sportsdataio/tournament_data/tournament=575/player_holes_fact.json
aws s3 cp s3://personal-pgatour-raw-useast1-dev/sportsdataio/tournament_data/tournament=575/player_rounds_fact.json ./streamData
"""
#%%

def define_course_side(backStart, holePlayed):
    if backStart == 1:
        if holePlayed in range(10, 19):
            side = '0.Out'
        else:
            side = '1.In'
    else:
        if holePlayed in range(1, 10):
            side = '0.Out'
        else:
            side = '1.In'
    return side

def pace_of_play(groupSize, coursePar):
    if groupSize == 1:
        roundTime = 11.5 * 18
    elif groupSize == 2:
        roundTime = 12.5 * 18
    elif groupSize == 3:
        roundTime = 13.5 * 18
    else:
        roundTime = 14.5 * 18
    
    minPerPar = roundTime / coursePar
    return minPerPar
#%%

dfRounds_575 = pd.read_json(
    "./streamData/player_rounds_fact_575.json"
    )
dfRounds_575['DummyIndex'] = 'holes'
print(dfRounds_575.shape)

dictDummyHoles = {
    'HoleNumber': [idx for idx in range(1, 19)]
    }
dfDummyHoles = pd.DataFrame(dictDummyHoles)
dfDummyHoles['DummyIndex'] = 'holes'

dfRounds_575 = (
    pd.merge(
        left = dfRounds_575,
        right = dfDummyHoles,
        left_on = ['DummyIndex'],
        right_on = ['DummyIndex']
        )
      .drop(
        columns = ['DummyIndex']
        )
    )
print(dfRounds_575.columns)
print(dfRounds_575.shape)

dfRounds_575['courseSide'] = dfRounds_575.apply(
    lambda x: define_course_side(
        x['BackNineStart'],
        x['HoleNumber'   ]
        ),
        axis = 1
    )
dfRounds_575 = dfRounds_575.sort_values(
    ['PlayerRoundID', 'courseSide', 'HoleNumber']
    )
dfRounds_575['thruHoleNumber'] = dfRounds_575.groupby(
    'PlayerRoundID'
    )['HoleNumber'].cumcount() + 1

dfHoles_575 = pd.read_json(
        "./streamData/player_holes_fact_575.json"
    )
dfHoleLayout = dfHoles_575[['HoleNumber', 'Par']].drop_duplicates()

dfRounds_575 = (
    pd.merge(
        left     = dfRounds_575,
        right    = dfHoleLayout,
        left_on  = ['HoleNumber'],
        right_on = ['HoleNumber']
        )
    )
print(dfRounds_575.shape)

dfRounds_575_GroupSize = (
    dfRounds_575.groupby(
        [
            'RoundNumber',
            'BackNineStart',
            'TeeTime'
            ]
        )[['PlayerRoundID']].nunique().reset_index().rename(columns = {'PlayerRoundID':'GroupSize'})
    )

dfRounds_575 = (
    pd.merge(
        left     = dfRounds_575,
        right    = dfRounds_575_GroupSize,
        left_on  = ['RoundNumber', 'BackNineStart', 'TeeTime'],
        right_on = ['RoundNumber', 'BackNineStart', 'TeeTime']
        )
    )
dfRounds_575['timePerPar'] = dfRounds_575.apply(
    lambda x: pace_of_play(x['GroupSize'], 72), axis = 1
    )
dfRounds_575['timeOnHole'] = dfRounds_575.apply(
    lambda x: (x['Par'] * x['timePerPar']), axis = 1
    )
dfRounds_575['timeOnCourse'] = dfRounds_575.groupby(
    ['PlayerRoundID']
    )['timeOnHole'].cumsum()
dfRounds_575['timestampScore'] = dfRounds_575.apply(
    lambda x: (
        dt.datetime.strptime(
            x['TeeTime'],
            '%Y-%m-%dT%H:%M:%S'
            ) + dt.timedelta(minutes = x['timeOnCourse'])), axis = 1
    )

dictRounds_575 = {
    f'Round{round}' : pd.DataFrame() for round in list(
        dfRounds_575['RoundNumber'].unique()
        )
    }
for key in dictRounds_575.keys():
    roundNumber = int(key[-1])
    dfTmp = dfRounds_575[dfRounds_575['RoundNumber'] == roundNumber][:]
    dictRounds_575[key] = dfTmp

for value in dictRounds_575.values():
    print(value['RoundNumber'].unique())
    print(value.shape)

dfRounds_575 = (
    dfRounds_575.drop(
        columns = [
            'DateScheduled',
            'TeeTime',
            'BackNineStart',
            'IsWithdrawn',
            'PlayerID',
            'PlayerTournamentID',
            'TournamentID',
            'Par',
            'GroupSize',
            'timePerPar',
            'timeOnHole',
            'timeOnCourse'
            ]
        )
    )
#%%

dfHoles_575 = pd.read_json(
        "./streamData/player_holes_fact_575.json"
    )

dfTournament_575 = (
    pd.merge(
        left = dfHoles_575,
        right = dfRounds_575,
        left_on = ['PlayerRoundID', 'HoleNumber'],
        right_on = ['PlayerRoundID', 'HoleNumber']
        )
    )
dfTournament_575 = (dfTournament_575.iloc[:][
    [
        'timestampScore',
        'PlayerRoundID',
        'RoundNumber',
        'HoleNumber',
        'Par',
        'ScrambledStrokes',
        'ToPar',
        'Strokes'
        ]
    ].sort_values(['timestampScore', 'ToPar']))

for round in list(dfTournament_575['RoundNumber'].unique()):
    fileName = f'round{round}'
    dfTournament_575[
        dfTournament_575['RoundNumber'] == round][
            [
                'timestampScore',
                'PlayerRoundID',
                'HoleNumber',
                'Par',
                'ScrambledStrokes',
                'ToPar',
                'Strokes',
                ]
            ].to_csv(
                f"./streamData/players_hole_stream_{fileName}.csv"
                )
#%%
"""
dictTournament_575 = {
    f'Round{round}' : {
        streamTime : pd.DataFrame() for streamTime in list(
            dfTournament_575['stringstampScore'].unique()
            )
        } for round in list(
            dfTournament_575['RoundNumber'].unique()
            )
    }
for round, stream1 in dictTournament_575.items():
    roundNumber = int(round[-1])
    for stream2 in stream1.keys():
        dfStream = (
            dfTournament_575[
                (dfTournament_575['RoundNumber'   ] == roundNumber) &
                (dfTournament_575['stringstampScore'] == stream2    )
                ][[
                    'PlayerRoundID',
                    'HoleNumber',
                    'Par',
                    'ScrambledStrokes',
                    'ToPar',
                    'Strokes',
                    ]]
            ).to_dict(orient = "records")
        
        dictTournament_575[round][stream2] = dfStream

jsonTournament_575 = json.dumps(dictTournament_575, indent=4)
with open("./streamData/players_hole_stream.json", "w") as file:
    json.dump(jsonTournament_575, file)
"""