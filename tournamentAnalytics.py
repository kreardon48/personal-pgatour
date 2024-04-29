#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 16:50:05 2024

@author: reardonfamily

This file contains the process needed to execute the leaderboard_anaytics
Pipeline.

"""

import os
import time
import pandas as pd

from orchestration import post_TournamentAnalytics_Pipeline

"""
In Terminal Run the Following to Download tournament_fact.json from AWS S3 Bucket
cd /Users/reardonfamily/Desktop/kylewreardon_dataportfolio/pgatour
aws s3 cp s3://personal-pgatour-raw-useast1-dev/sportsdataio/schedule_data/tournaments_fact.json
./outputData
"""

path_Data   = os.getcwd()
print(path_Data)

dfTournament_Fact = (
    pd.read_json(
        path_Data + "/outputData/tournaments_fact.json",
        orient = "columns"
        )
    )

for tournamentName in dfTournament_Fact['tournamentName'].unique():
    print(tournamentName)

tournamentsIn = [
    'U.S. Open',
    'PGA Championship',
    'The Open Championship'
    ]
for tournament in tournamentsIn:
    if tournament == tournamentsIn[0]:
        actionIn = "replace"
        sleepTime = 120
    else:
        actionIn = "append"
        sleepTime = 30
    print(tournament, actionIn, sleepTime)
    print(
        post_TournamentAnalytics_Pipeline(
            tournament,
            actionIn
            )
        )
    time.sleep(sleepTime)