#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 16:50:05 2024

@author: reardonfamily

This file contains the process that I used to automate the normalization
and DataModeling of the data extracted from the SportsDataIO Tournament
Endpoint.

"""

#%%
###  Import Libraries

import os
import time
import pandas as pd
from util         import read_apikey
from sportsdataio import _test_leaderboard_endpoint
from sportsdataio import _test_projections_endpoint

from models import clean_scheduleEndpoint_file
from models import scheduleEndpoint_courseModel
from models import scheduleEndpoint_tournamentModel
from models import scheduleEndpoint_roundsModel
from orchestration import post_Leaderboard_Endpoint
from orchestration import post_Projections_Endpoint

#%%

###  Define User Paths
path_apiKey = "/Users/reardonfamily/Desktop/kylewreardon_dataportfolio/"
path_Data   = os.getcwd()

#%%
"""
Identify which TournamentIDs have both the leaderboardEndpoint
and projectionsEndpoint. Ideally in future phases of this project, this
will be built in using an in-Pipeline Exception handling.
"""

lstLeaderboard = _test_leaderboard_endpoint()
lstProjections = _test_projections_endpoint(lstLeaderboard)

#%%
"""
Apply DataModel formatting to tournamentEndpoint.
"""

dfTournament = (
    clean_scheduleEndpoint_file(
        path_Data + "/outputData/dfTournament_ScheduleEndpoint.csv"
        )
    )
dfSchedule_Course_dim, dfSchedule_Course_fact = (
    scheduleEndpoint_courseModel(dfTournament)
    )
dfSchedule_Tournament = (
    scheduleEndpoint_tournamentModel(
        dfTournament, 
        dfSchedule_Course_dim,
        lstLeaderboard,
        lstProjections
        )
    )
dfSchedule_Rounds = (
    scheduleEndpoint_roundsModel(
        read_apikey(
            path_apiKey +
            "sportsdataio_apikey.txt"
            )
        )
    )

dictEndpoint_Schedule = {
    'courses_dim'      : dfSchedule_Course_dim,
    'courses_fact'     : dfSchedule_Course_fact,
    'tournaments_fact' : dfSchedule_Tournament,
    'rounds_fact'      : dfSchedule_Rounds,
    }

"""
print(dfSchedule_Course_dim.dtypes)
dfSchedule_Course_dim.head()

print(dfSchedule_Course_fact.dtypes)
dfSchedule_Course_fact.head()

print(dfSchedule_Tournament.dtypes)
dfSchedule_Tournament.head()

print(dfSchedule_Rounds.dtypes)
dfSchedule_Rounds.head()

"""

for file, data in dictEndpoint_Schedule.items():
    dfOut = data.copy()
    dfOut.to_json(
        path_Data +
        f"/outputData/{file}.json",
        orient = "columns",
        date_format="string",
        index = True#, indent = True
        )
#%%

"""
Now that we have the three dfSchedule_ Tables, we will now upload them to our S3 Bucket.  
For this project our S3 Bucket Name is personal-pgatour-raw-useast1-dev.  

Using terminal window, run the following code:
cd ./Desktop/kylewreardon_dataportfolio/pgatour/outputData 
(base) reardonfamily@Kyles-MacBook-Pro outputData % ls
courses_dim.json	rounds_fact.json
courses_fact.json	tournaments_fact.json
(base) reardonfamily@Kyles-MacBook-Pro scheduleEndpoint % aws s3 cp . s3://personal-pgatour-raw-useast1-dev/sportsdataio/schedule_data/ --recursive --exclude "*" --include "*.json"
upload: ./courses_dim.json to s3://personal-pgatour-raw-useast1-dev/sportsdataio/schedule_data/courses_dim.json
upload: ./courses_fact.json to s3://personal-pgatour-raw-useast1-dev/sportsdataio/schedule_data/courses_fact.json
upload: ./tournaments_fact.json to s3://personal-pgatour-raw-useast1-dev/sportsdataio/schedule_data/tournaments_fact.json
upload: ./rounds_fact.json to s3://personal-pgatour-raw-useast1-dev/sportsdataio/schedule_data/rounds_fact.json
"""

#%%
"""
Execute leaderboard_ep_etl_pipeline and projections_ep_etl_pipeline by
posting Pipeline API Triggers.
"""

dfTournaments_fact = (
    pd.read_json(
        "/Users/reardonfamily/Desktop/DataProjects/GolfProject/dataSportsDataIO/scheduleEndpoint/tournaments_fact.json",
        orient = "columns"
        )
    )
lstIdx = list(
    dfTournaments_fact[
        dfTournaments_fact['endpointProjections'] == 1
        ]['TournamentID'].unique()
    )

for tournament in lstIdx:
    print(
        post_Leaderboard_Endpoint(tournament)
        )
    time.sleep(5)
    print(
        post_Projections_Endpoint(tournament)
        )
    time.sleep(60)