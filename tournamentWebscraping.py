#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 16:50:05 2024

@author: reardonfamily

This file contains the process, that I used to automate the standardization,
and normalization of the data extracted from the SportsDataIO Tournament
Endpoint.

"""

#%%
###  Import Libraries

import os
import joblib as jb
import pandas as pd

from util         import read_apikey
from util         import espn_pga_schedule_start_date
from util         import clean_espn_pga_schedule_date
#from webscrapers  import wiki_tournament_search
#from webscrapers  import espn_pga_schedule_extract
from sportsdataio import extract_tournamentEndpoint

#%%
###  Define User Paths
path_apiKey = "/Users/reardonfamily/Desktop/kylewreardon_dataportfolio/"
path_Data   = os.getcwd()

#%%
###  Import SportsDataIO Tournament Endpoint
"""
The Tournament Endpoint .json extract from SportsDataIO uses
TournamentNames, CourseNames and CourseLocations from the PGA Tour
website. Unfortunately, this data is not consistent. For example,
look at the Arnold Palmer Invitational, and the Waste Management
Phoenix Open.  

In an attempt to automate the cleaning process of the .json file, use the
below process to execute a Google search for the Year and Tournament Name.
Following the Google search, export the CourseName and CourseLocations
from ESPN's Golf Schedule.  
"""

dfTournament_Tmp = (
    extract_tournamentEndpoint(
        read_apikey(
            path_apiKey +
            "sportsdataio_apikey.txt"
            )
        )
    )
dfTournament = dfTournament_Tmp.copy()
#dfTournament = dfTournament.iloc[:5]
#%%
"""
Using wiki_tournament_search() function, perform Google Search to return
wikipedia link that is associated with the searched TournamentName and
TournamentDate.

lstWikiLinks = []
for index, row in dfTournament.iterrows():
    yyyy, tournament = row['EndDate'].year, row['TournamentName']
    print(yyyy, tournament)
    lstWikiLinks.append(wiki_tournament_search(yyyy, tournament))


dictWikiLinks = {
    'SeasonYear'       : [idx[0] for idx in lstWikiLinks],
    'apiTournametName' : [idx[1] for idx in lstWikiLinks],
    'searchTitle'      : [idx[2] for idx in lstWikiLinks],
    'searchURL'        : [idx[3] for idx in lstWikiLinks]
    }
jb.dump(
    dictWikiLinks,
    path_Data + "/scrapedData/dictWikiLinks.pkl")

dictWikiLinks = jb.load(path_Data + "/scrapedData/dictWikiLinks.pkl")
dfWikiLinks = pd.DataFrame.from_dict(dictWikiLinks)
dfWikiLinks['SeasonYear'] = dfWikiLinks['SeasonYear'].astype(str)
dfWikiLinks['wikiLink'] = dfWikiLinks.apply(
    lambda x: x['searchURL'].split('/wiki/')[-1].strip(), axis = 1)
dfWikiLinks.reset_index(inplace = True)

dfWikiLinks_Tmp = (
    pd.read_excel(
        path_Data +
        "/scrapedData/dfWikiLinks_Tmp.xlsx"
        )
      .drop(columns = ['Unnamed: 0'])
    )
dfWikiLinks_Tmp['SeasonYear'] = dfWikiLinks_Tmp['SeasonYear'].astype(str)

dfWikiLinks = (
    pd.merge(
        left = dfWikiLinks,
        right = dfWikiLinks_Tmp,
        left_on = [
            'SeasonYear',
            'apiTournametName',
            'searchURL'
            ],
        right_on = [
            'SeasonYear',
            'apiTournametName',
            'searchURL'
            ]
        )
      .drop_duplicates()
      .drop(columns = ['searchTitle_x'])
      .rename(columns = {'searchTitle_y':'tournamentName_Clean'})
    )

jb.dump(dfWikiLinks, path_Data + "/scrapedData/dfWikiLinks.pkl")
"""

dfWikiLinks = (
    jb.load(
        path_Data + 
        "/scrapedData/dfWikiLinks.pkl"
        )
      .set_index(['index'])
      .drop(
          columns = [
              'SeasonYear',
              'apiTournametName',
              'searchURL',
              'wikiLink'
              ]
          )
    )

dfTournament = (
    pd.merge(
        left        = dfTournament,
        right       = dfWikiLinks,
        left_index  = True,
        right_index = True
        )
    )

#%%
"""
startDate = dt.date(2014, 9, 1)
endDate   = dt.date(2025, 3, 15)
lstSeason = [idx.strftime("%Y") for idx in pd.date_range(startDate, endDate, freq = "YE")]

dfESPN_Schedule = pd.DataFrame()
for season in lstSeason:
    dfTmp_Schedule = espn_pga_schedule_extract(season)
    dfESPN_Schedule = pd.concat([dfESPN_Schedule, dfTmp_Schedule], sort = False)

jb.dump(
    dfESPN_Schedule, 
    path_Data + 
    "/scrapedData/dfESPN_Schedule.pkl"
    )
"""
dfESPN_Schedule = (
    jb.load(
        path_Data + 
        "/scrapedData/dfESPN_Schedule.pkl"
        )
    )

dictESPN_Schedule = {season : pd.DataFrame() for season in list(dfESPN_Schedule['Season'].unique())}
dfESPN_TournamentSchedule = pd.DataFrame()
totalRows = 0
for key in dictESPN_Schedule.keys():
    print(key)
    d01 = dfESPN_Schedule[dfESPN_Schedule['Season'] == key][:].reset_index().drop(columns = ['index'])
    d01['tmpStartMonth'] = d01.apply(lambda x: espn_pga_schedule_start_date(x['DATES']), axis = 1)
    newYearIdx = d01[d01['tmpStartMonth'] == "Jan"].index.min()
    d01.loc[d01.index < newYearIdx, 'tmpStartYear'] = key.split("-")[0]
    d01.loc[d01['tmpStartYear'].isna() == True, 'tmpStartYear'] = "20" + key.split("-")[1]
    d01.loc[d01['Season'] == "2023-24", 'tmpStartYear'] = "20" + key.split("-")[1]
    d01['startDate'] = d01.apply(lambda x: clean_espn_pga_schedule_date(x['DATES'], x['tmpStartYear'], "start"), axis = 1)
    d01['endDate'  ] = d01.apply(lambda x: clean_espn_pga_schedule_date(x['DATES'], x['tmpStartYear'], "end"  ), axis = 1)
    d01 = d01.drop(columns = [col for col in d01.columns if col not in ['DATES', 'TOURNAMENT', 'Season', 'startDate', 'endDate']]).drop_duplicates()
    splitData = d01['TOURNAMENT'].str.split("\n", n = 3, expand = True).drop(columns = [2])
    d01["tournamentName"] = splitData[0]
    d01["courseName"] = splitData[1]
    splitData = d01['courseName'].str.split("-", n = 1, expand = True)
    d01['courseName'] = splitData[0].str.strip()
    d01['courseLocation'] = splitData[1].str.strip()
    d01.loc[d01['courseLocation'] == "La Quinta, CA", "courseName"] = "La Quinta Country Club"
    d01.loc[d01['courseLocation'] == "Truckee, CA", "courseName"] = "Tahoe Mountain Club (Old Greenwood)"
    print(d01.shape)
    d02 = pd.merge(
        left = d01,
        right = dfTournament,
        left_on = ['startDate', 'tournamentName'],
        right_on = ['StartDate', 'TournamentName']
        )
    d02 = d02.iloc[:][['TournamentID', 'tournamentName', 'courseName', 'courseLocation', 'TournamentName']]
    print(d02.shape)
    totalRows = totalRows + d02.shape[0]
    print(totalRows)
    dictESPN_Schedule[key] = d02
    dfESPN_TournamentSchedule = pd.concat([dfESPN_TournamentSchedule, d02], sort = False)

dfTournament = (
    pd.merge(
        how      = 'left',
        left     = dfTournament,
        right    = dfESPN_TournamentSchedule,
        left_on  = [
            'TournamentID',
            'TournamentName'
            ],
        right_on = [
            'TournamentID',
            'TournamentName'
            ]
        )
    )

"""
jb.dump(
    dfESPN_TournamentSchedule,
    path_Data + 
    "/scrapedData/dfESPN_TournamentSchedule.pkl"
    )

dfTournament.to_csv(
    path_date +
    "/outputData/dfTournament_ScheduleEndpoint.csv"
    )
"""
#%%
"""
After running the above process from start to finish, roughly 85% of all
Tournaments and Courses were populated with the correct standardized name.
The 15% that remains was not populated incorrectly, rather they were not
populated becasue they either havent happened yet, or they are new
tournaments and didnt have an online profile to scrape.  

To account for the remaining 15%, I have manually reviewed the results,
and saved them to the above exported .csv file that can be imported and
used for the main portion of this project.  
"""