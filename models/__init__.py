name = 'models'

import requests
import datetime as dt
import pandas   as pd

from util import convert_object2date

def clean_scheduleEndpoint_file(filePath):
    dfTournamentTmp = (
        pd.read_csv(filePath)
          .drop(
              columns = [
                  'Unnamed: 0',
                  'TournamentName',
                  'IsOver',
                  'IsInProgress',
                  'CourseName',
                  'Location',
                  'City',
                  'State',
                  'ZipCode',
                  'Country',
                  'TimeZone',
                  'tournamentName'
                  ]
              )
          .rename(
              columns = {
                  'tournamentName_Clean' : 'tournamentName'
                  }
              )
        )
    dfTournamentTmp['StartDate'] = dfTournamentTmp.apply(lambda x: dt.datetime.strptime(x['StartDate'], "%Y-%m-%d"), axis = 1)
    dfTournamentTmp['EndDate'  ] = dfTournamentTmp.apply(lambda x: dt.datetime.strptime(x['EndDate'  ], "%Y-%m-%d"), axis = 1)
    meanPar = float(
        round(
            dfTournamentTmp[dfTournamentTmp['Par'].isna() == False]['Par'].mean(),
            0
            )
        )
    meanYards = float(
        round(
            dfTournamentTmp[dfTournamentTmp['Yards'].isna() == False]['Yards'].mean(),
            0
            )
        )
    dfTournamentTmp.loc[dfTournamentTmp['Par'   ].isna() == True, 'Par'   ] = meanPar
    dfTournamentTmp.loc[dfTournamentTmp['Yards' ].isna() == True, 'Yards' ] = meanYards
    dfTournamentTmp.loc[dfTournamentTmp['Purse' ].isna() == True, 'Purse' ] = 0.0
    dfTournamentTmp.loc[dfTournamentTmp['Format'] == "TeamMatch", 'Format'] = "Team"
    
    dfTournamentTmp_FormatTmp = (
        dfTournamentTmp[dfTournamentTmp['Format'].isna() == False].groupby(['courseName'])['Format'].nunique().to_frame().reset_index()
        )
    dfTournamentTmp_Format = dfTournamentTmp[(
        dfTournamentTmp['courseName'].isin(
            list(
                dfTournamentTmp_FormatTmp[dfTournamentTmp_FormatTmp['Format'] == 1]['courseName'].unique()
                )
            )
        ) & (
        dfTournamentTmp['Format'].isna() == False
        )][['courseName', 'Format']].drop_duplicates()
    dfTournamentTmp = (
        pd.merge(
            how = 'left',
            left = dfTournamentTmp,
            right = dfTournamentTmp_Format,
            left_on = ['courseName'],
            right_on = ['courseName']
            )
          .rename(
              columns = {
                  'Format_x':'oldFormat',
                  'Format_y':'Format'
                  }
              )
        )
    
    dfTournamentTmp_FormatTmp = (
    dfTournamentTmp[dfTournamentTmp['Format'].isna() == False].groupby(['tournamentName'])['Format'].count().to_frame().reset_index()
        )
    dfTournamentTmp_Format = dfTournamentTmp[(
        dfTournamentTmp['tournamentName'].isin(
            list(
                dfTournamentTmp_FormatTmp['tournamentName'].unique()
                )
            )
        ) & (
        dfTournamentTmp['oldFormat'].isna() == False
        )][['tournamentName', 'oldFormat']].drop_duplicates()
    dfTournamentTmp = (
        pd.merge(
            how = 'left',
            left = dfTournamentTmp,
            right = dfTournamentTmp_Format,
            left_on = ['tournamentName'],
            right_on = ['tournamentName']
            )
        )
    dfTournamentTmp.loc[dfTournamentTmp['Format'].isna() == True, 'Format'] = dfTournamentTmp['oldFormat_y']
    dfTournamentTmp = dfTournamentTmp.drop(columns = ['oldFormat_x', 'oldFormat_y']).drop_duplicates()
    #The above standardization of the Format worked for all but 1 tournamentID: 32, so we will fix that with a .loc
    dfTournamentTmp.loc[dfTournamentTmp['TournamentID'] == 32, 'Format'] = 'Stroke'
    dfTournamentTmp['Purse'] = dfTournamentTmp["Purse"].astype(int)
    return dfTournamentTmp


def scheduleEndpoint_courseModel(dfIn):
    dfTmp = dfIn.groupby(
        [
            'courseName',
            'courseLocation',
            'Par',
            'Yards'
            ]
        )['StartDate'].min().reset_index()
    dfTmp['CourseID'] = dfTmp.index + 1000
    dfTmpIdx = dfTmp.groupby(
        [
            'courseName',
            'courseLocation'
            ]
        )['CourseID'].max().reset_index()
    dfTmp = (
        pd.merge(
            how      = 'left',
            left     = dfTmp,
            right    = dfTmpIdx,
            left_on  = [
                'courseName',
                'courseLocation'
                ],
            right_on = [
                'courseName',
                'courseLocation'
                ]
            )
          .drop(columns = ['CourseID_x'])
          .rename(columns = {'CourseID_y':'CourseID'})
        )
    dfCourse_dim  = dfTmp.iloc[:][['CourseID', 'courseName', 'courseLocation']].drop_duplicates()
    dfCourse_fact = dfTmp.iloc[:][['CourseID', 'StartDate', 'Par', 'Yards']]
    dfCourse_fact["StartDate"] = dfCourse_fact["StartDate"].astype(str)
    dfCourse_fact["Par"      ] = dfCourse_fact["Par"      ].astype(int)
    dfCourse_fact["Yards"    ] = dfCourse_fact["Yards"    ].astype(int)
    return dfCourse_dim, dfCourse_fact


def scheduleEndpoint_tournamentModel(dfInTournament, dfInCourse_Dim, lstLeaderIn, lstProjIn):
    dfTournament_fact = (
        pd.merge(
            left = dfInTournament,
            right = dfInCourse_Dim,
            left_on = [
                'courseName',
                'courseLocation'
                ],
            right_on = [
                'courseName',
                'courseLocation'
                ]
            )
          .drop(
              columns = [
                  'Par',
                  'Yards',
                  'courseName',
                  'courseLocation',
                  'StartDateTime'
                  ]
              )
          .assign(
              intCanceled = lambda x: x['Canceled'].apply(lambda y: 1 if y == True else 0),
              intCovered  = lambda x: x['Covered' ].apply(lambda y: 1 if y == True else 0),
              endpointLeaderboard = lambda x: x['TournamentID'].apply(lambda y: 1 if y in lstLeaderIn else 0),
              endpointProjections = lambda x: x['TournamentID'].apply(lambda y: 1 if y in lstProjIn   else 0)
              )
       .iloc[:][
        [
            'TournamentID',
            'tournamentName',
            'StartDate',
            'EndDate',
            'Purse',
            'intCanceled',
            'intCovered',
            'Format',
            'SportRadarTournamentID',
            'OddsCoverage',
            'endpointLeaderboard',
            'endpointProjections',
            'CourseID'
            ]]
        .rename(columns = {'intCanceled':'Canceled', 'intCovered':'Covered'})
        )
    dfTournament_fact['StartDate'] = dfTournament_fact['StartDate'].astype(str)
    dfTournament_fact['EndDate'  ] = dfTournament_fact['EndDate'  ].astype(str)
    return dfTournament_fact


def scheduleEndpoint_roundsModel(apiKey):
    dfRounds_fact = (
        pd.json_normalize(
            requests.get(
                f'https://api.sportsdata.io/golf/v2/json/Tournaments?key={apiKey}'
                    ).json(),
            record_path = ["Rounds"]
            )
          .rename(
              columns = {
                  'Number':'RoundNumber',
                  'Day':'DateScheduled'
                  }
              )
          .iloc[:][['RoundID', 'RoundNumber', 'DateScheduled', 'TournamentID']]
        )
    dfRounds_fact['DateScheduled'] = dfRounds_fact.apply(lambda x: convert_object2date(x['DateScheduled']), axis = 1)
    dfRounds_fact['DateScheduled'] = dfRounds_fact.apply(lambda x: x['DateScheduled'].strftime("%Y-%m-%d"), axis = 1)
    return dfRounds_fact