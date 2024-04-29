name = 'sportsdataio'

import requests
import datetime as dt
import pandas   as pd

from util import convert_object2date

def extract_tournamentEndpoint(apiKey):
    """
    Using Tournaments API Endpoint:
        1. Import .json File, and convert to pd.DataFrame
        2. Drop the nested array in dfRequest_Tmp.Rounds
        3. Rename columns to be more contextual

    Parameters
    ----------
    apiKey : string
        Provided API Key from SportsDataIO to access authorized Endpoints.

    Returns
    -------
    dfRequest : pd.DataFrame
        Entire PGA Golf Tournament Schedule dating back to 2014 and through
        2025.

    """
    
    dfRequest = (
        pd.DataFrame(
            requests.get(
                f'https://api.sportsdata.io/golf/v2/json/Tournaments?key={apiKey}'
                ).json()
            )
          .drop(columns = ["Rounds"])
          .rename(
              columns = {'Name'  : 'TournamentName',
                         'Venue' : 'CourseName'}
              )
          #.assign(CourseId = '')
        )
    
    dfRequest['StartDate'] = dfRequest.apply(
        lambda x: convert_object2date(
            x['StartDate'],
            '%Y-%m-%dT%H:%M:%S'
            ), axis = 1
        )
    dfRequest['EndDate'] = dfRequest.apply(
        lambda x: convert_object2date(
            x['EndDate'],
            '%Y-%m-%dT%H:%M:%S'
            ), axis = 1
        )
    
    return dfRequest


def _test_leaderboard_endpoint():
    """
    Not all of the TournamentIDs have available data at all the endpoints that are
    used in this process. Since this is for extraciriccular learning, and I havent
    figured out how to use Exception Handling in a Production Pipeline, this function
    will allow the user to filter on TournamentIDs that have the necessary endpoints.

    Returns
    -------
    lstOutput : list
        List of TournamentIDs that have all the necessary data available with the
        leaderboardEndpoint.

    """

    lstOutput = []
    lstIds = (
        list(
            extract_tournamentEndpoint(
                open('../.././kylewreardon_dataportfolio/sportsdataio_apikey.txt', 'r').read()
                )['TournamentID'].unique()
            )
        )
    def test_leaderboard_endpoint(tournamentIn):
        data = (
            requests.get(
            'https://api.sportsdata.io/golf/v2/json/Leaderboard/{tournamentid}?key={golfData_Api}'
                .format(
                    tournamentid = tournamentIn,
                    golfData_Api = open('../.././kylewreardon_dataportfolio/sportsdataio_apikey.txt', 'r').read()
                    )
                ).json()
            )
        try:
            (
                pd.json_normalize(
                    data['Players'],
                    meta = ['TournamentID', 'PlayerID', 'IsWithdrawn'],
                    record_path = ['Rounds']
                    )
                  .assign(
                    intBackNineStart = lambda x: x['BackNineStart'].apply(
                        lambda y: 1 if y == True else 0
                        ),
                    intIsWithdrawn   = lambda x: x['IsWithdrawn'  ].apply(
                        lambda y: 1 if y == True else 0
                        ),
                    )
                  .iloc[:][[
                    'PlayerRoundID',
                    'Number',
                    'Day',
                    'TeeTime',
                    'intBackNineStart',
                    'intIsWithdrawn',
                    'PlayerID',
                    'PlayerTournamentID',
                    'TournamentID'
                    ]]
                  .rename(
                    columns = {
                        'Number'           : 'RoundNumber'  ,
                        'Day'              : 'DateScheduled',
                        'intBackNineStart' : 'BackNineStart',
                        'intIsWithdrawn'   : 'IsWithdrawn'  ,
                        }
                    )
                )
            
            (
                pd.json_normalize(
                    data['Players'],
                    meta = ['PlayerTournamentID'],
                    record_path = ['Rounds', 'Holes']
                    )
                  .assign(
                    Strokes = lambda x: x['Par'] + x['ToPar']
                    )
                  .iloc[:][[
                    'PlayerRoundID',
                    'Number',
                    'Par',
                    'Score',
                    'ToPar',
                    'Strokes',
                    ]]
                  .rename(
                    columns = {
                        'Number' : 'HoleNumber'      ,
                        'Score'  : 'ScrambledStrokes'
                        }
                    )
                )
            lstOutput.append(tournamentIn)
            strOut = f"{tournamentIn} Appended to lstLeaderboard"
        except:
            strOut = f"{tournamentIn} endpoint Has No Data"
        
        return strOut
    
    for idx in lstIds:
        print(
            test_leaderboard_endpoint(idx)
            )
    
    return lstOutput


def _test_projections_endpoint(lstIn):
    """
    Not all of the TournamentIDs have available data at all the endpoints that are
    used in this process. Since this is for extraciriccular learning, and I havent
    figured out how to use Exception Handling in a Production Pipeline, this function
    will allow the user to filter on TournamentIDs that have the necessary endpoints.

    Returns
    -------
    lstProjections_Tmp : list
        List of TournamentIDs that have all the necessary data available with the
        projectionsEndpoint.

    """
    lstOutput = []
    
    def test_projections_endpoint(tournamentIn):
        data = (
            pd.DataFrame(
                requests.get(
                    'https://api.sportsdata.io/golf/v2/json/PlayerTournamentProjectionStats/{tournamentid}?key={golfData_Api}'
                        .format(
                            tournamentid = tournamentIn,
                            golfData_Api = open('../.././kylewreardon_dataportfolio/sportsdataio_apikey.txt', 'r').read()
                            )
                    ).json()
                )
            )
        try:
            (
                data.drop(
                    columns = [
                        'Name',
                        'Country',
                        'TotalStrokes',
                        'TotalThrough',
                        'Earnings',
                        'FedExPoints',
                        'FantasyPoints',
                        'FantasyPointsDraftKings',
                        'DraftKingsSalary',
                        'TeeTime',
                        'TournamentStatus',
                        'IsAlternate',
                        'FanDuelSalary',
                        'FantasyDraftSalary',
                        'MadeCutDidNotFinish',
                        'FantasyPointsFanDuel',
                        'FantasyPointsFantasyDraft',
                        'IsWithdrawn',
                        'FantasyPointsYahoo',
                        'Rounds',
                        'PlayerID',
                        'TournamentID'
                        ]
                    )
                )
            
            (
                data.iloc[:][[
                'PlayerTournamentID',
                'FantasyPoints',
                'FantasyPointsDraftKings',
                'DraftKingsSalary',
                'IsAlternate',
                'FanDuelSalary',
                'FantasyDraftSalary',
                'MadeCutDidNotFinish',
                'FantasyPointsFanDuel',
                'FantasyPointsFantasyDraft',
                'IsWithdrawn',
                'FantasyPointsYahoo'
                ]]
                )
            lstOutput.append(tournamentIn)
            strOut = f"{tournamentIn} Appended to lstProjections"
        except:
            strOut = f"{tournamentIn} endpoint Has No Data"
        
        return strOut
    
    for idx in lstIn:
        print(
            test_projections_endpoint(idx)
            )
    
    return lstOutput


