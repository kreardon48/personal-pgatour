name = "orchestration"

import json
import requests
import pandas as pd

def post_Leaderboard_Endpoint(tournamentId):
    """
    Trigger leaderboard_ep_etl_pipeline by posting
    API Trigger, post_leaderboard_endpoint.
        - Extract leaderboardEndpoint, Transform into player_round_fact
          and player_hole_fact, using outlined Data Model, and Load to AWS S3.

    Parameters
    ----------
    tournamentId : integer
        TournamentID in to which the ETL Pipeline will run.

    """

    headers = {
        "Content-Type" : "application/json"
        }
    payload = {
        "pipeline_run": {
            "variables": {
                "tournament": f"{tournamentId}"
                }
            }
        }
    
    response = requests.post(
        "http://localhost:6789/api/pipeline_schedules/2/pipeline_runs/efca0244099744fabf95b3ca6590ecf6",
        headers = headers,
        data    = json.dumps(payload)
        )
    return response.json()


def post_Projections_Endpoint(tournamentId):
    """
    Trigger projections_ep_etl_pipeline by posting
    API Trigger, post_projections_endpoint.
        - Extract projectionsEndpoint, Transform into player_projections_fact
          and player_fantasy_fact, using outlined Data Model, and Load to AWS S3.

    Parameters
    ----------
    tournamentId : integer
        TournamentID in to which the ETL Pipeline will run.

    """

    headers = {
        "Content-Type" : "application/json"
        }
    payload = {
        "pipeline_run": {
            "variables": {
                "tournament": f"{tournamentId}"
                }
            }
        }
    
    response = requests.post(
        "http://localhost:6789/api/pipeline_schedules/3/pipeline_runs/e2a44c39372b4bca8c23d48a01fe01f7",
        headers = headers,
        data    = json.dumps(payload)
        )
    return response.json()

def post_TournamentAnalytics_Pipeline(tournamentName, loadAction):
    """
    Trigger leaderboard_analytics_pipeline by posting
    API Trigger, post_TournamentAnalytics.
        - Extract player_hole_fact, player_round_fact, course_dim, and course_fact all
          historical Tournaments for the passed TournamentName, Transform into
          the appropriate Analytics Tables and Load Google BigQuery.

    Parameters
    ----------
    tournamentName : string
        TournamentName in to which the Analytics ETL Pipeline will run.
    
    loadAction : string
        The action that will be used (replace/append) when loading analytics
        Dataset to appropriate BigQuery Datasets and Tables.

    """

    headers = {
        "Content-Type" : "application/json"
        }
    payload = {
        "pipeline_run": {
            "variables": {
                "tournament": f"{tournamentName}",
                "load_action" : f"{loadAction}"
                }
            }
        }
    
    response = requests.post(
        "http://localhost:6789/api/pipeline_schedules/4/pipeline_runs/9169e60ab94e4b859e1352e8056b8bbf",
        headers = headers,
        data = json.dumps(payload)
        )
    return response.json()