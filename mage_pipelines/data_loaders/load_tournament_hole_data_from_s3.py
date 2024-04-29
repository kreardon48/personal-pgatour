from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_from_s3_bucket(data, *args, **kwargs):
    lstIds = [
        str(tournament) for tournament in list(
            data['TournamentID'].unique()
            )
        ]
    """
    Template for loading data from a S3 bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """
    config_path    = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'personal-pgatour-raw-useast1-dev'
    dataOut = pd.DataFrame()
    for tournament in lstIds:
        holes_object_key  = f'sportsdataio/tournament_data/tournament={tournament}/player_holes_fact.json'
        #rounds_object_key = f'sportsdataio/tournament_data/tournament={tournament}/player_rounds_fact.json'
        try:
            dataHoles = (
                S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
                bucket_name,
                holes_object_key,
                )
            )
            """
            dataRounds = (
                S3.with_config(ConfigFileLoader(config_path, config_profile)).load(
                bucket_name,
                rounds_object_key,
                )
            )
            dataTmp = (
                pd.merge(
                    left     = dataRounds,
                    right    = dataHoles,
                    left_on  = ['PlayerRoundID'],
                    right_on = ['PlayerRoundID']
                    )
                )
            dataOut = pd.concat([dataOut, dataTmp], sort = False)
            """
            dataOut = pd.concat([dataOut, dataHoles], sort = False)
        except:
            print('TournamentID does not have available Data')
    """
    dataOut = dataOut.sort_values(
        [
            'TournamentID',
            'PlayerTournamentID',
            'RoundNumber',
            'HoleNumber'
            ]
        )
    """
    dataOut = dataOut.sort_values(['PlayerRoundID', 'HoleNumber'])
    print(lstIds)
    print(dataOut.shape)

    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
