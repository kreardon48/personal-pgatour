import io
import pandas as pd
import requests

from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(**tournaments):

    api_key = get_secret_value('sportsdataio_api_key')
    tournament = tournaments.get('tournament')

    """
    Template for loading data from API
    """
    url = r'https://api.sportsdata.io/golf/v2/json/PlayerTournamentProjectionStats/{tournament}?key={api_key}'.format(
        tournament = tournament, api_key = api_key
        )
    response = requests.get(url)
    response_json = response.json()

    return pd.DataFrame(response_json)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
