import io
import pandas as pd
import requests

from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):

    api_key = get_secret_value('sportsdataio_api_key')
    
    """
    Template for loading data from API
    """
    url = 'https://api.sportsdata.io/golf/v2/json/Players?key={api_key}'.format(
        api_key = api_key
        )
    response = requests.get(url)

    return response.json()


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
