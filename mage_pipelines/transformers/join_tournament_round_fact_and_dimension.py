if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


@transformer
def transform(data, data_2, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    dataOut = (
        pd.merge(
            left     = data_2,
            right    = data,
            left_on  = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
            )
          .sort_values(
            [
              'TournamentID',
              'PlayerTournamentID',
              'RoundNumber',
              'HoleNumber'
              ]
            )
        )
    dataOut 

    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
