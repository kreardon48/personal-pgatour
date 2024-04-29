import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
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
        pd.json_normalize(
            data['Players'],
            meta = ['TournamentID', 'PlayerID', 'IsWithdrawn'],
            record_path = ['Rounds']
            )
          .assign(
            intBackNineStart = lambda x: x['BackNineStart'].apply(lambda y: 1 if y == True else 0),
            intIsWithdrawn   = lambda x: x['IsWithdrawn'  ].apply(lambda y: 1 if y == True else 0),
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
    print(dataOut.columns)
    
    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
