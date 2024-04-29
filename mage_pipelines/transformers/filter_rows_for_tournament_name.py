if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import datetime as dt

@transformer
def transform(data, *args, **tournaments):
    tournament = tournaments.get('tournament')
    """
    tournament = ['U.S. Open']
    if not isinstance(tournament, list):
        raise ValueError('tournament Input must be list')
    else:
        pass
    """
    
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
    todayString = dt.datetime.today().strftime("%Y-%m-%d")
    print(todayString)
    dataOut = data[
        #(data['tournamentName'].isin(tournament)) &
        (data['tournamentName'] == tournament ) &
        (data['StartDate'     ] <  todayString) &
        (data['EndDate'       ] <  todayString)
        ][:]

    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
