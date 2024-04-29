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
    dataOut.columns = (
        list(dataOut.columns)[0:1] +
        ['Proj_' + col for col in dataOut.columns[1:]]
        )
    print(dataOut.columns)
    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
