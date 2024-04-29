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
    #roundPartition = ['PlayerTournamentID', 'RoundNumber']

    dataOut = (
        data.assign(
          HoleInOne            = lambda x: x['Strokes'].apply(lambda y: 1 if y ==  1 else 0),
          DoubleEagle          = lambda x: x['ToPar'  ].apply(lambda y: 1 if y == -3 else 0),
          Eagle                = lambda x: x['ToPar'  ].apply(lambda y: 1 if y == -2 else 0),
          Birdie               = lambda x: x['ToPar'  ].apply(lambda y: 1 if y == -1 else 0),
          BirdieOrBetter       = lambda x: x['ToPar'  ].apply(lambda y: 1 if y <= -1 else 0),
          IsPar                = lambda x: x['ToPar'  ].apply(lambda y: 1 if y ==  0 else 0),
          Bogey                = lambda x: x['ToPar'  ].apply(lambda y: 1 if y ==  1 else 0),
          BogeyOrWorse         = lambda x: x['ToPar'  ].apply(lambda y: 1 if y >=  1 else 0),
          DoubleBogey          = lambda x: x['ToPar'  ].apply(lambda y: 1 if y ==  2 else 0),
          WorseThanDoubleBogey = lambda x: x['ToPar'  ].apply(lambda y: 1 if y  >  2 else 0),
          TripleBogey          = lambda x: x['ToPar'  ].apply(lambda y: 1 if y ==  3 else 0),
          WorseThanTripleBogey = lambda x: x['ToPar'  ].apply(lambda y: 1 if y  >  3 else 0)
        )
    )
    
    dataOut['startStreak'] = (
        dataOut.BirdieOrBetter.ne(
            dataOut.groupby('PlayerRoundID')['BirdieOrBetter'].shift()
            )
    )
    dataOut['streakID'] = (
        dataOut.groupby('PlayerRoundID')['startStreak'].cumsum()
    )
    dataOut['streakCounter'] = (
        dataOut.groupby(['PlayerRoundID', 'streakID']).cumcount() + 1
    )
    dataOut = (
        dataOut.drop(
            columns = ['startStreak']
            )
          .rename(
            columns = {
              'streakID' : 'BirdieStreakID',
              'streakCounter' : 'BirdieStreakCounter'
              }
            )
    )
    dataOut['BounceBack'] = (
        dataOut.BirdieOrBetter.eq(
            dataOut.groupby('PlayerRoundID')['BogeyOrWorse'].shift(+1)
            )
    )
    dataOut['BounceBack'] = dataOut.apply(lambda x: (x['BounceBack'] * 1), axis = 1)

    dataOut['ToParOnRound'] = (
        dataOut.groupby('PlayerRoundID')['ToPar'].cumsum()
    )
    dataOut['StrokesOnRound'] = (
        dataOut.groupby('PlayerRoundID')['Strokes'].cumsum()
    )

    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
