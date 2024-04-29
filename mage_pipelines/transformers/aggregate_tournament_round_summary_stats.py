if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd


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
        data.iloc[:][
            [
                'PlayerRoundID',
                'RoundNumber',
                'DateScheduled',
                'TeeTime',
                'BackNineStart',
                'IsWithdrawn',
                'PlayerID',
                'TournamentID',
                'PlayerTournamentID'
                ]
            ].drop_duplicates()
    )

    dataRoundScore = (
        pd.merge(
            left = data.iloc[:][
                [
                    'PlayerRoundID',
                    'HoleNumber',
                    'ToParOnRound',
                    'StrokesOnRound'
                    ]
                ],
            right = data.groupby('PlayerRoundID')[
                ['HoleNumber']].max().reset_index(),
            left_on = ['PlayerRoundID', 'HoleNumber'],
            right_on = ['PlayerRoundID', 'HoleNumber']
            )
          .drop(
              columns = ['HoleNumber']
              )
    )
    dataOut = (
        pd.merge(
            how = 'left',
            left = dataOut,
            right = dataRoundScore,
            left_on = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
        )
    )

    dataRoundDrawings = (
        data.groupby('PlayerRoundID')[
            [
                'HoleInOne',
                'DoubleEagle',
                'Eagle',
                'Birdie',
                'BirdieOrBetter',
                'IsPar',
                'Bogey',
                'BogeyOrWorse',
                'DoubleBogey',
                'WorseThanDoubleBogey',
                'TripleBogey',
                'WorseThanTripleBogey'
                ]
            ].sum()
          .reset_index()
          .rename(
              columns = {
                  'HoleInOne'   : 'HoleInOnes',
                  'DoubleEagle' : 'DoubleEagles',
                  'Eagle'       : 'Eagles',
                  'Birdie'      : 'Birdies',
                  'IsPar'       : 'Pars',
                  'Bogey'       : 'Bogeys',
                  'DoubleBogey' : 'DoubleBogeys',
                  'TripleBogey' : 'TripleBogeys'
                  }
              )
          .assign(
              BogeyFree = lambda x: x['BogeyOrWorse'].apply(lambda y: 1 if y == 0 else 0),
              IncludesFiveOrMoreBirdiesOrBetter = (
                  lambda x: x['BirdieOrBetter'].apply(lambda y: 1 if y >= 5 else 0)
                  )
              )
    )
    dataOut = (
        pd.merge(
            how = 'left',
            left = dataOut,
            right = dataRoundDrawings,
            left_on = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
        )
    )

    dataBounceBacks = (
        data[data['BirdieOrBetter'] == 1].groupby('PlayerRoundID')[
            ['BounceBack']].sum().reset_index().rename(
                columns = {'BounceBack':'BounceBackCount'}
                )
    )
    dataOut = (
        pd.merge(
            how = 'left',
            left = dataOut,
            right = dataBounceBacks,
            left_on = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
        )
    )

    dataMaxBirdieStreak = (
        data[data['BirdieOrBetter'] == 1].groupby('PlayerRoundID')[
            'BirdieStreakCounter'].max().to_frame().reset_index().rename(
                columns = {'BirdieStreakCounter' : 'LongestBirdieOrBetterStreak'}
                )
    )
    dataOut = (
        pd.merge(
            how = 'left',
            left = dataOut,
            right = dataMaxBirdieStreak,
            left_on = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
        )
    )

    dataBirdieStreak = (
        data[data['BirdieOrBetter'] == 1].groupby(
            ['PlayerRoundID', 'BirdieStreakID']
            )['BirdieStreakCounter'].max().to_frame().reset_index()
    )
    dataBirdieStreak = (
        dataBirdieStreak[dataBirdieStreak['BirdieStreakCounter'] > 1]
          .assign(
            IncludesStreakOfSixBirdiesOrBetter = (
                lambda x: x['BirdieStreakCounter'].apply(lambda y: 1 if y >= 6 else 0)
                ),
            IncludesStreakOfFiveBirdiesOrBetter = (
                lambda x: x['BirdieStreakCounter'].apply(lambda y: 1 if y >= 5 else 0)
                ),
            IncludesStreakOfFourBirdiesOrBetter = (
                lambda x: x['BirdieStreakCounter'].apply(lambda y: 1 if y >= 4 else 0)
                ),
            IncludesStreakOfThreeBirdiesOrBetter = (
                lambda x: x['BirdieStreakCounter'].apply(lambda y: 1 if y >= 3 else 0)
                ),
            )
    )

    dataBirdieStreak_Length = (
        dataBirdieStreak.groupby('PlayerRoundID')[
            [
                'IncludesStreakOfSixBirdiesOrBetter',
                'IncludesStreakOfFiveBirdiesOrBetter',
                'IncludesStreakOfFourBirdiesOrBetter',
                'IncludesStreakOfThreeBirdiesOrBetter'
                ]
            ].max().reset_index()
    )
    dataOut = (
        pd.merge(
            how = 'left',
            left = dataOut,
            right = dataBirdieStreak_Length,
            left_on = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
        )
    )

    dataBirdieStreak_Count = (
        dataBirdieStreak.groupby('PlayerRoundID')[
            ['BirdieStreakID']
            ].count().reset_index().rename(
            columns = {'BirdieStreakID' : 'ConsecutiveBirdieOrBetterCount'}
            )
    )
    dataOut = (
        pd.merge(
            how = 'left',
            left = dataOut,
            right = dataBirdieStreak_Count,
            left_on = ['PlayerRoundID'],
            right_on = ['PlayerRoundID']
        )
    )

    return dataOut


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
