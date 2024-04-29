SELECT 
     TournamentID
    ,CASE WHEN RoundNumber = 1 THEN '18Hole Results'
          WHEN RoundNumber = 2 THEN '36Hole Results'
          WHEN RoundNumber = 3 THEN '54Hole Results'
          WHEN RoundNumber = 4 THEN '72Hole Results'
          END AS RoundDescription
    ,HoleNumber
    ,LeaderboardPositionOnTournament_ToPar
    ,LeaderboardPositionOnTournament_Strokes
    ,COUNT(DISTINCT PlayerTournamentID) AS ttl_players_in_position
    ,MIN(ToParOnRound       ) AS ToParOnRound
    ,MIN(StrokesOnRound     ) AS StrokesOnRound
    ,MIN(ToParOnTournament  ) AS ToParOnTournament
    ,MIN(StrokesOnTournament) AS StrokesOnTournament
FROM `personal-pgatour.tournament_leaderboard.leaderboard_tournament_position`
WHERE HoleNumber = 18
GROUP BY
     TournamentID
    ,CASE WHEN RoundNumber = 1 THEN '18Hole Results'
          WHEN RoundNumber = 2 THEN '36Hole Results'
          WHEN RoundNumber = 3 THEN '54Hole Results'
          WHEN RoundNumber = 4 THEN '72Hole Results'
          END
    ,HoleNumber
    ,LeaderboardPositionOnTournament_ToPar
    ,LeaderboardPositionOnTournament_Strokes
ORDER BY
     TournamentID
    ,RoundDescription
    ,HoleNumber
    ,LeaderboardPositionOnTournament_ToPar
    ,LeaderboardPositionOnTournament_Strokes
LIMIT 1000
;