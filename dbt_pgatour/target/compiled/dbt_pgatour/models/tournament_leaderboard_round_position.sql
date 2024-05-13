

WITH cte_leaderboard_round AS
  (
   SELECT
        DISTINCT PlayerRoundID
       ,RoundNumber
       ,BackNineStart
       ,PlayerID
       ,TournamentID
       ,PlayerTournamentID
   FROM `personal-pgatour`.`tournament_leaderboard`.`leaderboard_round_stats`
  )
,
cte_leaderboard_hole AS
(
 SELECT
     DISTINCT PlayerRoundID
    ,HoleNumber
    ,Par
    ,ToPar
    ,Strokes
    ,ToParOnRound
    ,StrokesOnRound
 FROM `personal-pgatour`.`tournament_leaderboard`.`leaderboard_hole_stats`
)

SELECT
     rnd.PlayerRoundID
    ,rnd.RoundNumber
    ,hole.HoleNumber
    ,hole.Par
    ,hole.ToPar
    ,hole.Strokes
    ,hole.ToParOnRound
    ,hole.StrokesOnRound
    ,rnd.BackNineStart
    ,rnd.PlayerID
    ,rnd.TournamentID
    ,rnd.PlayerTournamentID
    ,RANK() OVER (
        PARTITION BY rnd.TournamentID, rnd.RoundNumber, hole.HoleNumber
        ORDER BY hole.ToParOnRound ASC
        ) AS LeaderboardPositionOnRound_ToPar
    ,RANK() OVER (
        PARTITION BY rnd.TournamentID, rnd.RoundNumber, hole.HoleNumber
        ORDER BY hole.StrokesOnRound ASC
        ) AS LeaderboardPositionOnRound_Strokes
FROM cte_leaderboard_round  rnd
JOIN cte_leaderboard_hole   hole
ON  rnd.PlayerRoundID = hole.PlayerRoundID

ORDER BY
     rnd.TournamentID
    ,rnd.PlayerTournamentID
    ,rnd.RoundNumber
    ,hole.HoleNumber