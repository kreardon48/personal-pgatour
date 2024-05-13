

  create or replace view `personal-pgatour`.`tournament_leaderboard`.`tournament_leaderboard_tournament_position`
  OPTIONS()
  as 

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
,
cte_leaderboard_tournament AS
  (
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
       ,SUM(ToPar) OVER (
           PARTITION BY rnd.PlayerTournamentID
           ORDER BY rnd.RoundNumber, hole.HoleNumber
           ) AS ToParOnTournament
       ,SUM(Strokes) OVER (
           PARTITION BY rnd.PlayerTournamentID
           ORDER BY  rnd.RoundNumber, hole.HoleNumber ASC
           ) AS StrokesOnTournament
   FROM cte_leaderboard_round  rnd
   JOIN cte_leaderboard_hole   hole
   ON  rnd.PlayerRoundID = hole.PlayerRoundID
  )

SELECT
     trny.PlayerRoundID
    ,trny.RoundNumber
    ,trny.HoleNumber
    ,trny.Par
    ,trny.ToPar
    ,trny.Strokes
    ,trny.ToParOnRound
    ,trny.StrokesOnRound
    ,trny.ToParOnTournament
    ,trny.StrokesOnTournament
    ,trny.BackNineStart
    ,trny.PlayerID
    ,trny.TournamentID
    ,trny.PlayerTournamentID
    ,RANK() OVER (
        PARTITION BY trny.TournamentID, trny.RoundNumber, trny.HoleNumber
        ORDER BY trny.ToParOnTournament ASC
        ) AS LeaderboardPositionOnTournament_ToPar
    ,RANK() OVER (
        PARTITION BY trny.TournamentID, trny.RoundNumber, trny.HoleNumber
        ORDER BY trny.StrokesOnTournament ASC
        ) AS LeaderboardPositionOnTournament_Strokes
FROM cte_leaderboard_tournament  trny

ORDER BY
     trny.TournamentID
    ,trny.PlayerTournamentID
    ,trny.RoundNumber
    ,trny.HoleNumber;

