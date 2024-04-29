WITH cte_tournament_details AS
  (
   SELECT
        DISTINCT TournamentID
       ,CourseID
       ,CAST(StartDate AS DATE FORMAT 'YYYY-MM-DD') AS StartDate
   FROM tournament_details.tournament_details_dim
  )
,
cte_course_fact AS
  (
   SELECT
        DISTINCT CourseID
       ,Par
       ,Yards
       ,CAST(StartDate AS DATE FORMAT 'YYYY-MM-DD') AS StartDate
   FROM tournament_details.golfcourse_fact
  )
,
cte_course_fact_filter AS
  (
   SELECT
        a.CourseID
       ,b.TournamentID
       ,MAX(a.StartDate) AS StartDate
   FROM cte_course_fact  a
   JOIN cte_tournament_details  b
   ON  a.CourseID = b.CourseID
   AND a.StartDate <= b.StartDate
   
   GROUP BY
        a.CourseID
       ,b.TournamentID
  )
,
cte_course_dim AS
  (
   SELECT
        DISTINCT CourseID
       ,courseName
       ,courseLocation
   FROM tournament_details.golfcourse_dim
  )

SELECT
     a.TournamentID
    ,d.courseName
    ,d.courseLocation
    ,b.Par
    ,b.Yards
    ,b.StartDate AS LayoutStartDate
    ,a.CourseID
FROM cte_tournament_details  a
JOIN cte_course_fact  b
ON  a.CourseID = b.CourseID

JOIN cte_course_fact_filter  c
ON  b.StartDate = c.StartDate
AND a.TournamentID = c.TournamentID

JOIN cte_course_dim  d
ON  a.CourseID = d.CourseID
;