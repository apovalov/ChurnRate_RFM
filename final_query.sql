SELECT
    day,
    user_id,
    (+ 0.06534785 * month_day - 0.5257531 * month + 0.05685462 * week_day
     + 0.01856204 * days_offline - 0.07516209 * online_days - 0.64478201 * avg_online
     + 0.0026427 * offline_days - 0.09769561 * avg_ofline - 2.47956163 * avg_submits_14d
     - 0.41700846 * success_rate_14d - 0.20706006 * solved_total - 0.29862989 * avg_level_task
     - 0.2011549 * max_level_task_solved + 0.08495633 * day_score_14 + 0.18994795 * total_score
     - 0.79502429) AS predict_14d
FROM (

SELECT date AS day, user_id,
       EXTRACT(day from date) / 100 AS month_day,
       EXTRACT(month from date) / 10 AS month,
       DAYOFWEEK(date) / 10 AS week_day,
       CASE WHEN last_day_online < (SELECT MIN(timestamp::DATE)
                                    FROM default.churn_submits) THEN NULL
            ELSE date - last_day_online
       END AS days_offline,
       online_days, avg_online, offline_days, avg_ofline,
       submits_14d / 14 AS avg_submits_14d,
       CASE WHEN submits_14d = 0 THEN 0
            ELSE success_14d / submits_14d
       END AS success_rate_14d,
       solved_total,
       avg_level_task,
       max_level_task_solved,
       day_score_14,
       total_score,
       CASE WHEN active_14d = 0 THEN 1
            ELSE 0
       END AS target_14d
FROM (SELECT date, user_id,
             MAX(date) FILTER (WHERE n_submits <> 0) OVER (PARTITION BY user_id ORDER BY date) AS last_day_online,
             SUM(n_submits) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS submits_14d,
             SUM(n_solved) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS success_14d,
             SUM(n_solved) OVER (PARTITION BY user_id ORDER BY date) AS solved_total,
             SUM(n_submits) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 1 FOLLOWING AND 14 FOLLOWING) AS active_14d,
             avg_level_task, max_level_task_solved,
             SUM(day_score) OVER (PARTITION BY user_id ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS day_score_14,
             SUM(day_score) OVER (PARTITION BY user_id ORDER BY date) AS total_score,
             COUNT(on) FILTER (WHERE on = 1) OVER (PARTITION BY user_id ORDER BY date) AS online_days,
             COUNT(off) FILTER (WHERE off = 1) OVER (PARTITION BY user_id ORDER BY date) AS offline_days,
             AVG(on) OVER (PARTITION BY user_id ORDER BY date) AS avg_online,
             AVG(off) OVER (PARTITION BY user_id ORDER BY date) AS avg_ofline
      FROM (SELECT user_id, date, n_submits, n_tasks, n_solved, avg_level_task, max_level_task_solved, day_score, on, 1 - on AS off
            FROM (SELECT user_id, date
                  FROM (SELECT DISTINCT user_id AS user_id
                        FROM default.churn_submits) AS t1
                  CROSS JOIN (SELECT DISTINCT timestamp::DATE AS date
                              FROM default.churn_submits) AS t2) AS t3
            LEFT JOIN (SELECT user_id, date,
                              COUNT(submit) AS n_submits,
                              COUNT(DISTINCT task_id) AS n_tasks,
                              COUNT(is_solved) FILTER (WHERE is_solved = True) AS n_solved,
                              AVG(task_level) AS avg_level_task,
                              MAX(task_level) FILTER (WHERE is_solved = True) AS max_level_task_solved,
                              SUM(score) FILTER (WHERE is_solved = True) AS day_score,
                              1 AS on
                       FROM (SELECT submit_id, timestamp::DATE AS date, user_id, submit, score, is_solved, task_id, task_level, task_tags, task_steps, task_min_score
                             FROM default.churn_submits AS t4
                             JOIN default.churn_tasks AS t5 ON t4.task_id = t5.task_id) AS t6
                             GROUP BY user_id, date) AS t7
            ON t3.user_id = t7.user_id AND t3.date = t7.date) AS t8) AS t9
ORDER BY user_id, day

) features
WHERE day = '2022-11-19'