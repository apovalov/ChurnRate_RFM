SELECT
    day,
    user_id,
    day - last_day_online AS days_offline,
    submits_14d / 14 AS avg_submits_14d,
    CASE WHEN submits_14d > 0 THEN solved_14d / submits_14d ELSE 0 END AS success_rate_14d,
    solved_total,
    solved_next_14d == 0 AS target_14d
FROM (
    SELECT
        a.day AS day,
        a.user_id AS user_id,
        MAX(CASE WHEN n_submits > 0 THEN a.day ELSE null END) OVER(
            PARTITION BY a.user_id ORDER BY a.day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_day_online,
        SUM(n_submits) OVER(
            PARTITION BY a.user_id ORDER BY a.day
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS submits_14d,
        SUM(n_solved) OVER(
            PARTITION BY a.user_id ORDER BY a.day
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS solved_14d,
        SUM(n_solved) OVER(
            PARTITION BY a.user_id ORDER BY a.day
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS solved_total,
        SUM(n_submits) OVER(
            PARTITION BY a.user_id ORDER BY a.day
            ROWS BETWEEN 1 FOLLOWING AND 14 FOLLOWING
            ) AS solved_next_14d
    FROM (
        SELECT day, user_id
        FROM (
            SELECT DISTINCT(DATE(timestamp)) AS day
            FROM churn_submits
        ) x
        CROSS JOIN (
            SELECT DISTINCT(user_id) AS user_id
            FROM churn_submits
        ) y
    ) a
    LEFT JOIN (
        SELECT
            user_id,
            DATE(timestamp) AS day,
            COUNT(submit) AS n_submits,
            COUNT(DISTINCT(task_id)) AS n_tasks,
            SUM(is_solved) AS n_solved
        FROM churn_submits
        GROUP BY user_id, day
    ) b
    ON a.user_id = b.user_id AND a.day = b.day
    ORDER BY user_id, day
)
