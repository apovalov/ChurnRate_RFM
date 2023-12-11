SELECT
    a.user_id AS user_id,
    a.day AS day,
    (CASE WHEN n_submits > 0 THEN n_submits ELSE 0 END) AS n_submits,
    (CASE WHEN n_tasks > 0 THEN n_tasks ELSE 0 END) AS n_tasks,
    (CASE WHEN n_solved > 0 THEN n_solved ELSE 0 END) AS n_solved
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
