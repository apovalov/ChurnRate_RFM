SELECT
    user_id,
    DATE(timestamp) AS day,
    COUNT(submit) AS n_submits,
    COUNT(DISTINCT(task_id)) AS n_tasks,
    SUM(is_solved) AS n_solved
FROM churn_submits
GROUP BY user_id, day
ORDER BY user_id, day
