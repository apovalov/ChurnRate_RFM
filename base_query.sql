SELECT
    user_id,
    toDate(timestamp) AS day,
    COUNT() AS n_submits,
    COUNTDistinct(task_id) AS n_tasks,
    SUM(if(is_solved, 1, 0)) AS n_solved
FROM churn_submits
GROUP BY user_id, day
ORDER BY user_id, day;
