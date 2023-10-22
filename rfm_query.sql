WITH IntermediateData AS (
WITH DateRange AS (
    SELECT
        toDate(min(timestamp)) AS start_date,
        toDate(max(timestamp)) AS end_date
    FROM churn_submits
),
NumberedDays AS (
    SELECT
        range(toUInt64(dateDiff('day', start_date, end_date) + 1)) AS day_offsets
    FROM DateRange
),
AllDates AS (
    SELECT
        start_date + arrayJoin(day_offsets) AS day
    FROM DateRange, NumberedDays
),
AllUsers AS (
    SELECT DISTINCT user_id
    FROM churn_submits
),
AllUserDays AS (
    SELECT
        user_id,
        day
    FROM AllUsers
    CROSS JOIN AllDates
)

SELECT
    AllUserDays.user_id,
    AllUserDays.day,
    COUNT(CASE WHEN coalesce(churn_submits.submit, 0) > 0 THEN 1 ELSE NULL END) AS n_submits,
    COUNTDistinct(CASE WHEN coalesce(churn_submits.task_id, 0) > 0 THEN churn_submits.task_id ELSE NULL END) AS n_tasks,
    SUM(multiIf(churn_submits.is_solved = 1, 1, 0)) AS n_solved
FROM AllUserDays
LEFT JOIN churn_submits ON AllUserDays.user_id = churn_submits.user_id AND AllUserDays.day = toDate(churn_submits.timestamp)
GROUP BY AllUserDays.user_id, AllUserDays.day

),
cte AS (
    SELECT
        user_id,
        day,
        n_submits,
        MAX(CASE WHEN n_submits > 0 THEN day END)
        OVER (PARTITION BY user_id ORDER BY day ASC Rows BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as last_active_day,
        n_solved,
        SUM(n_submits) OVER(PARTITION BY user_id ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_submits_14d,
        SUM(n_solved) OVER(PARTITION BY user_id ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS solved_last_14d,
        SUM(n_solved) OVER(PARTITION BY user_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS solved_total,
        SUM(n_submits) OVER(PARTITION BY user_id ORDER BY day ROWS BETWEEN CURRENT ROW AND 13 FOLLOWING) AS submits_next_14_days
    FROM IntermediateData
)

SELECT
    day,
    user_id,
    CASE
        WHEN n_submits > 0 THEN 0
        ELSE dateDiff('day', last_active_day, day)
    END AS days_offline,
    avg_submits_14d / 14 AS avg_submits_14d,
    CASE WHEN avg_submits_14d = 0 THEN 0 ELSE solved_last_14d / (14 * avg_submits_14d) END AS success_rate_14d,
    solved_total,
    CASE WHEN submits_next_14_days > 0 THEN 0 ELSE 1 END AS target_14d,

FROM cte;