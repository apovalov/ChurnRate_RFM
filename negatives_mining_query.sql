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
ORDER BY AllUserDays.user_id, AllUserDays.day;
