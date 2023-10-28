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
Groups AS (
    SELECT
        id.user_id,
        id.day,
        SUM(id.n_submits) as n_submits,
        -- Изменение логики для группировки активных и неактивных дней
        dateDiff('day', toDate('2000-01-01'), id.day) - sumIf(1, id.n_submits = 0) OVER (PARTITION BY id.user_id ORDER BY id.day ASC) AS group_id
    FROM IntermediateData id
    GROUP BY id.user_id, id.day
),
InactiveDays AS (
    SELECT
        user_id,
        group_id,
        COUNT() AS inactive_length -- Длина каждой группы неактивных дней
    FROM Groups
    WHERE n_submits = 0
    GROUP BY user_id, group_id
),
ActiveDays AS (
    SELECT
        user_id,
        group_id,
        COUNT() AS active_length -- Длина каждой группы активных дней
    FROM Groups
    WHERE n_submits > 0
    GROUP BY user_id, group_id
),
MaxConsecutiveInactive AS (
    SELECT
        user_id,
        MAX(inactive_length) AS max_consecutive_inactive_days
    FROM InactiveDays
    GROUP BY user_id
),
MaxConsecutiveActive AS (
    SELECT
        user_id,
        MAX(active_length) AS max_consecutive_active_days
    FROM ActiveDays
    GROUP BY user_id
),
FirstJoin AS (
    SELECT
        id.*,
        mca.max_consecutive_active_days
    FROM IntermediateData id
    LEFT JOIN MaxConsecutiveActive mca ON id.user_id = mca.user_id
),
SecondJoin AS (
    SELECT
        fj.*,
        mci.max_consecutive_inactive_days
    FROM FirstJoin fj
    LEFT JOIN MaxConsecutiveInactive mci ON fj.user_id = mci.user_id
),
cte AS (
    SELECT
        fj.user_id,
        fj.day,
        fj.n_submits,
        MAX(CASE WHEN fj.n_submits > 0 THEN fj.day END)
        OVER (PARTITION BY fj.user_id ORDER BY fj.day ASC Rows BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as last_active_day,
        fj.n_solved,
        SUM(fj.n_submits) OVER(PARTITION BY fj.user_id ORDER BY fj.day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_submits_14d,
        SUM(fj.n_solved) OVER(PARTITION BY fj.user_id ORDER BY fj.day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS solved_last_14d,
        SUM(fj.n_solved) OVER(PARTITION BY fj.user_id ORDER BY fj.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS solved_total,
        SUM(fj.n_submits) OVER(PARTITION BY fj.user_id ORDER BY fj.day ROWS BETWEEN 1 FOLLOWING AND 14 FOLLOWING) AS submits_next_14_days,
        max_consecutive_active_days,
        max_consecutive_inactive_days
    FROM SecondJoin fj
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
    max_consecutive_active_days,
    max_consecutive_inactive_days,
    CASE WHEN submits_next_14_days > 0 THEN 0 ELSE 1 END AS target_14d
FROM cte;