WITH user_day_all AS (
    SELECT *
    FROM (
            SELECT DISTINCT user_id
            FROM default.churn_submits
        ) t1
        CROSS JOIN (
            SELECT DISTINCT timestamp::DATE as day
            FROM default.churn_submits
        ) t2
    ORDER BY user_id,
        day
),
user_day AS (
    SELECT user_id,
        timestamp::DATE as day,
        COUNT(submit) as n_submits,
        COUNT(DISTINCT task_id) as n_tasks,
        SUM(is_solved) as n_solved
    FROM default.churn_submits
    GROUP BY user_id,
        day
),
agg_fixed AS (
    SELECT *
    FROM user_day_all
        LEFT JOIN user_day USING(user_id, day)
),
wf_res AS (
    SELECT *,
        maxIf(day, n_submits > 0) OVER (
            PARTITION BY user_id
            ORDER BY day
        ) AS last_visit_day,
        countIf(n_submits > 0) OVER (
            PARTITION BY user_id
            ORDER BY day
        ) AS window_size,
        countIf(n_submits = 0) OVER (
            PARTITION BY user_id
            ORDER BY day
        ) AS ws_0,
        maxIf(day, n_submits = 0) OVER (
            PARTITION BY user_id
            ORDER BY day
        ) AS last_off_day,
        SUM(n_submits) OVER (
            PARTITION BY user_id
            ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS sum_submits_14d,
        SUM(n_solved) OVER (
            PARTITION BY user_id
            ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS sum_solved_14d,
        SUM(n_solved) OVER (
            PARTITION BY user_id
            ORDER BY day
        ) AS solved_total
    FROM agg_fixed
),
feat1 AS (
    SELECT day,
        user_id,
        IF(toDayOfWeek(day) IN (6, 7), 1, 0) as is_weekend,
        IF(
            window_size != 0,
            dateDiff('day', last_visit_day, day),
            ws_0::int
        ) as days_offline,
        MAX(days_offline) OVER(
            PARTITION BY user_id
            ORDER BY day
        ) as max_days_off,
        IF(
            ws_0 != 0,
            dateDiff('day', last_off_day, day),
            window_size::int
        ) as days_online,
        MAX(days_online) OVER(
            PARTITION BY user_id
            ORDER BY day
        ) as max_days_on,
        sum_submits_14d / 14 AS avg_submits_14d,
        IF(
            sum_submits_14d != 0,
            sum_solved_14d / sum_submits_14d,
            0
        ) AS success_rate_14d,
        solved_total
    FROM wf_res
),
submits_lvl as (
    SELECT user_id,
        timestamp::DATE as day,
        task_id,
        task_level,
        submit,
        score,
        is_solved
    FROM default.churn_submits
        LEFT JOIN (
            SELECT task_id,
                task_level
            FROM default.churn_tasks
            GROUP BY task_id,
                task_level
        ) t1 USING(task_id)
),
all_days_submits AS (
    SELECT *
    FROM user_day_all
        LEFT JOIN submits_lvl USING(user_id, day)
),
feat2 AS (
    SELECT day,
        user_id,
        IF(
            n_u_task_14d != 0,
            n_submits_14d / n_u_task_14d,
            0
        ) as subm_p_task_14d,
        IF(
            n_u_task_14d != 0,
            n_solved_14d / n_u_task_14d,
            0
        ) as solved_task_rate_14d,
        sum_score_14d / 14 as avg_score_14d --,sum_score_28_14d / 14 as avg_score28_14d
,
        avg_score_14d - (sum_score_28_14d / 14) as score_diff,
        IF(
            n_u_task_14d != 0,
(
                1 * n_u_task_1_14d + 2 * n_u_task_2_14d + 3 * n_u_task_3_14d + 4 * n_u_task_4_14d
            ) / n_u_task_14d,
            0
        ) as avg_level_14d,
        IF(
            n_solved_14d != 0,
(
                1 * n_ust_1_14d + 2 * n_ust_2_14d + 3 * n_ust_3_14d + 4 * n_ust_4_14d
            ) / n_solved_14d,
            0
        ) as avg_slvd_level_14d
    FROM (
            SELECT day,
                user_id,
                countIf(DISTINCT task_id, submit > 0) OVER w as n_u_task_14d,
                countIf(submit, submit > 0) OVER w as n_submits_14d,
                SUM(is_solved) OVER w as n_solved_14d,
                countIf(DISTINCT task_id, task_level = 1) OVER w as n_u_task_1_14d,
                countIf(DISTINCT task_id, task_level = 2) OVER w as n_u_task_2_14d,
                countIf(DISTINCT task_id, task_level = 3) OVER w as n_u_task_3_14d,
                countIf(DISTINCT task_id, task_level = 4) OVER w as n_u_task_4_14d,
                countIf(
                    DISTINCT task_id,
                    is_solved
                    AND (task_level = 1)
                ) OVER w as n_ust_1_14d,
                countIf(
                    DISTINCT task_id,
                    is_solved
                    AND (task_level = 2)
                ) OVER w as n_ust_2_14d,
                countIf(
                    DISTINCT task_id,
                    is_solved
                    AND (task_level = 3)
                ) OVER w as n_ust_3_14d,
                countIf(
                    DISTINCT task_id,
                    is_solved
                    AND (task_level = 4)
                ) OVER w as n_ust_4_14d,
                sumIf(score, is_solved) OVER w as sum_score_14d,
                sumIf(score, is_solved) OVER (
                    PARTITION BY user_id
                    ORDER BY day RANGE BETWEEN 27 PRECEDING AND 14 PRECEDING
                ) as sum_score_28_14d
            FROM all_days_submits WINDOW w AS (
                    PARTITION BY user_id
                    ORDER BY day RANGE BETWEEN 13 PRECEDING AND CURRENT ROW
                )
        )
    GROUP BY day,
        user_id,
        subm_p_task_14d,
        solved_task_rate_14d,
        avg_score_14d,
        score_diff,
        avg_level_14d,
        avg_slvd_level_14d
    ORDER BY day,
        user_id
),
feat_total AS (
    SELECT *
    FROM feat1
        JOIN feat2 USING(day, user_id)
    WHERE day = '2022-11-19'
)
SELECT day,
    user_id,
    (
        0.009 * is_weekend + 0.001 * days_offline - 0.019 * max_days_off - 0.637 * days_online - 1.048 * max_days_on - 0.189 * avg_submits_14d - 0.096 * success_rate_14d - 0.087 * solved_total - 0.526 * subm_p_task_14d - 0.249 * solved_task_rate_14d - 0.055 * avg_score_14d -0.046 * score_diff - 0.234 * avg_level_14d - 0.277 * avg_slvd_level_14d
    ) as predict_14d
FROM feat_total
