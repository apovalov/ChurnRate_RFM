# Online platform churn rate


1. Data aggregation
2. Negatives Mining
3. RFM Analysis
4. Feature Engineering


# 1.Data aggregation
We have:
One line = one attempt to solve some problem by some student. The time, the score obtained, the number of attempts to solve this problem by this student (the more tries - the better understanding.... if, of course, the student can solve the problem at all), success or failure of the solution.


```
                    timestamp  user_id  task_id  submit  score  is_solved
0  2022-06-22 01:07:15.311388      784       57       1   1.00       True
1  2022-06-22 01:36:18.281159      785      310       1   0.07      False
2  2022-06-22 01:17:19.463893      787       67       1   0.19      False
3  2022-06-22 01:19:46.894989      797       90       1   1.00       True
4  2022-06-22 01:58:49.300107      800      247       1   0.03      False

```


Aggregation by (user_id, day)
Before we move on, let's try to perform a simple aggregation.

Let's do the math for each student....

n_submits - how many submits each student made in a day
n_tasks - how many problems each student tried to solve.
n_solved - how many submits were successful.
The table should be sorted by user_id and day.

```
One line = one attempt to solve some problem by some student. The time, the score obtained, the number of attempts to solve this problem by this student (the more tries - the better understanding.... if, of course, the student can solve the problem at all), success or failure of the solution.
```

# 2.Negatives Mining
Excellent, a user-day will play the role of the atomic unit of our dataset.

Missing days
But here's the problem, if you count how many days there are in the source table (D) and how many unique users (U), the number of rows in the dataset obtained in the previous step is significantly less. Can you guess why?

It is because on some days the user visited the site and on some days he did not. On the days when he did not visit the site, there were no activities (no attempts to solve tasks), so these data are not reflected in the table with sabmits.


# 3.RFM-analysis

Recency - how long ago was the last transaction/event/action?
Frequency - how often have transactions/events/actions occurred in the last N days?
Monetary - how much money has the user spent in the last N days?


# Feature Descriptions

The following features represent user engagement and activity, which are used for predictive modeling:

1. `days_offline` - The number of days since the last website visit (0 if the user visited the site on the current day, 1 if they did not visit today but did yesterday, 2 if the day before, etc.).
2. `avg_submits_14d` - The average number of submission attempts in the last 14 days (including the current day). For edge dates where less than 14 days of data is available, the average is taken over the available days.
3. `success_rate_14d` - The proportion of successful attempts in the last 14 days (including the current day).
4. `solved_total` - The cumulative number of successful submissions up to the current day.
5. `target_14d` - Whether the user will be online in the next 14 days (1 if they will not be online, 0 if they will be).

The last point is particularly crucial as it is not a feature but the target variable we aim to predict.

# 4. Feature Engineering

What other signs can you think of similar to those given in the previous task?

* How many maximum consecutive days has the user been offline? How many maximum days has the user been?
* How does the average level of tasks the user takes on grow? How does the level of tasks the user successfully completes grow?
* How many attempts on average does it take the user to complete a task?
* How often does the user finish a task?
* What is the user's average scor for the last N days? How has it changed from the previous period of similar length?
