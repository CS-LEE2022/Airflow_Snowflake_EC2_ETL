This repository includes two technical tasks. Task 1 provides a solution to a SQL challenge. Task 2 implements an ETL pipeline extracting data from a clinical database API, transform and load to AWS S3, then copy into Snowflake database. The solution is scheduled on Airflow and deployed on the AWS cloud platform.

# Task 1: SQL Challenge
The solution syntax is:
<pre>
```sql
SELECT propertyid, unitid, status, total, pastdue
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY propertyid, unitid ORDER BY timestamp DESC) rn
    FROM takehome.raw_rent_roll_history
    where timestamp <= '2023-09-15' -- this is an example date
)t1
WHERE
rn = 1;
```
</pre>

A snapshot of the returned result:
![Alt Text](<images/SQL_result_snapshot.png>)
