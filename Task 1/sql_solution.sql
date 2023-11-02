SELECT propertyid, unitid, status, total, pastdue
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY propertyid, unitid ORDER BY timestamp DESC) rn
    FROM takehome.raw_rent_roll_history
    where timestamp <= '2023-09-15' -- this is an example date
)t1
WHERE
rn = 1;
