WINDOW w AS (
PARTITION BY user_id
ORDER BY
    start_date,
    paid_through_date,
    end_date ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
);