

select status, count(1)
from common.subscription_d 
group by status;


select status, count(1)
from common.daily_status_snapshot 
where day_timestamp::date = current_date::date
group by status;

SELECT *
FROM common.daily_status_snapshot dss
WHERE dss.gcsi_user_id= 14432797
ORDER BY dss.day_timestamp DESC
LIMIT 20;

select * from common.user_d 
where gcsi_user_id = 14432797;

SELECT *
FROM common.subscription_d sd
WHERE sd.subscription_id = ;

select count(1) from common.user_d where valid_from = valid_to;

select gcsi_user_id, subscription_id, status, 
       subscription_start_date, paid_through_date, valid_from, valid_to
from common.user_d where gcsi_user_id in 
(select gcsi_user_id
from
    (select gcsi_user_id,
            day_timestamp today,
        lead(day_timestamp) over w tomorrow
    from common.daily_status_y2016q1
    WINDOW w AS (
        PARTITION BY gcsi_user_id
        ORDER BY day_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    ))foo
where tomorrow is not null AND
      tomorrow::date - today::date > 1)
order by gcsi_user_id, valid_from;


alter table moiram.daily_status_y2016q1 set schema  common;
alter table common.daily_status_y2016q1  owner to dw_admin;

SELECT count(*)
FROM common.daily_status_snapshot dss
WHERE
dss.day_timestamp >= '2015-02-26'
AND dss.day_timestamp < '2015-02-27'

select * from gcsi.t_subscriptions where subscription_id = 23058108;
