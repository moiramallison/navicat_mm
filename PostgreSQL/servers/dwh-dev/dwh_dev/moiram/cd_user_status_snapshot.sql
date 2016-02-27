drop table if exists tmp.cd_user_status_snapshot;

create table tmp.cd_user_status_snapshot
as select gcsi_user_id,
status,
case when status in ('Hold', 'Start/Hold') 
     then dd.day_timestamp::date - valid_from::date
     else 0
end hold_age,
valid_from,
subscription_cohort,
subscription_start_date,
cancel_date,
paid_through_date,
dd.day_timestamp
from common.user_d ud
join common.date_d dd
    on ud.valid_from <= dd.day_timestamp AND
       ud.valid_to >= dd.day_timestamp
where day_key >= '20150701' 
  and day_key <= '20160102'
  and (cancel_date is null or cancel_date >= '20150701'::date)
  and (subscription_end_date is null or subscription_end_date >= '20150701'::date);


/* this is the custom query in Tableau
select day_timestamp, 
        'All Cosmic Disclosure Subscriptions' description,
        sum(running_total) sub_count
from
(select day_timestamp,
    case when subscription_start_date::date <= day_timestamp then 1 
         else 0 
    end running_total
    from tmp.cd_user_status_snapshot
    where subscription_cohort = 'Cosmic Disclosure') t1
group by day_timestamp 
union
(select day_timestamp, 
        'Current Active Subscriptions' description,
        count(1)
from tmp.cd_user_status_snapshot
    where subscription_cohort = 'Cosmic Disclosure'
      and (status = 'Active' or 
           status = 'Hold' and day_Timestamp::date - valid_from < 15 OR
           status = 'Cancelled' and paid_through_date >= day_timestamp
         )
group by day_timestamp)
order by description, day_timestamp
*/