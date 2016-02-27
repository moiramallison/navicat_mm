
drop table if exists tmp.paid_through_dates;

create table tmp.paid_through_dates as 
select  sd.subscription_id, 
        se.schedule_date, 
        lead(se.schedule_date) over w as paid_through_date 
from common.subscription_d sd
join gcsi.subscription_event se 
    on se.subscription_id = sd.subscription_id
join gcsi.subscription_transaction_subscription_events stse 
    on stse.subscription_event_id = se.id
join gcsi.subscription_transaction st 
    on st.id = stse.subscription_transaction_id and 
       st.txn_state = 'SETTLED'
       -- I think I need at least a year for annuals
where se.schedule_date > '20150101'
window w as (
partition by sd.subscription_id
order by
    se.schedule_date
rows between unbounded preceding
and unbounded following
);

create index idx_ptd_sub on tmp.paid_through_dates (subscription_id);
create index idx_ptd_sub_ptdate on tmp.paid_through_dates (subscription_id, paid_through_date);


insert into moiram.daily_status_snapshot
select dd.day_timestamp,
    gcsi_user_id,
    ud.subscription_id,
    --the last scheduled transaction represents the current status?
    coalesce(ptd.paid_through_date,ud.paid_through_date) paid_through_date,
    case when status = 'Hold' or status = 'Start/Hold' then 
         case when sh.initiator in  ('CUSTOMER', 'ADMINISTRATIVE')
              then 'Hold'
              else 'Suspended'
         end 
         when cancel_date <= dd.day_timestamp then 
         case when ptd.paid_through_date > dd.day_timestamp 
              then 'Lapsed'
              else 'Cancelled'
         end 
         else status
    end status
from common.user_d ud
join common.date_d dd
    on ud.valid_from <= dd.day_timestamp AND
       ud.valid_to >= dd.day_timestamp
left join gcsi.subscription_hold sh 
    on ud.subscription_id = sh.subscription_id and 
       sh.deleted = 'f' and 
       day_timestamp >= sh.start_date and 
       (day_timestamp <= sh.end_date or sh .end_date is null)
join tmp.paid_through_dates ptd
    on day_timestamp > ptd.schedule_date and
       day_timestamp <= ptd.schedule_date
where day_key >= 20151001
  and day_timestamp <= current_date::date
  and (cancel_date is null or cancel_date >= '20151001'::date)
  and (subscription_end_date is null or subscription_end_date >= '20151001'::date);