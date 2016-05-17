
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
where se.schedule_date > '20130101'
window w as (
partition by sd.subscription_id
order by
    se.schedule_date
rows between unbounded preceding
and unbounded following
);

CREATE INDEX idx_ptd_sub ON tmp.paid_through_dates (subscription_id);
CREATE INDEX idx_ptd_sub_ptdate ON tmp.paid_through_dates (subscription_id, paid_through_date);

alter table tmp.paid_through_dates owner to dw_admin;

 
  truncate table common.daily_status_y2016q2;
 
       
  insert into common.daily_status_y2016q2
  select dd.day_timestamp,
      gcsi_user_id,
      ud.subscription_id,
      -- they are suspended, the paid_through_date is the schedule_date
     -- or, if it's a current suspension, the current_paid_through_date
      case when status = 'Hold' or status = 'Start/Hold' then
           case when ptd.paid_through_date is  null then
                case when  ud.paid_through_date <= day_timestamp 
                     then ud.paid_through_date
                     else ud.valid_from 
                end
           else ptd.schedule_date
           end
       else coalesce(ptd.paid_through_date,ud.paid_through_date)
       end paid_through_date,
      case when status like 'Trial%' then status
           when status = 'Hold' or status = 'Start/Hold' then 
           case when sh.initiator in  ('CUSTOMER', 'ADMINISTRATIVE')
                then 'Hold'
                else 'Suspended'
           end 
           when cancel_date::date <= dd.day_timestamp::date then 
           case when coalesce(ptd.paid_through_date,ud.paid_through_date) > dd.day_timestamp 
                then 'Lapsed'
                else 'Cancelled'
           end 
           else status
      end status
  from common.user_d ud
  join common.date_d dd
      on ud.valid_from <= dd.day_timestamp AND
         ud.valid_to >= dd.day_timestamp
  left join lateral
       --choose ADMINISTRATIVE hold over payment hold if we have two concurrent holds
      (select  subscription_id, min(initiator) initiator
       from gcsi.subscription_hold h 
       where  h.deleted = 'f'  
         and day_timestamp >= h.start_date::date  
         and (day_timestamp < h.end_date::date or h.end_date is null)
        group by subscription_id) sh
      on ud.subscription_id = sh.subscription_id   
  left join tmp.paid_through_dates ptd
      on ud.subscription_id = ptd.subscription_id and 
         day_timestamp >= ptd.schedule_date::date and
         day_timestamp < ptd.paid_through_date::date
  where day_key >= [$start]
    and day_key < [$end]
  --  and day_timestamp < current_date::date
         -- there are cases where cancel_date is not null and status is still active
         -- because of comps
   and (cancel_date is null or 
        cancel_date >= [$start] or
        ud.paid_through_date >= [$start] or
        status in( 'Active', 'Lapsed', 'Suspended'));


