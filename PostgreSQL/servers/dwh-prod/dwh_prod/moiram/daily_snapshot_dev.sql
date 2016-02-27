
 truncate table common.daily_status_y2014q1;

      
 insert into common.daily_status_y2014q1
 select dd.day_timestamp,
     gcsi_user_id,
     ud.subscription_id,
     -- they are suspended, the paid_through_date is the schedule_date
    -- or, if it's a current suspension, the current_paid_through_date
     case when status = 'Hold' or status = 'Start/Hold' and 
                         sh.initiator = 'PAYMENT'
          then coalesce(ptd.schedule_date,ud.paid_through_date)
          else coalesce(ptd.paid_through_date,ud.paid_through_date)
          end paid_through_date,
     case when status = 'Hold' or status = 'Start/Hold' then 
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
        day_timestamp > ptd.schedule_date and
        day_timestamp <= ptd.paid_through_date
 where day_key >= 20140101
     and day_key < 20140401
  -- and day_timestamp < current_date::date
        -- there are cases where cancel_date is not null and status is still active
        -- because of comps
   and (cancel_date is null or 
        cancel_date >= '20140101' or
        ud.paid_through_date >= '20140101' or
        status in( 'Active', 'Lapsed', 'Suspended'));

select count(distinct day_timestamp) from common.daily_status_snapshot;