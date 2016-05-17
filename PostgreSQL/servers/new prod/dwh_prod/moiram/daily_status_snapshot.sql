 truncate table daily_status_y2016q1;

      
 insert into moiram.daily_status_y2016q1
 select dd.day_timestamp,
     gcsi_user_id,
     ud.subscription_id,
     -- they are suspended, the paid_through_date is the schedule_date
    -- because it hasn't settled yet
     case when status = 'Hold' or status = 'Start/Hold' and 
                         sh.initiator = 'PAYMENT'
          then ptd.schedule_date
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
 left join gcsi.subscription_hold sh 
     on ud.subscription_id = sh.subscription_id and 
        sh.deleted = 'f' and 
        day_timestamp >= sh.start_date::date and 
        (day_timestamp < sh.end_date::date or sh .end_date is null)
 left join tmp.paid_through_dates ptd
     on ud.subscription_id = ptd.subscription_id and 
        day_timestamp > ptd.schedule_date and
        day_timestamp <= ptd.paid_through_date
 where day_key >= 20160101
   and day_timestamp < current_date::date
        -- there are cases where cancel_date is not null and status is still active
        -- because of comps
   and (cancel_date is null or 
        cancel_date >= '20151201'::date or 
        status in( 'Active', 'Lapsed'))
   and (subscription_end_date is null or 
       subscription_end_date >= '20151201'::date);

select count(distinct day_timestamp) from moiram.daily_status_snapshot;