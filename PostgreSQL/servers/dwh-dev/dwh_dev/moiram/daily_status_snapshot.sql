drop table if exists common.daily_status_snapshot;

create table common.daily_status_snapshot
as select dd.day_timestamp,
  gcsi_user_id,
  subscription_id,
  paid_through_date,
    case when status = 'Hold' or status = 'Start/Hold' then 
     case when paid_through_date >= dd.day_timestamp
          then 'Hold'
          else 'Suspended'
     end 
     when cancel_date <= dd.day_timestamp then 
     case when paid_through_date > dd.day_timestamp 
          then 'Lapsed'
          else 'Cancelled'
     end 
     else status
    end status
from common.user_d ud
join common.date_d dd
    on ud.valid_from <= dd.day_timestamp AND
       ud.valid_to >= dd.day_timestamp
where day_key >= 20140101
  and day_timestamp <= current_date::date
  and (cancel_date is null or cancel_date >= '20140101'::date)
  and (subscription_end_date is null or subscription_end_date >= '20140101'::date);


