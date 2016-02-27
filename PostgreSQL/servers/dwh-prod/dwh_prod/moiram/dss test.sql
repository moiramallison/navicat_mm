
select --ud.user_behavior_segment, 
count(distinct ud.drupal_user_id) 
from common.user_dim ud
right join common.subscription_d sd on
ud.current_subscription = sd.subscription_id
where sd.status = 'Lapsed' or sd.status = 'Active'
--and sd.start_date  < (date '01-20-16' - interval '1 year')
--group  by ud.user_behavior_segment;

select count(distinct gcsi_user_id) from common.user_d
where valid_from <= '20151231' AND
      valid_to >= '20151231' and 
      cancel_date < '20160101' AND
      paid_through_date >= '20160101' and 
      paid_through_date > cancel_date;

select count(distinct gcsi_user_id) from daily_status_snapshot
where day_timestamp = '20151231' 
  and status = 'Lapsed';

select distinct gcsi_user_id from common.user_d
where valid_from <= '20151231' AND
      valid_to >= '20151231' and 
      cancel_date < '20160101' AND
      paid_through_date >= '20160101' and 
      paid_through_date > cancel_date
except
select distinct gcsi_user_id from daily_status_snapshot
where day_timestamp = '20151231' 
  and status = 'Lapsed';



select 
