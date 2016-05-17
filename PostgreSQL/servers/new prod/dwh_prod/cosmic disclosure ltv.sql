select dd1.month_key start_month , 
       dd2.month_key paid_through_month, count(1)
from common.current_users u
join common.date_d dd1
  on u.subscription_start_date::date = dd1.day_timestamp
join common.date_d dd2
  on u.paid_through_date::date = dd2.day_timestamp
where subscription_cohort = 'Cosmic Disclosure'
group by dd1.month_key, dd2.month_key;

select subscription_cohort, count(1)
from common.current_users
group by subscription_cohort;