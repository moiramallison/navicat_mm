drop table if exists common.subscription_d;

create table common.subscription_d  as
select 
    subscription_id,
    drupal_user_id,
    gcsi_user_id,
    subscription_start_date  start_date,
    next_review_date,
    paid_through_date,
    cancel_date,
    subscription_end_date    end_date,
    case when status = 'Hold' or status = 'Start/Hold' then 
         case when paid_through_date >= current_date::date
              then 'Hold'
              else 'Suspended'
         end 
         when cancel_date <= current_date::date then 
         case when paid_through_date > current_date::date 
              then 'Lapsed'
              else 'Cancelled'
         end 
         else status
    end status,
    pd.dwh_plan_id
from common.user_d ud
join common.plan_d pd
   on ud.plan_id = pd.gcsi_plan_id and ud.segment_id = pd.segment_id
where id in 
   (select  id
    from common.user_d
    where valid_from >= current_date::date
      and valid_to <= current_date::date);



