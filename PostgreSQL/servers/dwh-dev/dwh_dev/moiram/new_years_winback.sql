create table new_years_winback as
(select  du.mail,
        campaign_dept,
        user_behavior_segment,
        subscription_start_date,
        cancel_date 
from  common.current_users cu 
join drupal.users du on 
   cu.drupal_user_id = du.uid
where subscription_start_date >= '2014-11-24'::date
  and subscription_start_date <= '2015-02-28'::date 
  and cu.status = 'Cancelled'
  and paid_through_date <= current_date)