drop table if exists tmp.closed_beta;

create table tmp.closed_beta as
select t.*,
       case when r <= .06 then 1 else 0 end closed_beta
from
(select u.gcsi_user_id, u.drupal_user_id, 
		   u.email_address, u.user_behavior_segment,
       u.engagement_level,
       case when tenure >= 0 and tenure <=120 then '0-120'
            when tenure > 120 and tenure <= 395 then '121-395'
            when tenure > 395 then '396+'
       end tenure_group,
       random() r
from common.user_dim u
join (select subscription_id,
      current_date::date - start_date::date tenure
      from common.subscription_d
      where status = 'Active') s 
   on u.current_subscription = s.subscription_id
where u.engagement_level in ('Low', 'Medium', 'High'))  t;

select user_behavior_segment, tenure_group, engagement_level, count(1), sum(closed_beta)
from tmp.closed_beta
group by user_behavior_segment, tenure_group, engagement_level;