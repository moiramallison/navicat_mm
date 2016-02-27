drop table if exists tmp.churn_stats;

create table tmp.churn_stats as 
select distinct u.gcsi_user_id, u.drupal_user_id, u.service_name, u.subscription_id, u.subscription_start_date, u.cancel_date, u.user_behavior_segment,
u.cid_channel,  u.plan_name,
       u.cancel_date::date-u.subscription_start_date::date tenure_at_cancel,
       case when u.cancel_date::date - u.paid_through_date::date > 15 then 1 else 0 end long_hold,
       case when u.cancel_date < '2015-10-05':: date then 0 else dd.week_numberinyear end control,
       ud.last_video_view_date,
       ud.last_video_view_nid,
       u.plan_id, u.plan_period, pt.player_name, 
   case when eol.mail is not null then 1 else 0 end eol_device
from common.current_users u
join drupal.users u2 on u.drupal_user_id = u2.uid
join common.date_d dd on cancel_date::date = dd.day_timestamp
left join eol_emails eol on u2.mail = eol.mail
left join 
   (select uid, 
      coalesce(player_name, 'Type Not Defined') player_name
   from 
			(select uid, max(player_type) player_type
			 from 
					(select uid, last_value(player_type) over (partition by uid order by total_watched) player_type
					 from common.user_player_type_segmentation) foo
			 group by uid) bar
      join common.player_type p on bar.player_type = p.id::text) pt
   on u.drupal_user_id = pt.uid
left join common.user_dim ud on u.drupal_user_id = ud.drupal_user_id
where u.cancel_date >= '2015-09-01'::date;


select min(cancel_date) from  tmp.churn_stats;

select control, count(1), count(distinct gcsi_user_id) from tmp.churn_stats
group by control;

select * from
(select cancel_date, count(1) c
from tmp.churn_stats
group by cancel_date) foo
where c > 20
order by cancel_date;

select datediff, count(1) c
from
(select cancel_date::date - paid_through_date::date datediff
from common.current_users
where cancel_date > paid_through_date) foo
group by datediff
order by datediff;


--select * from common.date_d where week_key = '201549';

select count(1) from tmp.churn_stats t
where control in (49,50)
  and last_video_view_date::date > cancel_date::date
  and exists 
     (select subscription_id 
      from common.current_users u
      where t.gcsi_user_id = u.gcsi_user_id
        and u.subscription_start_date > t.cancel_date);

select cid_channel, count(1) c
from tmp.churn_stats 
where  control > 1 
group by cid_channel
order by c desc

select subscription_start_date::date, count(1) c
from tmp.churn_stats 
where  control in (49,50) AND
   tenure_at_cancel < 45 and cid_channel = 'cid:soc:facebook:myyogapaid:c015'
group by subscription_start_date::date
order by  subscription_start_date::date


select week_numberinyear, cid_channel, count(1), sum(churner)
from 
(select  dd.week_numberinyear, 
        case when t.subscription_id is null then 0 else 1 end churner
from common.current_users u
join common.date_d dd
  on u.subscription_start_date::date = dd.day_timestamp
left join tmp.churn_stats t
  on u.subscription_id = t.subscription_id
where (u.cid_channel like '%c015' or u.cid_channel like '%ppd:brand')
  and dd.quarter_key = 20154) foo
group by week_numberinyear, cid_channel
order by week_numberinyear;

select * from common.date_d where week_key = 201544;
      

select control, status, count(1)
from common.current_users u
join tmp.churn_stats cs 
  on u.gcsi_user_id = cs.gcsi_user_id
where tenure_at_cancel > 365
  and cs.plan_period = '1 mon'
group by control, status
order by status, control;


select * from common.user_d where gcsi_user_id in 
(select u1.gcsi_user_id from common.user_d u1
join gcsi.subscription_rollover sr
  on u1.subscription_id = sr.next_subscription_id AND
     u1.plan_period = '1 mon'
join common.user_d u2 
  on sr.prior_subscription_id = u2.subscription_id AND
     u2.plan_period = '1 year');

select distinct plan_period from common.current_users;

select median(days_since_last_video_view)
from
  (select round(cancel_date::date - last_video_view_date::date,0)::INTEGER days_since_last_video_view
  from tmp.churn_stats
  where tenure_at_cancel > 365) foo;
