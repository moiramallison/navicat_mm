drop table if exists look_alikes_subs;


create table look_alikes_subs as
(select gcsi_user_id, drupal_user_id, 
           user_behavior_segment,
           subscription_cohort
        from common.current_users
        where status = 'Active' 
          and subscription_start_date <'2015-08-05'
          and user_behavior_segment in ('My Yoga', 'Seeking Truth'));

drop table if exists look_alikes_mtbv;

create table look_alikes_mtbv as 
select drupal_user_id,
round(avg(days_between_views)) mean_time_between_views
from
    (select  drupal_user_id,
        next_view_date - created_date days_between_views
     from
         (select drupal_user_id,
            created_date, 
            lead(created_date) over (partition by drupal_user_id order by created_date) next_view_date
          from
             (select distinct sm3.drupal_user_id, vdc.created_date
             from look_alikes_subs sm3
             join  common.video_daily_cube vdc
                 on sm3.drupal_user_id = vdc.user_id
             where vdc.created_date >= '2015-07-21') t ) t1 ) t2
 group by drupal_user_id;

drop table if exists look_alike_video_views;


create table look_alike_video_views as 
select
    vxc.drupal_user_id,
    user_behavior_segment,
    subscription_cohort,
    coalesce(vxc.num_views,0)                       num_views,
    coalesce(vxc.num_days_watched,0)                num_days_watched,
    coalesce(vxc.hrs_watched,0)                     hrs_watched,
    coalesce(vxc.engagement_ratio,0)                engagement_ratio,
    days_since_last_video_view,
    t.mean_time_between_views
from
    (select drupal_user_id,     
       max(user_behavior_segment)   user_behavior_segment,
       max(subscription_cohort)     subscription_cohort,
       sum(num_views)               num_views, 
       count(distinct created_date) num_days_watched, 
       round(sum(watched)/3600,5)   hrs_watched,
       case when sum(duration) > 0
            then round(sum(watched)/sum(duration),5)                  
            else 0
       end                                                  engagement_ratio,
       min(days_since_video_view) days_since_last_video_view
    from
        (select sm3.drupal_user_id,
					 user_behavior_segment,
					 subscription_cohort,
           vdc.created_date,
           vdc.num_views, 
           vdc.watched, 
           v.duration,
           current_date - vdc.created_date days_since_video_view
        from look_alikes_subs sm3
        join  common.video_daily_cube vdc
            on sm3.drupal_user_id = vdc.user_id
        join common.video_d v
           on vdc.media_nid = v.media_nid
        where vdc.created_date >= '2015-07-21') vv1
    group by drupal_user_id) vxc
join look_alikes_mtbv t
    on vxc.drupal_user_id = t.drupal_user_id;



drop table if exists look_alike_ranks;

create table look_alike_ranks as
select bar.*,
    recent_rank + days_watched_rank + engagement_rank +  mtbv_rank total_rank
from
    (select drupal_user_id,
       look_alike_segment,
       rank() OVER (PARTITION BY look_alike_segment ORDER BY days_since_last_video_view ) AS recent_rank,
       rank() OVER (PARTITION BY look_alike_segment ORDER BY num_days_watched DESC) AS days_watched_rank,
       rank() OVER (PARTITION BY look_alike_segment ORDER BY engagement_ratio DESC) AS engagement_rank,
       rank() OVER (PARTITION BY look_alike_segment ORDER BY mtbv ) AS mtbv_rank
    from    
        (select drupal_user_id,
                case when subscription_cohort = 'Cosmic Disclosure'
            then 'Cosmic Disclosure'
            else user_behavior_segment
                end look_alike_segment,
                days_since_last_video_view,
                num_days_watched,
                engagement_ratio,
                mean_time_between_views mtbv
        from look_alike_video_views
        where days_since_last_video_view <=7 AND
           engagement_ratio >= .7 AND
           num_days_watched >= 20 AND
           mean_time_between_views <= 7) foo ) bar;


       

    

select look_alike_segment, count(1)
from look_alike_ranks
group by look_alike_segment;


drop view yoga_lookalikes;

create view yoga_lookalikes as 
select  distinct du.mail, r.*
from look_alike_ranks r
join drupal.users du on du.uid = r.drupal_user_id
where look_alike_segment = 'My Yoga';

create view st_lookalikes as 
select  distinct du.mail, r.*
from look_alike_ranks r
join drupal.users du on du.uid = r.drupal_user_id
where look_alike_segment = 'Seeking Truth';

create view cd_lookalikes as 
select  distinct du.mail, r.*
from look_alike_ranks r
join drupal.users du on du.uid = r.drupal_user_id
where look_alike_segment = 'Cosmic Disclosure';


