drop table if exists tmp.sub_master;

create table tmp.sub_master
as
   select dss.subscription_id,
          dss.gcsi_user_id,  
          ud.drupal_user_id, 
          [$ivend]::date ivend,
          ud.user_behavior_segment,
          [$ivend]::date - sd.start_date::date subscription_age
  from common.daily_status_snapshot dss 
  join common.user_dim ud on dss.gcsi_user_id = ud.gcsi_user_id
  join common.subscription_d sd on dss.subscription_id = sd.subscription_id
where dss.day_timestamp = [$ivend] 
  and dss.status = 'Active';

drop table if exists tmp.sub_targets;

create table tmp.sub_targets
as 
  select sd.subscription_id,
         sd.gcsi_user_id,
         sd.drupal_user_id,
         [$ivend]::date ivend,
         cancel_date,
         1 hard_cancel_flag
  from common.subscription_d sd
  where cancel_date >= [$dvbegin]
    and cancel_date <= [$dvend]
    and cancel_date <= paid_through_date;

drop table if exists tmp.video_views_master;


-- this is a little odd, but right now I think I want 
-- days since last video view as of the cancel date if they have one
create table tmp.video_views_master
as 
select sm .drupal_user_id,
    [$ivend]::date ivend,
    coalesce(v4.days_since_last_video_view, v2.days_since_last_video_view) days_since_last_video_view
from
    tmp.sub_master sm 
left join
    (select drupal_user_id,
        [$ivend]::date - video_view_date days_since_last_video_view
     from
        (select distinct  sm .drupal_user_id,
               max(created_date) video_view_date
         from tmp.sub_master sm 
         left join common.video_daily_cube vdc
            on sm .drupal_user_id = vdc.user_id
         where engagement_ratio > .25 
           and created_date <= [$ivend]
         group by sm.drupal_user_id) v1 ) v2
   on sm .drupal_user_id = v2.drupal_user_id
left join
     (select drupal_user_id,
        [$dvend]::date - video_view_date days_since_last_video_view
      from
        (select  st .drupal_user_id,
               max(created_date) video_view_date
         from tmp.sub_targets st 
         left join common.video_daily_cube vdc
            on st .drupal_user_id = vdc.user_id
         where engagement_ratio > .25 
           AND created_date <= [$dvend]::date
         group by st.drupal_user_id) v3 ) v4 
   on v2.drupal_user_id = v4.drupal_user_id;
  
drop table if exists tmp.sfss;

create table tmp.sfss
as 
select sm .*,
        vv.days_since_last_video_view,
        coalesce(st .hard_cancel_flag,1) churn_flag
 from tmp.sub_master sm 
 left join tmp.video_views_master vv
    on sm.drupal_user_id = vv.drupal_user_id and 
       sm.ivend = vv.ivend
 left join tmp.sub_targets st 
    on sm.drupal_user_id = st.drupal_user_id and 
       sm.ivend = st.ivend;
        
