delete from tmp.sub_master where ivend = [$ivend]::date;

insert into tmp.sub_master
   (select dss.subscription_id,
          dss.gcsi_user_id,  
          ud.drupal_user_id,
          [$ivend]::date ivend,
          ud.user_behavior_segment,
          [$ivend]::date - sd.start_date::date subscription_age
  from common.daily_status_snapshot dss 
  join common.user_dim ud on dss.gcsi_user_id = ud.gcsi_user_id
  join common.subscription_d sd on dss.subscription_id = sd.subscription_id
where dss.day_timestamp = [$ivend] 
  and dss.status = 'Active');

select ivend, count(1)
from tmp.sub_master
group by ivend;

delete from tmp.sub_targets where ivend = [$ivend]::date;

insert into tmp.sub_targets
  (select sd.subscription_id,
         sd.gcsi_user_id,
         sd.drupal_user_id,
         [$ivend]::date ivend,
         sd.cancel_date::date, 
         1 hard_cancel_flag
  from common.subscription_d sd
  where cancel_date >= [$dvbegin]
    and cancel_date <= [$dvend]
    and cancel_date <= paid_through_date);

select ivend, count(1)
from tmp.sub_targets
group by ivend;


drop table if exists tmp.video_views_smry;

create table tmp.video_views_smry
as 






delete from tmp.video_views_master where ivend = [$ivend]::date;




-- this is a little odd, but right now I think I want 
-- days since last video view as of the cancel date if they have one
insert into tmp.video_views_master
(select sm .drupal_user_id,
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
where sm.ivend = [$ivend]);
  
delete from tmp.sfss_bak where ivend = [$ivend]::date;

insert into tmp.sfss_bak
(select sm .*,
        vv.days_since_last_video_view,
        coalesce(st .hard_cancel_flag,0) churn_flag
 from tmp.sub_master sm 
 left join tmp.video_views_master vv
    on sm.drupal_user_id = vv.drupal_user_id and 
       sm.ivend = vv.ivend
 left join tmp.sub_targets st 
    on sm.drupal_user_id = st.drupal_user_id and 
       sm.ivend = st.ivend
where sm.ivend = [$ivend]);

drop table if exists tmp.sfss;

CREATE TABLE "tmp"."sfss" (
"id" serial,
"subscription_id" int8,
"gcsi_user_id" int8,
"drupal_user_id" int8,
"ivend" date,
"user_behavior_segment" text COLLATE "default",
"subscription_age" int4,
"days_since_last_video_view" int4,
"churn_flag" int4
)
WITH (OIDS=FALSE)
;


insert into tmp.sfss(subscription_id, gcsi_user_id, drupal_user_id, ivend, 
    user_behavior_segment, subscription_age,days_since_last_video_view,churn_flag)
(select * from tmp.sfss_bak);
        
