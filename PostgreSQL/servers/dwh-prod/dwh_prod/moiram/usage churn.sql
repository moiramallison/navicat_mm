drop table if exists tmp.usage_churn_subs;


create table tmp.usage_churn_subs as
select t2.*,
   case when paid_through_age >  0 then 1 else 0 end sub_120_day,
   case when paid_through_age >= 118 and
             anniversary_120_date < '2016-02-15'::date and
             churn_120_day = 0 
        then 1 
        else 0
   end sub_395_day,
   case when paid_through_age >= 393 and
             anniversary_395_date < '2016-02-15'::date and
             churn_395_day = 0 
        then 1 
      --  when churn_395_day = 1 then 0
        else 0   -- if they are counted as a churner, make sure they are counted as a sub
   end sub_400_day
from
   (select t1.*,
       case when paid_through_age <= 121 and cancel_age is not null 
            then 1
            else 0
       end   churn_120_day,
       case when paid_through_age > 121 and paid_through_age  <= 396 and cancel_age is not null 
            then 1
            else 0
       end   churn_395_day,
       case --when cancel_age >  399  then 1 
            when paid_through_age > 396 and cancel_age is not null then 1
            else 0
       end   churn_400_day
    from
        (select ud.gcsi_user_id, ud.drupal_user_id, 
           sd.subscription_id, 
           ud.user_behavior_segment, 
           pd.gcsi_plan_id,
           pd.plan_period, 
           sd.start_date::date start_date,
           sd.cancel_date::date cancel_date,
           sd.paid_through_date::date, 
           sd.start_date::date + 120 anniversary_120_date,
           sd.start_date::date + 395 anniversary_395_date,
           sd.cancel_date::date - start_date::date cancel_age,
           paid_through_date::date - start_date::date paid_through_age
        from common.subscription_d sd
        join common.user_dim ud 
           on sd.gcsi_user_id  = ud.gcsi_user_id
        join common.plan_d pd
           on sd.dwh_plan_id = pd.dwh_plan_id
        where status in ('Active','Lapsed','Cancelled')) t1 ) t2
;


drop table if exists tmp.video_views_smry;
drop table if exists tmp.video_views_master;

create table tmp.video_views_smry as 
select
    drupal_user_id,
    period, 
    coalesce(vxc.num_views,0)                       num_views,
    coalesce(vxc.num_days_watched,0)                num_days_watched,
    coalesce(vxc.hrs_watched,0)                     hrs_watched,
    coalesce(vxc.engagement_ratio,0)                engagement_ratio,
    coalesce(vxc.yoga_engagement_ratio,0)           yoga_engagement_ratio,
    coalesce(vxc.sg_engagement_ratio,0)             sg_engagement_ratio,
    coalesce(vxc.st_engagement_ratio,0)             st_engagement_ratio,
    coalesce(vxc.yoga_views,0)                      yoga_views,
    coalesce(vxc.yoga_watched,0)                    yoga_watched,
    coalesce(vxc.sg_views,0)                        sg_views,
    coalesce(vxc.sg_watched,0)                      sg_watched,
    coalesce(vxc.st_views,0)                        st_views,
    coalesce(vxc.st_watched,0)                      st_watched
from
    (select drupal_user_id,
       period,
       sum(num_views)               num_views, 
       count(distinct created_date) num_days_watched, 
       round(sum(watched)/3600,5)   hrs_watched,
       case when sum(duration) > 0
            then round(sum(watched)/sum(duration),5)                  
            else 0
       end                                                  engagement_ratio,
       case when sum(yoga_video_duration) > 0
            then round(sum(yoga_watched)/sum(yoga_video_duration),5)
            else 0
       end                                                  yoga_engagement_ratio,
       case when sum(sg_video_duration) > 0
            then round(sum(sg_watched)/sum(sg_video_duration),5)
            else 0
       end                                                  sg_engagement_ratio,
          case when sum(st_video_duration) > 0
            then round(sum(st_watched)/sum(st_video_duration),5)
            else 0
       end                                                  st_engagement_ratio,
       sum(yoga_views)                                      yoga_views,
       sum(yoga_watched)                                    yoga_watched,
       sum(sg_views)                                        sg_views,
       sum(sg_watched)                                      sg_watched,
       sum(st_views)                                        st_views,
       sum(st_watched)                                      st_watched
    from
    (select vv.*,
            case when view_120_day = 1 then 1
                 when view_395_day =1 then 2
                 when view_400_day = 1 then 3
                 else 0
            end period
    from
        (select vdc.user_id drupal_user_id,
           vdc.created_date,
           vdc.num_views, 
           vdc.watched, 
           vdc.completion_ratio,
           v.duration,
           case when vdc.created_date <= sm3.anniversary_120_date then 1 else 0 end      view_120_day,
           case when vdc.created_date <= sm3.anniversary_395_date then 1 else 0 end      view_395_day,
           case when vdc.created_date > sm3.anniversary_395_date then 1 else 0 end      view_400_day,
           case when fl.level_1 = 1 then num_views else 0 end                           level_1_views,
           case when reporting_segment = 'My Yoga' then num_views else 0 end            yoga_views,
           case when reporting_segment = 'My Yoga' then watched else 0 end              yoga_watched,
           case when reporting_segment = 'My Yoga' then duration else 0 end             yoga_video_duration,
           case when reporting_segment = 'Spiritual Growth' then num_views else 0 end   sg_views,
           case when reporting_segment = 'Spiritual Growth' then watched else 0 end     sg_watched,
           case when reporting_segment = 'Spiritual Growth' then duration else 0 end    sg_video_duration,
           case when reporting_segment = 'Seeking Truth' then num_views else 0 end      st_views,
           case when reporting_segment = 'Seeking Truth' then watched else 0 end        st_watched,
           case when reporting_segment = 'Seeking Truth' then duration else 0 end       st_video_duration
        from tmp.usage_churn_subs sm3
        left join  common.video_daily_cube vdc
            on sm3.drupal_user_id = vdc.user_id
        left join common.video_d v
           on vdc.media_nid = v.media_nid
        left join common.facet_levels fl 
           on v.facet_level_gid = fl.gid
        left join common.guide_d g
           on vdc.media_nid = g.media_nid) vv ) v2
    group by drupal_user_id, period) vxc;

   

create table tmp.video_views_master as
select 
    v1.drupal_user_id,
    v1.num_views                       num_views_1,                             
    v1.hrs_watched                     hrs_watched_1,
    v1.engagement_ratio                engagement_ratio_1,
    v1.yoga_engagement_ratio           yoga_engagement_ratio_1,
    v1.sg_engagement_ratio             sg_engagement_ratio_1,
    v1.st_engagement_ratio             st_engagement_ratio_1,
    v1.yoga_views                      yoga_views_1,
    v1.yoga_watched                    yoga_watched_1,
    v1.sg_views                        sg_views_1,
    v1.sg_watched                      sg_watched_1,
    v1.st_views                        st_views_1,
    v1.st_watched                      st_watched_1,
    v1.num_days_watched               num_days_watched_1,
    v2.num_views                       num_views_2,                             
    v2.hrs_watched                     hrs_watched_2,
    v2.engagement_ratio                engagement_ratio_2,
    v2.yoga_engagement_ratio           yoga_engagement_ratio_2,
    v2.sg_engagement_ratio             sg_engagement_ratio_2,
    v2.st_engagement_ratio             st_engagement_ratio_2,
    v2.yoga_views                      yoga_views_2,
    v2.yoga_watched                    yoga_watched_2,
    v2.sg_views                        sg_views_2,
    v2.sg_watched                      sg_watched_2,
    v2.st_views                        st_views_2,
    v2.st_watched                      st_watched_2,
    v2.num_days_watched               num_days_watched_2,
    v3.num_views                       num_views_3,                             
    v3.hrs_watched                     hrs_watched_3,
    v3.engagement_ratio                engagement_ratio_3,
    v3.yoga_engagement_ratio           yoga_engagement_ratio_3,
    v3.sg_engagement_ratio             sg_engagement_ratio_3,
    v3.st_engagement_ratio             st_engagement_ratio_3,
    v3.yoga_views                      yoga_views_3,
    v3.yoga_watched                    yoga_watched_3,
    v3.sg_views                        sg_views_3,
    v3.sg_watched                      sg_watched_3,
    v3.st_views                        st_views_3,
    v3.st_watched                      st_watched_3,
    v3.guide_day_views                 guide_day_views_3,
    v3.num_days_watched               num_days_watched_3
from sub_master_30_60_90 sm 
left join 
     (select * from video_views_smry where period = 1) v1
     on sm.drupal_user_id = v1.drupal_user_id
left join 
     (select * from video_views_smry where period = 2) v2
     on sm.drupal_user_id = v2.drupal_user_id
left join 
     (select * from video_views_smry where period = 3) v3
     on sm.drupal_user_id = v3.drupal_user_id;

select sub_120_day, churn_120_day, sub_395_day, churn_395_day, sub_400_day, churn_400_day, count(1)
from tmp.usage_churn_subs
group by sub_120_day, churn_120_day, sub_395_day, churn_395_day, sub_400_day, churn_400_day;