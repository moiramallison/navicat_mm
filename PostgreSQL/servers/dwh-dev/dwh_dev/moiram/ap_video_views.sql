drop table if exists annual_plan_subs;

create table annual_plan_subs as 
    select id,
    drupal_user_id,
    gcsi_user_id,
    user_start_date,
    user_end_date,
    subscription_id,
    subscription_start_date,
    paid_through_date,
    next_review_date,
    cancel_date,
    subscription_end_date,
    activity,
    status,
    entitled,
    plan_id,
    segment_id,
    plan_name,
    plan_period,
    cid_channel,
    campaign_dept
from common.current_users where plan_period = '1 year'::interval
    and subscription_start_date <= '2015-04-30';


drop table if exists ap_video_views_smry;

create table ap_video_views_smry as 
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
    coalesce(vxc.level_1_views,0)                   level_1_views,
    coalesce(vxc.level_1_watched,0)                 level_1_watched,
    coalesce(vxc.level_2_views,0)                   level_2_views,
    coalesce(vxc.level_2_watched,0)                 level_2_watched,
    coalesce(vxc.level_3_views,0)                   level_3_views,
    coalesce(vxc.level_3_watched,0)                 level_3_watched,
    coalesce(vxc.duration_lt_15_views,0)            duration_lt_15_views,
    coalesce(vxc.duration_lt_15_watched,0)          duration_lt_15_watched,
    coalesce(vxc.duration_15_to_29_views,0)         duration_15_to_29_views,
    coalesce(vxc.duration_15_to_29_watched,0)       duration_15_to_29_watched,
    coalesce(vxc.duration_30_to_59_views,0)         duration_30_to_59_views,
    coalesce(vxc.duration_30_to_59_watched,0)       duration_30_to_59_watched,
    coalesce(vxc.duration_ge_60_views,0)            duration_ge_60_views,
    coalesce(vxc.duration_ge_60_watched,0)          duration_ge_60_watched,
    coalesce(vxc.yoga_views,0)                      yoga_views,
    coalesce(vxc.yoga_watched,0)                    yoga_watched,
    coalesce(vxc.sg_views,0)                        sg_views,
    coalesce(vxc.sg_watched,0)                      sg_watched,
    coalesce(vxc.st_views,0)                        st_views,
    coalesce(vxc.st_watched,0)                      st_watched,
    coalesce(vxc.guide_day_views,0)                 guide_day_views,
    coalesce(vxc.first_quartile_complete_rat,0)     first_quartile_complete_rat,
    coalesce(vxc.second_quartile_complete_rat,0)    second_quartile_complete_rat,
    coalesce(vxc.third_quartile_complete_rat,0)     third_quartile_complete_rat,
    coalesce(vxc.fourth_quartile_complete_rat,0)    fourth_quartile_complete_rat,
    days_since_last_video_view
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
       sum(level_1_views)                                   level_1_views, 
       sum(level_1_watched)                                 level_1_watched,   
       sum(level_2_views)                                   level_2_views,   
       sum(level_2_watched)                                 level_2_watched,
       sum(level_3_views)                                   level_3_views,
       sum(level_3_watched)                                 level_3_watched,
       sum(duration_lt_15_views)                            duration_lt_15_views,
       sum(duration_lt_15_watched)                          duration_lt_15_watched, 
       sum(duration_15_to_29_views)                         duration_15_to_29_views,
       sum(duration_15_to_29_watched)                       duration_15_to_29_watched,  
       sum(duration_30_to_59_views)                         duration_30_to_59_views,
       sum(duration_30_to_59_watched)                       duration_30_to_59_watched,  
       sum(duration_ge_60_views)                            duration_ge_60_views,
       sum(duration_ge_60_watched)                          duration_ge_60_watched,
       sum(yoga_views)                                      yoga_views,
       sum(yoga_watched)                                    yoga_watched,
       sum(sg_views)                                        sg_views,
       sum(sg_watched)                                      sg_watched,
       sum(st_views)                                        st_views,
       sum(st_watched)                                      st_watched,
       sum(guide_day_views)                                 guide_day_views,
       sum(first_quartile_complete_rat)                     first_quartile_complete_rat,
       sum(second_quartile_complete_rat)                    second_quartile_complete_rat,
       sum(third_quartile_complete_rat)                     third_quartile_complete_rat,
       sum(fourth_quartile_complete_rat)                    fourth_quartile_complete_rat,
       min(days_since_viewed)																days_since_last_video_view
    from
    (select vv.*,
            case when days_since_viewed <=30 then 1
                 when days_since_viewed <=60 then 2
                 when days_since_viewed <=90 then 3
                 else 0
            end period,
            case when completion_ratio <= .25 then 1 else 0 end     first_quartile_complete_rat,
            case when completion_ratio >  .25 
                  and completion_ratio <= .5 then 1 else 0 end      second_quartile_complete_rat,
            case when completion_ratio > .5 
                  and completion_ratio <= .75 then 1 else 0 end     third_quartile_complete_rat,
            case when completion_ratio > .75 then 1 else 0 end      fourth_quartile_complete_rat
    from
        (select vdc.user_id drupal_user_id,
           vdc.created_date,
           vdc.num_views, 
           vdc.watched, 
           vdc.completion_ratio,
           v.duration,
           current_date - vdc.created_date::date																				days_since_viewed,
           case when fl.level_1 = 1 then num_views else 0 end                           level_1_views,
           case when fl.level_1 = 1 then watched else 0 end                             level_1_watched,
           case when fl.level_2 = 1 then num_views else 0 end                           level_2_views,
           case when fl.level_2 = 1 then watched else 0 end                             level_2_watched,
           case when fl.level_3 = 1 then num_views else 0 end                           level_3_views,
           case when fl.level_3 = 1 then watched else 0 end                             level_3_watched,
           case when facet_duration = '0-14 minutes' then num_views else 0 end          duration_lt_15_views,
           case when facet_duration = '0-14 minutes' then watched else 0 end            duration_lt_15_watched, 
           case when facet_duration = '15-29 minutes' then num_views else 0 end         duration_15_to_29_views,
           case when facet_duration = '15-29 minutes' then watched else 0 end           duration_15_to_29_watched,  
           case when facet_duration = '30-59 minutes' then num_views else 0 end         duration_30_to_59_views,
           case when facet_duration = '30-59 minutes' then watched else 0 end           duration_30_to_59_watched,  
           case when facet_duration = '60+ minutes' then num_views else 0 end           duration_ge_60_views,
           case when facet_duration = '60+ minutes' then watched else 0 end             duration_ge_60_watched,
           case when reporting_segment = 'My Yoga' then num_views else 0 end            yoga_views,
           case when reporting_segment = 'My Yoga' then watched else 0 end              yoga_watched,
           case when reporting_segment = 'My Yoga' then duration else 0 end             yoga_video_duration,
           case when reporting_segment = 'Spiritual Growth' then num_views else 0 end   sg_views,
           case when reporting_segment = 'Spiritual Growth' then watched else 0 end     sg_watched,
           case when reporting_segment = 'Spiritual Growth' then duration else 0 end    sg_video_duration,
           case when reporting_segment = 'Seeking Truth' then num_views else 0 end      st_views,
           case when reporting_segment = 'Seeking Truth' then watched else 0 end        st_watched,
           case when reporting_segment = 'Seeking Truth' then duration else 0 end       st_video_duration,
           case when guide_day is null then 0 else 1 end                                guide_day_views
        from annual_plan_subs sm3
        left join  common.video_daily_cube vdc
            on sm3.drupal_user_id = vdc.user_id
        left join common.video_d v
           on vdc.media_nid = v.media_nid
        left join common.facet_levels fl 
           on v.facet_level_gid = fl.gid
        left join common.guide_d g
           on vdc.media_nid = g.media_nid) vv ) v2
    group by drupal_user_id, period) vxc;