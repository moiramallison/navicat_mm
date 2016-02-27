drop table if exists tmp.st395_sub_master;

create table tmp.st395_sub_master as 
select foo.subscription_id, 
       foo.gcsi_user_id,
       drupal_user_id,
       subscription_age,
       acquisition_channel,
       ct.department marketing_channel,
       case when dss2.paid_through_date::date >= '20160115' and 
                 dss2.paid_through_date::date <= '20160214' and
                 dss2.status <> 'Hold'
            then 1
            else 0
       end churner
from 
   (select dss.subscription_id,
          dss.gcsi_user_id,  
          ud.drupal_user_id, 
          ud.user_behavior_segment,
					ud.acquisition_channel,
          '20160114'::date - sd.start_date::date subscription_age
   from common.daily_status_snapshot dss 
   join common.user_dim ud on dss.gcsi_user_id = ud.gcsi_user_id
   join common.subscription_d sd on dss.subscription_id = sd.subscription_id
   where dss.day_timestamp = '20160114'
     and dss.paid_through_date >=  '20160114') foo
   left join common.daily_status_snapshot dss2 
      on foo.subscription_id = dss2.subscription_id and 
         dss2.day_timestamp = '20160214'
left join common.campaign_tracking ct
   on foo.acquisition_channel = ct.reported_channel
where user_behavior_segment = 'Seeking Truth'
  and subscription_age >=395;


drop table if exists tmp.st395_video;

create table tmp.st395_video as
(select sf.drupal_user_id,
        vdc.media_nid,
				vdc.extra_nid,
        vdc.created_date, 
        vdc.player_name,
        coalesce(vdc.watched,0) watched,
        coalesce(vdc.num_views,0) num_views,
        vdc.engagement_ratio completion_ratio,
           case when engagement_ratio >= .25 then 1 else 0 end engaged_view,
    v.title,
    v.series_title,
    v.episode,
    v.duration, 
    v.reporting_segment
from tmp.st395_sub_master sf
left join common.video_daily_cube vdc
   on sf.drupal_user_id = vdc.user_id and 
      vdc.created_date > '2015-01-01'::date
left join common.video_d v on vdc.media_nid = v.media_nid);

drop table if exists tmp.st395_video_cube;

create table tmp.st395_video_cube as 
select
    drupal_user_id,
    engaged_view,  
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
       engaged_view,
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
        (select nv. drupal_user_id,
           nv.created_date,
           nv.num_views, 
           nv.watched, 
           nv.completion_ratio,
           case when completion_ratio >= .25 then 1 else 0 end engaged_view,
           nv.duration,
           case when reporting_segment = 'My Yoga' then num_views else 0 end            yoga_views,
           case when reporting_segment = 'My Yoga' then watched else 0 end              yoga_watched,
           case when reporting_segment = 'My Yoga' then duration else 0 end             yoga_video_duration,
           case when reporting_segment = 'Spiritual Growth' then num_views else 0 end   sg_views,
           case when reporting_segment = 'Spiritual Growth' then watched else 0 end     sg_watched,
           case when reporting_segment = 'Spiritual Growth' then duration else 0 end    sg_video_duration,
           case when reporting_segment = 'Seeking Truth' then num_views else 0 end      st_views,
           case when reporting_segment = 'Seeking Truth' then watched else 0 end        st_watched,
           case when reporting_segment = 'Seeking Truth' then duration else 0 end       st_video_duration    
        from tmp.st395_video nv  ) v   
    group by drupal_user_id, engaged_view) vxc;

drop table if exists  tmp.st395_video_series;
create table tmp.st395_video_series as
(select vdc.*,
    v.title,
    v.series_title,
    v.season,
    v.episode,
    case when v.series_title = 'Disclosure'
              then v.series_title || ' ' || season
         when v.series_title in ('Disclosure', 'Wisdom Teachings','Beyond Belief','Open Minds','Healing Matrix',
                   'Arcanum','Secrets to Health','Spirit Talk' ,'On the Road With Lilou' ,
                   'Eleventh House','Mind Shift','Inspirations', 'Cosmic Disclosure')
             then v.series_title
         when v.site_segment in('My Yoga', 'Spiritual Growth', 'Film & Series') then v.site_segment
         when series_title is null then 'Standalone'
         else 'Other ST Series'
    end series_of_interest
from common.video_daily_cube vdc
join common.video_d v on vdc.media_nid = v.media_nid
where vdc.created_date > '2015-01-01'::date);