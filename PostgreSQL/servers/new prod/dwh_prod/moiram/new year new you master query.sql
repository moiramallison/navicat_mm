

drop table if exists tmp.users_nyny;

create table tmp.users_nyny as 
select  ud.drupal_user_id,
    ud.gcsi_user_id,   
    sd.subscription_id,
    sd.start_date,
    sd.paid_through_date,
    sd.cancel_date,
    sd.end_date,
    sd.status,
    pd.gcsi_plan_id,
    pd.plan_period,
    ud.onboarding_parent AS onboarding_segment,
    ud.user_behavior_segment,
    current_date - ud.last_video_view_date::date days_since_last_video_view,
    current_date - ud.last_login::date days_since_last_login
from common.user_dim ud
join common.subscription_d sd 
  on ud.current_subscription = sd.subscription_id
join common.plan_d pd
  on sd.dwh_plan_id = pd.dwh_plan_id
where sd.start_date >= '2015-12-26'::date;

drop table if exists tmp.nyny_cohorts;

create table tmp.nyny_cohorts as
select un.gcsi_user_id, un.subscription_id,
   case 
   when un.user_behavior_segment = 'My Yoga'
        then coalesce(c.cohort_name, 'Other: MY')
   when un.user_behavior_segment = 'Spiritual Growth'
        then coalesce(c.cohort_name, 'Other: SG')
   when un.user_behavior_segment = 'Seeking Truth'
        then coalesce(c.cohort_name, 'Other: ST')
   else c.cohort_name
   end cohort_name
from tmp.users_nyny un
left join
    (select gcsi_user_id, subscription_id, cohort_name
    from
      (select 
           gcsi_user_id, 
           subscription_id, 
           cohort_name,
           row_number() over w as rn
       from common.cohort_d
       where cohort_name <> 'Commit to You'
       window w as (
         partition by gcsi_user_id, subscription_id
         order by rule_id
       rows between unbounded preceding
       and unbounded following)) foo
    where rn = 1 ) c
  on un.subscription_id = c.subscription_id;

drop table if exists tmp.nyny_video;

create table tmp.nyny_video as
(select drupal_user_id,
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
    v.reporting_segment,
    case when gd.media_nid is not null
         then 1 
         else 0
    end guide_day_video,
    gd.guide_title,
    gd.guide_day,
    case when gdv.field_guide_day_videos_nid is NULL
         then 0
         else 1
    end bonus_video 
from tmp.users_nyny un
left join common.video_daily_cube vdc
   on un.drupal_user_id = vdc.user_id and 
      vdc.created_date > '2015-12-26'::date
left join common.video_d v on vdc.media_nid = v.media_nid
left join common.guide_d gd on vdc.media_nid = gd.media_nid
left join 
      (select distinct field_guide_day_videos_nid
       from drupal.content_field_guide_day_videos) gdv 
    on v.page_nid = gdv.field_guide_day_videos_nid);

drop table if exists tmp.nyny_video_cube;

create table tmp.nyny_video_cube as 
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
    coalesce(vxc.st_watched,0)                      st_watched,
    coalesce(vxc.guide_day_views,0)                 guide_day_views,
    coalesce(vxc.bonus_video_views,0)               bonus_video_views
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
       sum(st_watched)                                      st_watched,
       sum(guide_day_views)                                 guide_day_views,
       sum(bonus_video)                                     bonus_video_views
    from
        (select un. drupal_user_id,
           nv.created_date,
           nv.num_views, 
           nv.watched, 
           nv.completion_ratio,
           case when completion_ratio >= .25 then 1 else 0 end engaged_view,
           nv.duration,
           nv.guide_day_video guide_day_views,
           nv.bonus_video,
           case when reporting_segment = 'My Yoga' then num_views else 0 end            yoga_views,
           case when reporting_segment = 'My Yoga' then watched else 0 end              yoga_watched,
           case when reporting_segment = 'My Yoga' then duration else 0 end             yoga_video_duration,
           case when reporting_segment = 'Spiritual Growth' then num_views else 0 end   sg_views,
           case when reporting_segment = 'Spiritual Growth' then watched else 0 end     sg_watched,
           case when reporting_segment = 'Spiritual Growth' then duration else 0 end    sg_video_duration,
           case when reporting_segment = 'Seeking Truth' then num_views else 0 end      st_views,
           case when reporting_segment = 'Seeking Truth' then watched else 0 end        st_watched,
           case when reporting_segment = 'Seeking Truth' then duration else 0 end       st_video_duration    
        from tmp.users_nyny un
        left join tmp.nyny_video nv  
           on un.drupal_user_id = nv.drupal_user_id ) v   
    group by drupal_user_id, engaged_view) vxc;

insert into  tmp.nyny_video_cube (drupal_user_id, engaged_view, num_views)
(select drupal_user_id, 1, 0
 from tmp.users_nyny
 except select drupal_user_id,engaged_view,0
from  tmp.nyny_video_cube);




drop table if exists tmp.ho_video;
--specifically for Hidden Origins
-- has a different start date
create table tmp.ho_video as
(select vdc.*,
    v.title,
    v.series_title,
    v.episode,
    v.reporting_segment,
    v.duration, 
    -- right now designed to be used only by Hidden Origins
    case when v.series_title = 'Disclosure'
              then v.series_title || ' ' || season
         when v.series_title in ('Disclosure', 'Wisdom Teachings','Beyond Belief','Open Minds','Healing Matrix',
                   'Arcanum','Secrets to Health','Spirit Talk' ,'On the Road With Lilou' ,
                   'Eleventh House','Mind Shift','Inspirations', 'Cosmic Disclosure', 'Hidden Origins')
             then v.series_title
         when v.site_segment in('My Yoga', 'Spiritual Growth', 'Film & Series') then v.site_segment
         when series_title is null then 'Standalone'
         else 'Other ST Series'
    end series_of_interest
from common.video_daily_cube vdc
join common.video_d v on vdc.media_nid = v.media_nid
where vdc.created_date > '2016-01-11'::date);

/*   re-work this for HO
drop table if exists user_series_smry;

-- this is really user behavior summary
create table user_series_smry as 
(select user_id, 
    count(distinct series) num_series,
    count(distinct cd_episodes) num_cd_episodes,
    sum(cd_hours) cd_hours,
    sum(watched) all_hours
from
    (select user_id,
        case when series_of_interest <> 'Other'  and 
                  series_of_interest <> 'Films & Series' and 
                  series_of_interest <> 'Standalone' and 
                  series_of_interest <> 'Cosmic Disclosure'
                         then series_of_interest 
        end series,
        case when series_title = 'Cosmic Disclosure' then watched else 0 end cd_hours,
        case when series_title = 'Cosmic Disclosure' then episode  end cd_episodes,
        watched
    from cd_video
    where user_id in (select drupal_user_id from users_cd_tmp)) v
group by user_id);
*/


drop table if exists tmp.all_ny_subscriptions;

create table tmp.all_ny_subscriptions as 
select day_timestamp, cohort_name, 
        'All Subscriptions'::text description,
        running_total sub_count
from
(select day_timestamp, cohort_name, count(1)running_total
 from common.daily_status_snapshot dss
 join tmp.nyny_cohorts c
   on dss.subscription_id = c.subscription_id
group by day_timestamp, cohort_name)foo;


drop table if exists tmp.current_ny_subscriptions;

create table tmp.current_ny_subscriptions as 
(select day_timestamp, cohort_name,
        'Current Active Subscriptions'::text description,
        count(1)
 from common.daily_status_snapshot dss
 join tmp.nyny_cohorts c
   on dss.subscription_id = c.subscription_id
 where dss.status in ('Active',  'Hold')
group by day_timestamp, cohort_name);


drop table if exists tmp.nyny_churn_summary;

create table tmp.nyny_churn_summary as 
select  cohort_name, day_timestamp measure_date, customer_days, total_cancels, total_cancels/customer_days::float churn_rate
from 
(select a.cohort_name, a.day_timestamp, a.total customer_days, coalesce(c.total ,0) total_cancels
from 
(SELECT  dd.day_timestamp,
     cd.cohort_name,
    count(distinct dss.subscription_id) as total
    FROM common.daily_status_snapshot dss
    INNER JOIN common.subscription_d sd ON dss.subscription_id = sd.subscription_id
    inner join common.date_d dd on dss.day_timestamp::date = dd.day_timestamp::date
    left join tmp.nyny_cohorts cd 
      on sd.subscription_id = cd.subscription_id
    WHERE dss.paid_through_date >= dd.day_timestamp::date - 1
    AND dss.status NOT IN ('Hold')
    and dd.day_timestamp >= '20151226' 
    and dd.day_timestamp <= current_date::date - 1
  group by  dd.day_timestamp, cohort_name) a
left join
    (SELECT  dd.day_timestamp,
     cohort_name, 
    count(distinct dss.subscription_id) as total
      FROM common.daily_status_snapshot dss
      INNER JOIN common.subscription_d sd ON sd.subscription_id = dss.subscription_id
      inner join common.date_d dd on dss.day_timestamp::date = dd.day_timestamp::date
    left join tmp.nyny_cohorts cd 
      on sd.subscription_id = cd.subscription_id
      WHERE
      dss.day_timestamp = dd.day_timestamp
      AND dss.paid_through_date >= dd.day_timestamp::date -1
      AND dss.paid_through_date < dd.day_timestamp::date
      AND dss.paid_through_date >= sd.paid_through_date::date - INTERVAL '15 day'
    group by  dd.day_timestamp, cohort_name
    ) c
on a.day_timestamp = c.day_timestamp)foo;

SELECT 
cohort_name, measure_date,
sum(churn_rate) over(partition by cohort_name order by measure_date rows 30 PRECEDING) as trailing_30_day_churn_sum
FROM tmp.nyny_churn_summary;

/*

custom query

select * from tmp.all_cd_subscriptions
union
select * from tmp.current_cd_subscriptions
order by description, day_timestamp
*/


/*
select * from tmp.nyny_video
where user_id  in 
(select drupal_user_id from tmp.users_nyny
where gcsi_user_id in (select gcsi_user_id from tmp.nyny_cohorts where cohort_name is null));


select series_of_interest, created_date, sum(round(watched/3600,3)) hours_watched
from 
   (select 
      case when series_title in ('Wisdom Teachings' ,'Disclosure','Beyond Belief','Open Minds', 'Cosmic Disclosure','Hidden Origins')
           then series_title
           else 'Other Series'
      end series_of_interest,
      created_date::date created_date,
      watched
    from tmp.nyny_video ny
    join tmp.users_nyny nu 
        on ny.user_id = nu .drupal_user_id 
    join tmp.nyny_cohorts NC 
       on nu .gcsi_user_id = nc .gcsi_user_id and 
          cohort_name in ('Seeking Truth Marathon', 'Cosmic Disclosure',
                          'Hidden Origins', 'Other: ST')) foo
group by series_of_interest, created_date;
*/

select user_behavior_segment, cohort_name, count(1)
from tmp.users_nyny un 
join tmp.nyny_cohorts nc 
  on un.subscription_id = nc.subscription_id
group by user_behavior_segment, cohort_name
order by user_behavior_segment, cohort_name;


select user_behavior_segment, count(1)
from tmp.users_nyny un 
group by user_behavior_segment;


select gcsi_user_id from tmp.users_nyny where drupal_user_id in 
(select drupal_user_id from tmp.users_nyny
except 
select user_id from common.video_tmp
where created > 1451081188);

select count(distinct drupal_user_id) from tmp.nyny_video;

select cohort_name, status, count(1)
from tmp.nyny_cohorts ud
join subscription_d sd
  on ud.subscription_id = sd.subscription_id
group by cohort_name, status;

select cohort_name, engaged_view, num_days_watched, count(1)
from tmp.nyny_video_cube vc
right join tmp.users_nyny un 
   on vc.drupal_user_id = un.drupal_user_id
right join tmp.nyny_cohorts nc 
   on nc.gcsi_user_id = un.gcsi_user_id
group by cohort_name, engaged_view, num_days_watched;
