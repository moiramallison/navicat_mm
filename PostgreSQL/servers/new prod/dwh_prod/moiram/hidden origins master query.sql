-- this query is a concatenation of users_cd_tmp view, cd_video and cd_user_status_snapshot
drop table if exists tmp.ho_users;

create table tmp.ho_users as 
 SELECT ud.drupal_user_id,
    ud.gcsi_user_id,
    sd.subscription_id,
    sd.start_date AS start_date,
    sd.status,
    pd.gcsi_plan_id,
    pd.plan_period,
    case
       when (sd.winback = 'Winback'::text) then 1
       else 0
    end as winback,
    ud.onboarding_parent AS onboarding_segment,
    case
        when cd.subscription_id is not null
        then 1
        else 0
    end as hidden_origins
   from common.user_dim ud
   join common.subscription_d sd 
       on ud.current_subscription = sd.subscription_id
   join common.plan_d pd
       on sd.dwh_plan_id = pd.dwh_plan_id
   left join 
      (select distinct subscription_id 
       from common.cohort_d
       where cohort_name = 'Hidden Origins') cd
       on sd.subscription_id = cd.subscription_id
   where sd.start_date >= '2015-12-26'::date;


drop table if exists tmp.ho_video;

create table tmp.ho_video as
(select vdc.*,
    v.title,
    v.series_title,
    v.episode,
    v.site_segment,
    v.admin_category,
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


drop table if exists tmp.ho_user_series_smry;

-- this is really user behavior summary
create table tmp.ho_user_series_smry as 
(select user_id, 
    count(distinct series) num_series,
    count(distinct ho_episodes) num_ho_episodes,
    sum(ho_hours) ho_hours,
    sum(watched) all_hours
from
    (select user_id,
        case when series_of_interest <> 'Other'  and 
                  series_of_interest <> 'Films & Series' and 
                  series_of_interest <> 'Standalone' and 
									series_of_interest <> 'Cosmic Disclosure'
						 then series_of_interest 
        end series,
        case when series_title = 'Hidden Origins' then watched else 0 end ho_hours,
        case when series_title = 'Hidden Origins' then episode  end ho_episodes,
        watched
    from tmp.ho_video
    where user_id in (select drupal_user_id from tmp.ho_users)) v
group by user_id);

drop table if exists tmp.all_ho_subscriptions;

create table tmp.all_ho_subscriptions as 
select day_timestamp, 
        'All Subscriptions'::text description,
        running_total sub_count
from
(select day_timestamp,  count(1)running_total
 from common.daily_status_snapshot dss
 join tmp.ho_users h
   on dss.subscription_id = h.subscription_id  and 
      h.hidden_origins = 1
group by day_timestamp)foo;


drop table if exists tmp.current_ho_subscriptions;

create table tmp.current_ho_subscriptions as 
(select day_timestamp,
        'Current Active Subscriptions'::text description,
        count(1)
 from common.daily_status_snapshot dss
 join tmp.ho_users h
   on dss.subscription_id = h.subscription_id  and 
      h.hidden_origins = 1
 where dss.status in ('Active',  'Hold')
group by day_timestamp);



/*

custom query

select * from tmp.all_ho_subscriptions
union
select * from tmp.current_ho_subscriptions
order by description, day_timestamp
*/

drop table if exists tmp.ho_churn_summary;

create table tmp.ho_churn_summary as 
select  day_timestamp measure_date, customer_days, total_cancels, total_cancels/customer_days::float churn_rate
from 
(select a.day_timestamp, a.total customer_days, coalesce(c.total ,0) total_cancels
from 
(SELECT  dd.day_timestamp,
    count(distinct dss.subscription_id) as total
    FROM common.daily_status_snapshot dss
    INNER JOIN common.subscription_d sd ON dss.subscription_id = sd.subscription_id
    inner join common.date_d dd on dss.day_timestamp::date = dd.day_timestamp::date
    WHERE dss.paid_through_date >= dd.day_timestamp::date - 1
      and sd.subscription_id in 
          (select subscription_id 
           from common.cohort_d
           where cohort_name = 'Hidden Origins')
    AND dss.status NOT IN ('Hold')
    and dd.day_timestamp >= '20151226' 
    and dd.day_timestamp <= current_date::date - 1
  group by  dd.day_timestamp) a
left join
    (SELECT  dd.day_timestamp,
    count(distinct dss.subscription_id) as total
      FROM common.daily_status_snapshot dss
      INNER JOIN common.subscription_d sd ON sd.subscription_id = dss.subscription_id
      inner join common.date_d dd on dss.day_timestamp::date = dd.day_timestamp::date
      WHERE
      dss.day_timestamp = dd.day_timestamp
      AND dss.paid_through_date >= dd.day_timestamp::date -1
      AND dss.paid_through_date < dd.day_timestamp::date
      AND dss.paid_through_date >= sd.paid_through_date::date - INTERVAL '15 day'
        and sd.subscription_id in 
          (select subscription_id 
           from common.cohort_d
           where cohort_name = 'Hidden Origins')
    group by  dd.day_timestamp
    ) c
on a.day_timestamp = c.day_timestamp)foo;

SELECT 
measure_date,
sum(churn_rate) over(order by measure_date rows 30 PRECEDING) as trailing_30_day_churn_sum
FROM tmp.ho_churn_summary;

select * from tmp.ho_churn_summary;

select distinct plan_period from common.plan_d;