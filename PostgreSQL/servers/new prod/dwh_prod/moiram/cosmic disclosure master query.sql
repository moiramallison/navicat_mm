-- this query is a concatenation of users_cd_tmp view, cd_video and cd_user_status_snapshot
drop table if exists users_cd_tmp;

create table users_cd_tmp as 
 SELECT ud.drupal_user_id,
    ud.gcsi_user_id,
    sd.start_date AS start_date,
    sd.status,
    pd.gcsi_plan_id,
    case
       when (sd.winback = 'Winback'::text) then 1
       else 0
    end as winback,
    ud.onboarding_parent AS onboarding_segment,
    case
        when cd.gcsi_user_id is not null
        then 1
        else 0
    end as cosmic_disclosure
   from common.user_dim ud
   join common.subscription_d sd 
       on ud.current_subscription = sd.subscription_id
   join common.plan_d pd
       on sd.dwh_plan_id = pd.dwh_plan_id
   left join 
      (select distinct gcsi_user_id 
       from common.cohort_d
       where cohort_name = 'Cosmic Disclosure') cd
       on ud.gcsi_user_id = cd.gcsi_user_id
   where sd.start_date >= '2015-07-01'::date;


drop table if exists cd_video;

create table cd_video as
(select vdc.*,
    v.title,
    v.series_title,
    v.episode,
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
where vdc.created_date > '2015-07-21'::date);


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

drop table if exists tmp.cd_user_status_snapshot;

create table tmp.cd_user_status_snapshot
as select gcsi_user_id,
status,
case when status in ('Hold', 'Start/Hold') 
     then dd.day_timestamp::date - valid_from::date
     else 0
end hold_age,
valid_from,
subscription_cohort,
subscription_start_date,
cancel_date,
paid_through_date,
dd.day_timestamp
from common.user_d ud
join common.date_d dd
    on ud.valid_from <= dd.day_timestamp AND
       ud.valid_to >= dd.day_timestamp
where day_key >= '20150701' 
  and day_key <= '20160102'
  and (cancel_date is null or cancel_date >= '20150701'::date)
  and (subscription_end_date is null or subscription_end_date >= '20150701'::date);

drop table if exists tmp.all_cd_subscriptions;

create table tmp.all_cd_subscriptions as 
select day_timestamp, 
        'All Cosmic Disclosure Subscriptions'::text description,
        sum(running_total) sub_count
from
(select day_timestamp,
    case when subscription_start_date::date <= day_timestamp then 1 
         else 0 
    end running_total
    from tmp.cd_user_status_snapshot
    where subscription_cohort = 'Cosmic Disclosure') t1
group by day_timestamp;


drop table if exists tmp.current_cd_subscriptions;
create table tmp.current_cd_subscriptions as 
(select day_timestamp, 
        'Current Active Subscriptions'::text description,
        count(1)
from tmp.cd_user_status_snapshot
    where subscription_cohort = 'Cosmic Disclosure'
      and (status = 'Active' or 
           status = 'Hold' and day_Timestamp::date - valid_from < 15 OR
           status = 'Cancelled' and paid_through_date >= day_timestamp
         )
group by day_timestamp);

/*

custom query

select * from tmp.all_cd_subscriptions
union
select * from tmp.current_cd_subscriptions
order by description, day_timestamp
*/