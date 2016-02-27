drop table if exists cd_video;

create table cd_video as
(select vdc.*,
    v.series_title,
    v.episode,
    case when v.series_title = 'Disclosure'
              then v.series_title || ' ' || season
         when v.series_title in ('Disclosure', 'Wisdom Teachings','Beyond Belief','Open Minds','Healing Matrix',
                   'Arcanum','Secrets to Health','Spirit Talk' ,'On the Road With Lilou' ,
                   'Eleventh House','Mind Shift','Inspirations', 'Cosmic Disclosure')
             then v.series_title
         when v.site_segment = 'Film & Series' then v.site_segment
         when series_title is null then 'Standalone'
         else 'Other'
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

