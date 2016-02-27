drop table if exists common.video_daily_cube cascade;

create table common.video_daily_cube as 
(select user_id, 
        media_nid,
				extra_nid,
        created_date, 
        player_name,
        sum(watched) watched,
        count(1) num_views,
        max(completion_ratio) completion_ratio,
        case when max(duration) > 0 then
           sum(watched)/max(duration)     --- we're grouping by media nid, so durations should all be same. 
        end   engagement_ratio           --- max is just so we can group by   
from 
    (select vt.user_id, 
         vt.nid media_nid,
         vt.extra_nid, 
         pt.player_name,
         to_timestamp(vt.created)::date created_date,
         case when v.duration > 0 then "position"/v.duration::real end completion_ratio,
         least(watched, v.duration*2) watched,
         v.duration
    from common.video_tmp vt
    left join common.video_d v
      on vt.nid = v.media_nid
    left join common.player_type pt
      on vt."type"=pt.id
    where user_id > 0) t
group by user_id, media_nid, extra_nid, created_date, player_name );



