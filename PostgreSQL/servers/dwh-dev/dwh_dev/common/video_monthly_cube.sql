drop table if exists common.video_monthly_cube cascade;

create table common.video_monthly_cube as 
(select user_id, 
        media_nid,
        month_key, 
        player_name,
        sum(watched) watched,
        count(1) num_views,
        sum(completion) video_completion        
from 
    (select vt.user_id, 
         vt.nid media_nid,
         pt.player_name,
         dd.month_key, 
         case when vt.position > (v.duration*.9) 
              then 1
              else 0
         end completion,
         least(watched, v.duration*2) watched
    from common.video_tmp vt
    left join common.video_d v
      on vt.nid = v.media_nid
    left join common.player_type pt
      on vt."type"=pt.id
    join common.date_d dd
      on dd.day_key = common.to_date_d(to_timestamp(vt.created)::timestamp)
    where user_id > 0) t
group by user_id, media_nid, month_key, player_name );





