drop materialized view if exists common.video_views;

create  materialized view common.video_views
as (select 
        case when suh.uid = 0 then suh.client_uid else suh.uid end user_id,
        suh.id, 
        suh.nid, 
        suh.extra_nid,
        suh.created, 
        suh.join_ms,
        suh.watched,
        suh.paused,
        suh.seekcount, 
        pt.player_name,
        case when (duration > 0) and (watched/duration >= .25) then 1 else 0 end engaged_view
from drupal.smfplayer_user_history suh
join v_node vn on suh.nid = vn.nid
left join common.player_type pt on suh.type = pt.id
where ign = 0 
  and bad = 0 
  and watched > 15
  and watched < duration * 2);

create or replace view common.drop table if exists common.video_daily_cube cascade;
as select * from common.video_views where user_id > 0;

create or replace view common.engaged_video_views
as select * from  common.qualified_video_views 
   where  engaged_view = 1;
   
create or replace view common.qualifed_guide_views as 
select gd.guide_nid, gd.guide_title, qvv.*  
from common.qualifed_video_views qvv
join common.guide_d gd on qvv.nid = gd.media_nid and qvv.extra_nid = gd.guide_day_nid;

create or replace view common.engaged_guide_views as 
select gd.guide_nid, gd.guide_title, qvv.*  
from common.engaged_video_views qvv
join common.guide_d gd on qvv.nid = gd.media_nid and qvv.extra_nid = gd.guide_day_nid;
  


alter table common.video_daily_cube owner to moiram;
drop table if exists common.video_daily_cube cascade; 
drop materialized view if exists common.video_daily_cube cascade;


create materialized view common.video_daily_cube as 
(select user_id, 
        media_nid,
        extra_nid,
        created_date, 
        player_name,
        sum(watched) watched,
        count(1) num_qualified_views,
        sum(engaged_view) num_engaged_views
from 
    (select user_id, 
         nid media_nid,
         extra_nid, 
         player_name,
         watched,
         engaged_view,
         to_timestamp(created)::date created_date
    from common.qualified_video_views )qv
group by user_id, media_nid, extra_nid, created_date, player_name );
