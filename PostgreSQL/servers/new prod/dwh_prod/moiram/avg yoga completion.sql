drop table if exists avg_video_completion;

create table avg_video_completion as
(select v.admin_category,
       v.title, 
       v.page_nid node_id,
       v.duration,
       sum(num_views) total_views, 
       sum(watched) total_watched
from common.video_daily_cube vt 
join common.video_d v
on vt.media_nid = v.media_nid
join drupal.node n
  on v.page_nid = n.nid
where admin_category not in ( 'Yoga', 'Fitness')
  and n.status = 1
  and feature = 'Feature'
 and  vt.created_date > '20150101'::date
group by v.admin_category, v.title, v.page_nid, v.duration);

select * from avg_video_completion;