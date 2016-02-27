drop table if exists avg_yoga_completion

create table avg_yoga_completion as
(select v.title, 
       v.page_nid,
       v.duration,
       sum(num_views) total_views, 
       sum(watched) total_watched
from common.video_daily_cube vt 
join common.video_d v
on vt.nid = v.media_nid
join drupal.node n
  on v.page_nid = n.nid
where admin_category = 'Yoga'
  and n.status = 1
  and feature = 'Feature'
 and  vt.created_date > '20150101'::date
group by v.title, v.page_nid, v.duration);