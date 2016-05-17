with v as 
    (select admin_category,
            v1.title, 
            v1.series_title,
            v1.content_origin,
            (current_date::date - created_date)/30 video_age,
            page_nid, 
            duration, 
            media_nid
     from common.video_d v1
     join drupal.node n
        on v1.page_nid = n.nid
     where admin_category  not in (  'Yoga', 'Fitness')
      and n.status = 1
      and feature = 'Feature')
select v.admin_category,
       v.title,
       v.series_title,
       v.content_origin,
       v.video_age,
       v.page_nid node_id,
       v.duration,
       sum(num_views) total_views, 
       sum(watched) total_watched
from common.video_daily_cube vt 
join  v on vt.media_nid = v.media_nid
where  vt.created_date > '20140101'::date
group by v.admin_category, 
       v.series_title,
       v.content_origin,v.title, v.video_age,v.page_nid, v.duration;

