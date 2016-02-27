with v as 
    (select v1.title, 
            (current_date::date - created_date)/30 video_age,
            page_nid, 
            duration, 
            media_nid,
            teacher
     from common.video_d v1
     join drupal.node n on v1.page_nid = n.nid
     left join 
				(select  nid, string_agg(name, ',') teacher 
				from tmp.facets_tmp
				where facet_name = 'teacher'
				group by nid) t
      on v1.page_nid = t.nid
     where admin_category  =  'Yoga'
      and n.status = 1
      and feature = 'Feature')
select v.title,
       v.video_age,
       v.page_nid,
       v.duration,
       v.teacher,
       sum(num_views) total_views, 
       sum(watched) total_watched
from common.video_daily_cube vt 
join  v on vt.media_nid = v.media_nid
where  vt.created_date > '20150101'::date
group by v.title, v.video_age,v.page_nid, v.duration, v.teacher;

