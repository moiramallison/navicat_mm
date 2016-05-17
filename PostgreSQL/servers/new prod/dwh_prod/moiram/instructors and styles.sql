drop table if exists avg_yoga_completion;


create table avg_yoga_completion as
(with v as 
    (select v1.title, 
            (current_date::date - created_date)/30 video_age,
            page_nid, 
            duration, 
            media_nid, facet_teacher_gid
     from common.video_d v1
     join drupal.node n on v1.page_nid = n.nid
     where admin_category in ('Yoga', 'Fitness')
      and n.status = 1
      and feature = 'Feature')
select v.title,
       v.video_age,
       v.page_nid,
       v.duration,
v.facet_teacher_gid,
       count(1)total_views, 
       sum(watched) total_watched
from common.video_tmp vt 
join  v on vt.nid = v.media_nid
where  vt.created > 1420070400
group by v.title, v.video_age,v.page_nid, v.duration, v.facet_teacher_gid);

create table yoga_teachers as 
(select page_nid, fg.name as teacher
 from common.video_d v
 join common.facet_groups fg
   on v.facet_teacher_gid = fg.gid
     join drupal.node n on v.page_nid = n.nid
     where admin_category in ('Yoga', 'Fitness')
      and n.status = 1
      and feature = 'Feature')

create table yoga_styles as 
(select page_nid, fg.name as style
 from common.video_d v
 join common.facet_groups fg
   on v.facet_style_gid = fg.gid
     join drupal.node n on v.page_nid = n.nid
     where admin_category in ('Yoga', 'Fitness')
      and n.status = 1
      and feature = 'Feature')

select style, sum(total_watched/3600) hrs_watched
from avg_yoga_completion ayc
join yoga_styles ys on ayc.page_nid = ys.page_nid
group by style
order by style;



