drop table if exists common.first_video_engagement;

create table common.first_video_engagement as
select * from
	(select user_id drupal_user_id,
	   first_value(vt.id) over w as suh_id,
	   first_value(vt.nid) over w as nid,
	   first_value(vt.extra_nid) over w as extra_nid,
	   first_value(to_timestamp(created)) over w as created_ts,
	   first_value(site_segment) over w as site_segment,
	   first_value(series_title) over w as series_title,
	   row_number() over w as rn
	from common.video_tmp vt
	join common.video_d v
	  on vt.nid = v.media_nid and 
	     v.duration > 0
	where watched/duration > .25
	window w as (
	partition by user_id
	order by vt.id
	rows between unbounded preceding
        and unbounded following)) foo
where rn = 1;

alter table common.first_video_engagement owner to dwadmin;