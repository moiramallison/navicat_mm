drop table if exists tmp_valued_hours;

create table tmp_valued_hours as
select user_id, vdc.media_nid, watched, v.series_title
from common.video_daily_cube vdc
join common.user_dim ud on vdc.user_id = ud.drupal_user_id
join common.video_d v on vdc.media_nid = v.media_nid and 
      v.feature = 'Feature' AND
      v.content_origin like 'GTV%' AND
      v.series_title  is not null
where ud.user_behavior_segment = 'Seeking Truth'
  and vdc.created_date > '20160321'::date
  and vdc.created_date < '20160420'::date;


select num_other_series, count(1)
from
(select vh1.user_id,
       du.mail,
       least(coalesce(vh2.num_other_series, 0),4) num_other_series
FROM
   (select distinct user_id 
    from tmp_valued_hours 
    where series_title = 'Cosmic Disclosure') vh1
join drupal.users du on vh1.user_id = du.uid
left  JOIN
   (select user_id, count(distinct series_title) num_other_series
    from tmp_valued_hours
    where series_title <> 'Cosmic Disclosure'
    group by user_id) vh2
on vh1.user_id = vh2.user_id
   where vh1.user_id in (select drupal_user_id from common.subscription_d where status = 'Active')) foo
group by num_other_series
order by num_other_series;
