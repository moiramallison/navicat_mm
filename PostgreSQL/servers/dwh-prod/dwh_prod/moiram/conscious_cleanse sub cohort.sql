drop table if exists conscious_cleanse_2;

create table conscious_cleanse_2 as

select t.* 
from
(select 
   coalesce(bar.drupal_user_id::integer, ud1.drupal_user_id) drupal_user_id,
   coalesce(bar.gcsi_user_id, ud2.gcsi_user_id) gcsi_user_id,
   bar.opted_out,
   bar.dno_day14
from 
   (select
      coalesce(foo.drupal_user_id::integer, du.uid) drupal_user_id,
      foo.email_address,
      foo.gcsi_user_id,
      foo.opted_out,
      foo.dno_day14
   from
        (SELECT cc.drupal_user_id , cc.email_address, gu.id gcsi_user_id,
          case when opted_out is null then 0 else 1 end opted_out,
          case when dno_day14 is null then 0 else 1 end dno_day14
         FROM conscious_cleanse cc
         left join gcsi.users gu 
           on cc.customer_id = gu.uuid or
              cc.email_address = gu.email) foo
    left join drupal.users du on foo.email_address = du.mail) bar
left join common.user_dim ud1
  on bar.gcsi_user_id = ud1.gcsi_user_id
left join common.user_dim ud2
  on bar.drupal_user_id = ud2.drupal_user_id) t
where drupal_user_id > 0 and gcsi_user_id is not null;


select count(1), count(distinct drupal_user_id), 
count(distinct gcsi_user_id) from conscious_cleanse_2;


select count(1) from conscious_cleanse_2 where drupal_user_id > 0 and gcsi_user_id 
is not null;


drop table if exists cc_engagement;

create table cc_engagement as 
select * from current_engagement
where drupal_user_id in 
   (select drupal_user_id from conscious_cleanse_2);

drop table if exists cc_guides;

create table cc_guides as
(select user_id drupal_user_id, g.*
 from common.video_daily_cube vdc
 join common.guide_d g
   on vdc.media_nid = g.media_nid and 
      vdc.extra_nid = g.guide_day_nid
 where user_id in 
    (select drupal_user_id from moiram.conscious_cleanse_2));

select guide_title, count(distinct drupal_user_id), count(1)
from cc_guides
group by guide_title;

drop table cc_titles;

create table cc_titles as
(select user_id drupal_user_id, v.*,
   case when vdc.media_nid in     
        (select media_nid from common.guide_d)
        then 1 else 0 
   end guide_video
 from common.video_daily_cube vdc
 join common.video_d v
   on vdc.media_nid = v.media_nid
 where user_id in 
    (select drupal_user_id from moiram.conscious_cleanse_2)
   and vdc.created_date >= '20160101');


select t1.* from 
(select t.*,
  row_number() over (partition by site_segment order by c desc) rn
from
(select site_segment, title, series_title, count(1) c
from cc_titles
group by site_segment, title, series_title) t ) t1
where rn <= 10;

select status, count(1)
from common.subscription_d sd
join conscious_cleanse_2 cc
on sd.gcsi_user_id = cc.gcsi_user_id
group by status;
