/*

drop  table if exists fve;



create  table fve as 
select drupal_user_id, minutes, suh_id, suh.nid
from
    (select user_id drupal_user_id,0 minutes, min(id) suh_id
    from common.video_tmp
    group by user_id
    union
    select user_id drupal_user_id,1, min(id) suh_id
    from common.video_tmp
    where watched > 60
    group by user_id
    union
    select user_id drupal_user_id,3, min(id) suh_id
    from common.video_tmp
    where watched > 180
    group by user_id
    union
    select user_id drupal_user_id,5, min(id) suh_id
    from common.video_tmp
    where watched > 300
    group by user_id
    union
    select user_id drupal_user_id,8, min(id) suh_id
    from common.video_tmp
    where watched > 480
    group by user_id
    union
    select user_id drupal_user_id,10, min(id) suh_id
    from common.video_tmp
    where watched > 600
    group by user_id) foo
 join smfplayer_user_history suh on suh_id = suh.id;

create  table fve_users as select distinct drupal_user_id from fve;

*/

drop table if exists fve_min_bins;

create table fve_min_bins as 
select fu.drupal_user_id,
   case when (f0.suh_id = f1.suh_id  or f1.suh_id is null) and
             (f0.suh_id = f3.suh_id  or f3.suh_id is null) and 
             (f0.suh_id = f5.suh_id  or f5.suh_id is null) and 
             (f0.suh_id = f8.suh_id  or f8.suh_id is null) and
             (f0.suh_id = f10.suh_id or f10.suh_id is null)
        then 0
        when (f1.suh_id = f3.suh_id  or f3.suh_id is null) and 
             (f1.suh_id = f5.suh_id  or f5.suh_id is null) and 
             (f1.suh_id = f8.suh_id  or f8.suh_id is null) and
             (f1.suh_id = f10.suh_id or f10.suh_id is null)
        then 1
        when (f3.suh_id = f5.suh_id  or f5.suh_id is null) and
             (f3.suh_id = f8.suh_id  or f8.suh_id is null) and
             (f3.suh_id = f10.suh_id or f10.suh_id is null)
        then 3
        when (f5.suh_id = f8.suh_id  or f8.suh_id is null) and
             (f5.suh_id = f10.suh_id or f10.suh_id is null)
        then 5
        when f8.suh_id = f10.suh_id or f10.suh_id is null
        then 8
        when f10.suh_id is not null then 10
        end fve_min_bin,
   case when f10.suh_id is not null then 10
        when f8.suh_id is not null then 8
        when f5.suh_id is not null then 5
        when f3.suh_id is not null then 3
        when f1.suh_id is not null then 1
        else 0
   end max_non_null_bin
from fve_users fu
left join (select * from fve where minutes =0) f0 on fu.drupal_user_id = f0.drupal_user_id
left join (select * from fve where minutes =1) f1 on fu.drupal_user_id = f1.drupal_user_id
left join (select * from fve where minutes =3) f3 on fu.drupal_user_id = f3.drupal_user_id
left join (select * from fve where minutes =5) f5 on fu.drupal_user_id = f5.drupal_user_id
left join (select * from fve where minutes =8) f8 on fu.drupal_user_id = f8.drupal_user_id
left join (select * from fve where minutes =10) f10 on fu.drupal_user_id = f10.drupal_user_id;select max_non_null_bin, count(1)
from fve_min_bins
group by max_non_null_bin
order by max_non_null_bin;

select fve_min_bin, count(1)
from fve_min_bins
group by fve_min_bin
order by fve_min_bin;

select max_non_null_bin, fve_min_bin, count(1)
from fve_min_bins
group by max_non_null_bin,fve_min_bin
order by max_non_null_bin,fve_min_bin;

select count(distinct drupal_user_id) from fve_min_bins;



/*
create table first_video_engagement as 
select fu.drupal_user_id,
   f0.nid first_qualified_view,
   f1.nid first_1min_view,
   f3.nid first_3min_view,
   f5.nid first_5min_view,
   f8.nid first_8min_view,
   f10.nid first_10min_view
from fve_users fu
left join (select * from fve where minutes =0) f0 on fu.drupal_user_id = f0.drupal_user_id
left join (select * from fve where minutes =1) f1 on fu.drupal_user_id = f1.drupal_user_id
left join (select * from fve where minutes =3) f3 on fu.drupal_user_id = f3.drupal_user_id
left join (select * from fve where minutes =5) f5 on fu.drupal_user_id = f5.drupal_user_id
left join (select * from fve where minutes =8) f8 on fu.drupal_user_id = f8.drupal_user_id
left join (select * from fve where minutes =10) f10 on fu.drupal_user_id = f10.drupal_user_id;
*/

