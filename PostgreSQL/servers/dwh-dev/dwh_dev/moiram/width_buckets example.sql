select   width_bucket(hrs_watched, 0, 15, 30), count(distinct gcsi_user_id), 
min(hrs_watched), max(hrs_watched) 
from
(select  gcsi_user_id, sum(watched/3600) hrs_watched
from cd_video cv
join users_cd_tmp cu 
  on cv.user_id = cu.drupal_user_id
where cosmic_disclosure = 1 AND
      series_title = 'Cosmic Disclosure'
group by  gcsi_user_id) foo
GROUP BY  1 order by 1;

select foo.gcsi_user_id, cd.total_hours_watched tableau_hrs, foo.hrs_watched db_hrs_watched
from 
   (select distinct gcsi_user_id,total_hours_watched
    from cd_cohort) cd 
right join 
(select  gcsi_user_id, sum(watched/3600) hrs_watched
from cd_video cv
join users_cd_tmp cu 
  on cv.user_id = cu.drupal_user_id
where cosmic_disclosure = 1 AND
      series_title = 'Cosmic Disclosure'
group by  gcsi_user_id) foo  
on cd.gcsi_user_id::integer = foo.gcsi_user_id
where foo.hrs_watched >  .5 and foo.hrs_watched < 1
and cd.total_hours_watched is null
order by cd.gcsi_user_id;




