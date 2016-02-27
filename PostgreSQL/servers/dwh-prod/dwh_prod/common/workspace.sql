select * from pg_stat_activity
WHERE state = 'active';

SELECT pg_cancel_backend(11261);

drop materialized view first_video_engagement;

create materialized view first_video_engagement as 
select coalesce(uid, client_uid) drupal_user_id,
        id suh_id, 
        nid media_nid
from drupal.smfplayer_user_history
where id in 
   (select min(id) 
    from drupal.smfplayer_user_history
    where bad = 0 and 
          ign = 0 and
          watched > 0 and
          uid <> 0
    group by uid);

select day_timestamp, status, count(1) from moiram.daily_status_snapshot
group by day_timestamp, status
order by day_timestamp;

grant select on moiram to pjackson;

select * from moiram.daily_status_snapshot

select * from common.user_dim 
where cancel_date::date = '20151001'::date 
and paid_through_date > '20151001'::date
and valid_from 
