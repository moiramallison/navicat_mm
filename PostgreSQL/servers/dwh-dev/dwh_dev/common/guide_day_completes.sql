drop table if exists common.guide_day_completes;

create table common.guide_day_completes as 
select g.*,
       dd.month_key
from 
    (select uid drupal_user_id,
        content_id guide_day_nid,
        to_timestamp(timestamp)::date complete_day
     from drupal.flag_content where fid = 6) g
join common.date_d dd
  on g.complete_day = dd.day_timestamp::date;
