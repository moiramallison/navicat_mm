drop table if exists common.guide_opt_ins;

create table common.guide_opt_ins as 
select g.*,
       dd.month_key
from 
    (select uid drupal_user_id,
        content_id guide_nid,
        to_timestamp(timestamp)::date opt_in_date
     from drupal.flag_content where fid = 11) g
join common.date_d dd
  on g.opt_in_date = dd.day_timestamp::date;
