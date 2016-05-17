drop table if exists common.guide_opt_ins;

create or replace view common.guide_opt_ins as
select distinct on (drupal_user_id, guide_title, opt_in_date) 
    ud.drupal_user_id, 
    ud.gcsi_user_id, 
    gd.guide_nid, 
    gd.guide_title, 
    to_timestamp(fc.timestamp) opt_in_date
from common.user_dim ud
join drupal.flag_content fc
    on ud.drupal_user_id = fc.uid and fc.fid = 11
join common.guide_d gd
    on fc.content_id = gd.guide_nid
order by drupal_user_id, guide_title, opt_in_date;

drop table if exists common.guide_day_completes;

create or replace view common.guide_day_completes as
select gcsi_user_id, 
    ud.drupal_user_id, 
    gd.guide_nid, 
    gd.guide_title, 
    gd.guide_day_nid,
    gd.guide_day,
    to_timestamp(fc.timestamp) completion_date
from common.user_dim ud
join drupal.flag_content fc
on ud.drupal_user_id = fc.uid and fc.fid = 6
join common.guide_d gd
on fc.content_id = gd.guide_day_nid;