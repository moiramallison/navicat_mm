drop table if exists eol_emails;

create table eol_emails
 as
select distinct du.mail
from common.video_daily_cube vdc
join common.current_users ud on ud.drupal_user_id = vdc.user_id
join drupal.users du on du.uid = vdc.user_id
where ud.status = 'Active' and
  player_name in 
    ('Android FireTV',
'Android Kindle',
'Android Nook',
'MS Xbox',
'Panasonic Tv'
'Sony PS3',
'Sony PS Vita'
'Sony TV');