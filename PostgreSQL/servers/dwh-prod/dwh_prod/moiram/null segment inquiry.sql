select segment_name, count(1) from common.user_behavior_segmentation
 group by segment_name;

select user_behavior_segment, count(1)
from common.user_dim
where user_end_date is null
group by user_behavior_segment;

drop table if exists null_segment_analysis;

create table null_segment_analysis
as 
select ud.drupal_user_id, gcsi_user_id, current_subscription subscription_id,
case when vt.user_id is not null then 1 else 0 end  video_tmp,
case when suh.uid is not null then 1 else 0 end suh,
case when fc.uid is not null then 1 else 0 end guide,
case when pl.num_videos > 5 then 1 else 0 end playlist
from common.user_dim ud
left join (select distinct user_id
           from common.video_tmp) vt
    on ud.drupal_user_id = vt.user_id
left join 
			(select uid from drupal.smfplayer_user_history
       UNION
       select client_uid from drupal.smfplayer_user_history) suh
   on ud.drupal_user_id = suh.uid
left join (select distinct uid
           from drupal.flag_content
           where fid = 11) fc
       on ud.drupal_user_id = fc.uid
left join 
     (select drupal_user_id, count(1) num_videos
      from common.playlist_activity
      group by drupal_user_id) pl 
     on ud.drupal_user_id = pl.drupal_user_id
where user_end_date is null 
  and user_behavior_segment is NULL;

select video_tmp, suh, guide, playlist, 
count(1)
from null_segment_analysis
group by video_tmp, suh, guide, playlist;


