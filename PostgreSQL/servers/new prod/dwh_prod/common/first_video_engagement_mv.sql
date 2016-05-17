create materialized view first_video_engagement as
select user_id drupal_user_id, min(id) suh_id
from common.video_tmp
where watched > 600
group by user_id;