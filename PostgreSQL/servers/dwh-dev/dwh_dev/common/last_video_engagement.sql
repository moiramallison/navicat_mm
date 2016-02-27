create materialized view last_video_engagement as
select user_id drupal_user_id, max(id) suh_id
from common.video_tmp
--where watched > 600
group by user_id;