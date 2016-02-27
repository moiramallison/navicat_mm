insert into common.playlist_activity
(select pa.*,
        current_date snapshot_date
 from 
    (select uid drupal_user_id,
            content_id page_nid,
            to_timestamp(timestamp)::date  add_date,
            weight
     from drupal.flag_content where fid = 2
     except
     (select drupal_user_id, page_nid, add_date, weight
      from common.playlist_activity)) pa);