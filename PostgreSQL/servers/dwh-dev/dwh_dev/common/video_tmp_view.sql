drop view if exists common.video_tmp;

create  view common.video_tmp
as (select 
        case when uid = 0 then client_uid else uid end user_id,
        id, 
        nid, 
        extra_nid,
        created, 
			  position,
        join_ms,
        watched,
        paused,
        seekcount, type
    from drupal.smfplayer_user_history
where ign = 0 
  and bad =0 
  and watched > 15);


    