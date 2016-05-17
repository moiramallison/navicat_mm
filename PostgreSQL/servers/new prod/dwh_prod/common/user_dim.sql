refresh materialized view common.last_video_engagement with data;

drop table if exists tmp.previous_user_dim;

create table tmp.previous_user_dim as 
select * from common.user_dim;

drop table if exists common.user_dim;

create table common.user_dim as 
select ud.drupal_user_id,
    ud.gcsi_user_id,
    du.mail email_address, 
    ud.user_start_date,
    ud.user_end_date,
    ud.service_name,
    ud.subscription_id current_subscription,
    to_timestamp(le.end) entitlement_end_date,
    ud.cid_channel acquisition_channel,
    ud.onboarding_segment,
    ud.onboarding_parent,
    case when pu.user_behavior_segment = ud.user_behavior_segment
         then pu.prev_user_behavior_segment
         else pu.user_behavior_segment
    end prev_user_behavior_segment,
    case when pu.user_behavior_segment = ud.user_behavior_segment
         then pu.ubs_change_date
         else current_date
    end ubs_change_date,
    ud.user_behavior_segment,
    case when pu.player_segment = ps.player_segment
         then pu.prev_player_segment
         else pu.player_segment
    end prev_player_segment,
    case when pu.player_segment = ps.player_segment
         then pu.player_change_date
         else current_date
    end player_change_date,
    ps.player_segment,
    lvv.last_video_view_sid,
    lvv.last_video_view_nid,
    lvv.last_video_view_date,
    to_timestamp(du.login) last_login
from common.current_users ud
left join tmp.previous_user_dim pu 
    on ud.gcsi_user_id = pu.gcsi_user_id
left join
    (select * from drupal.lemonade_entitlements
     where leid in 
        (select max(leid) leid
         from drupal.lemonade_entitlements
         group by uid)) le
    on ud.drupal_user_id = le.uid
left join 
    (select pts.uid, pt.player_name player_segment
     from common.player_type pt
     join 
         (select uid, player_type,
              row_number() over (partition by uid order by engagement_ratio desc) as rn
          from common.user_player_type_segmentation
          where player_type <> 'Type Not Defined') pts
        on pt.id = pts.player_type::integer and 
           pts.rn = 1) ps
     on ud.drupal_user_id = ps.uid
left join 
    (select lve.drupal_user_id, 
        lve.suh_id last_video_view_sid,
        suh.nid last_video_view_nid, 
        to_timestamp(suh.created) last_video_view_date
     from common.last_video_engagement lve
     join drupal.smfplayer_user_history suh
       on lve.suh_id = suh.id) lvv
   on ud.drupal_user_id = lvv.drupal_user_id
left join drupal.users du
    on ud.drupal_user_id = du.uid;

ALTER TABLE common.user_dim OWNER TO "dwadmin";




