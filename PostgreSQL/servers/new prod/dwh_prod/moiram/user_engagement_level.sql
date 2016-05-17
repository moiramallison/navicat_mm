-- now that we have subscription_d built, update the engagement_level column in user_dim


with engagement_level as 
(select gcsi_user_id,
    case when e.engagement_level is null then 
         case when subscription_age <= 90 
              then 'Low' 
              else 'Dormant'
         end
         else e.engagement_level
    end engagement_level
from
    (select ud.gcsi_user_id,
            date_part('day',current_date - ud.last_video_view_date) days_since_last_video_view,
            date_part('day',current_date - sd.start_date) subscription_age,
            ud.user_behavior_segment
     from common.user_dim ud
     join common.subscription_d sd
        on ud.current_subscription = sd.subscription_id) u
left join common.engagement_level e
    on u.user_behavior_segment = e.user_behavior_segment and
       u.subscription_age >= e.tenure_cat_low and
       u.subscription_age <= e.tenure_cat_high and
       u.days_since_last_video_view >= days_since_last_video_view_low and
       u.days_since_last_video_view <= days_since_last_video_view_high)
update common.user_dim u set engagement_level = e.engagement_level
from  engagement_level e
where u.gcsi_user_id = e.gcsi_user_id;