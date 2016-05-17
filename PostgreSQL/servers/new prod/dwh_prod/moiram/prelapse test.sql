drop table if exists tmp.retention_test;

create table tmp.retention_test as 
select tst.*,
       to_timestamp(du.access) last_access_date,
       case when version_group in ('A','B') then
           case when ud.last_video_view_date > '20160311'::date and
                     ud.last_video_view_date < '20160319'::date
                then 1 else 0 
           end 
           else 
           case when ud.last_video_view_date > '20160311'::date and
                     ud.last_video_view_date < '20160325'::date
                then 1 else 0 
           end 
       end video_view,
       case when version_group in ('A','B') then
            case when du.access  > 1457683200 and
                      du.access  < 1458374400
                 then 1 else 0
            end 
            else 
            case when du.access  > 1457683200 and
                      du.access  < 1458892800
            then 1 else 0
            end
       end login,
       sd.status
from
   (select "CUSTOMER_ID"                               customer_uuid,
           "ENGAGEMENT_LEVEL"                          engagement_level,
           "EMAIL_ADDRESS"                             email_address,
           coalesce("DRUPAL_ID"::integer,gu.uid)       drupal_user_id,
           split_part("TESTGROUP", '_', 1)             engagement_group,
           split_part("TESTGROUP", '_', 2)             plan_group,
           split_part("TESTGROUP", '_', 3)             version_group
    from moiram."Test_Lifecycle_TestGroups" t
    left join drupal.gcsi_users gu 
        on t."CUSTOMER_ID" = gu.user_uuid)tst
left join common.user_dim ud
    on tst.drupal_user_id = ud.drupal_user_id
left join drupal.users du
    on tst.drupal_user_id = du.uid
left join common.subscription_d sd
    on ud.current_subscription = sd.subscription_id;

select email_address,   drupal_user_id, engagement_level, engagement_group,   plan_group, version_group,  last_access_date, video_view, login,  status
from tmp.retention_test;