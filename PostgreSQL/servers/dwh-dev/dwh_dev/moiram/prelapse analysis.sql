drop table if exists tmp.retention_test;

create table tmp.retention_test as 
select tst.*,
       dss1.subscription_id, 
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
       case when sr.prior_subscription_id is null then dss2.status else 'Rollover' end status,
       floor(( sd.paid_through_date::date - '20160318'::date )/30) pd_thru_bucket
from
   (select "CUSTOMER_ID"                               customer_uuid,
           "ENGAGEMENT_LEVEL"                          engagement_level,
           "EMAIL_ADDRESS"                             email_address,
           coalesce("DRUPAL_ID"::integer,gu.uid)       drupal_user_id,
           split_part("TESTGROUP", '_', 1)             behavior_segment,
           split_part("TESTGROUP", '_', 2)             engagement_group,
           split_part("TESTGROUP", '_', 3)             version_group
    from moiram."TestPrelapseAnalysis" t
    left join drupal.gcsi_users gu 
        on t."CUSTOMER_ID" = gu.user_uuid)tst
join common.user_dim ud
    on tst.drupal_user_id = ud.drupal_user_id
join common.daily_status_snapshot dss1 
    on ud.gcsi_user_id = dss1.gcsi_user_id AND
       dss1.day_timestamp = '20160318' AND
       dss1.status = 'Lapsed'
join common.daily_status_snapshot dss2 
    on ud.gcsi_user_id = dss2.gcsi_user_id AND
       dss2.day_timestamp = '20160330'
left join drupal.users du
    on tst.drupal_user_id = du.uid
left join common.subscription_d sd on sd.subscription_id = dss1.subscription_id
left join gcsi.subscription_rollover sr
    on dss1.subscription_id = sr.prior_subscription_id;

select email_address,   drupal_user_id, engagement_level, behavior_segment, engagement_group,   
 version_group,  last_access_date, video_view, login,  status, pd_thru_bucket
from tmp.retention_test;

select count(1) from tmp.retention_test rt
join gcsi.subscription_rollover sr on rt.subscription_id = sr.prior_subscription_id;

select count(1) from tmp.retention_test rt
join common.subscription_d sd on rt.subscription_id = sd.subscription_id
join common.user_dim ud on sd.gcsi_user_id = ud.gcsi_user_id
where rt.subscription_id <> current_subscription;
