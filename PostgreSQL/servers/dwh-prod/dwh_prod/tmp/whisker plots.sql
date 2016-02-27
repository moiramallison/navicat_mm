drop table if exists tmp.engagement_subs;

create table tmp.engagement_subs as
(select subscription_id,
   user_behavior_segment,
   trunc(subscription_age/30) subscription_months,
   cancel_date,
   churner
from
    (SELECT
       sd.gcsi_user_id, 
       subscription_id,
       user_behavior_segment,
       coalesce(cancel_date,end_date) cancel_date,
       case when cancel_date is not null then cancel_date::date - start_date::date
            when end_date is not null then end_date::date - start_date::date
            else current_date::date - start_date::date
        end subscription_age,
       case when cancel_date is null and end_date is null then 0 else 1 end churner
       from common.subscription_d sd   
       join common.user_dim ud 
         on sd.gcsi_user_id = ud.gcsi_user_id
       where cancel_date  >= '20140101' or cancel_date is null)foo
);



drop table if exists tmp.engagement_idx;

create table tmp.engagement_idx as
select ud.drupal_user_id, 
       ud.gcsi_user_id, 
       ud.user_behavior_segment,
       dss.subscription_id, 
       sd.start_date,
       vdc.created_date,
       vdc.watched,
       trunc((vdc.created_date::date - sd.start_date::date)/30) view_months
from common.user_dim ud
join 
     (select user_id drupal_user_id,
               created_date,
               sum(watched) watched
         from common.video_daily_cube 
        where watched > 150 and
              engagement_ratio >= .25
        group by user_id, created_date) vdc
    on ud.drupal_user_id = vdc.drupal_user_id
join common.daily_status_snapshot dss
   on ud.gcsi_user_id = dss.gcsi_user_id and
      vdc.created_date::date = dss.day_timestamp::date
join common.subscription_d sd
   on dss.subscription_id = sd.subscription_id;

drop table if exists tmp.engagement_smry;

create table tmp.engagement_smry
as
  select subscription_id,
         month,
         sum(watched) watched,
         max(churner) churner
from
(select
    es.subscription_id,
    month,
    coalesce(watched,0) watched, 
    case when gs.month = es.subscription_months
         then churner
         else 0
    end churner
from 
   (select generate_series(12,30) as month)gs
join tmp.engagement_subs es
   on gs.month <= es.subscription_months
left join tmp.engagement_idx ei
  on ei.subscription_id = es.subscription_id and 
     ei.view_months = gs.month) foo
group by subscription_id, month;


drop table if exists tmp.engagement_master;

create table tmp.engagement_master
as 
select e1.subscription_id,
   e1.subscription_months,
   e1.cancel_date,
   e1.churner,
   case when e1.churner = 1 
        then cancel_date::date - ei.created_date::date
        else current_date::date - ei.created_date::date 
   end days_since_last_video_view,
    round(e2.watched/3600,3) watched_0,
    round(e3.watched/3600,3) watched_1,
    round(e4.watched/3600,3) watched_2,
    round(e5.watched/3600,3) watched_3,
    round(e6.watched/3600,3) watched_4,
    round(e7.watched/3600,3) watched_5,
    round(e8.watched/3600,3) watched_6

from
    (select subscription_id,
       subscription_months,
       cancel_date,
       churner
     from tmp.engagement_subs
     where subscription_months between 12 and 30) e1
left join 
    (select subscription_id, max(created_date) created_date
     from tmp.engagement_idx
     group by subscription_id) ei
   on e1.subscription_id = ei.subscription_id
join tmp.engagement_smry e2
   on e1.subscription_id = e2.subscription_id and
      e1.subscription_months = e2.month
join tmp.engagement_smry e3
   on e1.subscription_id = e3.subscription_id and
      e1.subscription_months -1 = e3.month
join tmp.engagement_smry e4
     on e1.subscription_id = e4.subscription_id and
      e1.subscription_months -2 = e4.month
join tmp.engagement_smry e5
     on e1.subscription_id = e5.subscription_id and
      e1.subscription_months -3 = e5.month
join tmp.engagement_smry e6
     on e1.subscription_id = e6.subscription_id and
      e1.subscription_months -4 = e6.month
join tmp.engagement_smry e7
     on e1.subscription_id = e7.subscription_id and
      e1.subscription_months -5 = e7.month
join tmp.engagement_smry e8
     on e1.subscription_id = e8.subscription_id and
      e1.subscription_months -6 = e8.month;