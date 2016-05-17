
drop table if exists tmp.churners;

create table tmp.churners as
select ud.gcsi_user_id, 
		subscription_id, 
    paid_through_date,
    cancel_date, 
		user_behavior_segment, 
		gcsi_plan_id, 
		ud.engagement_level,
    ud.last_video_view_date::date last_video_view_date,  
    rs.engagement_level r_engagement_level,
		subscription_age,
    case when subscription_age <= 120 then '0-120 days'
         when subscription_age <= 395  then '121-395+ days'
         else '395+ days'
    end tenure_group,
    status,
    case when rs.uid is null then 0 else 1 end retention_contact
from common.user_dim ud
join common.subscription_d sd on ud.current_subscription = sd.subscription_id
join common.engagement_d ed on ud.drupal_user_id = ed.drupal_user_id AND
     measure_date = '20150414'
join common.plan_d pd on sd.dwh_plan_id = pd.dwh_plan_id
left join tmp.retention_subs rs on ud.drupal_user_id = rs.uid
where       (sd.paid_through_date >= '20160301' and 
      sd.paid_through_date <= '20160401' and sd.status = 'Suspended')
or
      (sd.paid_through_date >= '20160315' and 
      sd.paid_through_date <= '20160415' and sd.status = 'Cancelled');

select  retention_contact, r_engagement_level, count(1)
from tmp.churners
group by  retention_contact, r_engagement_level
order by  retention_contact, r_engagement_level;


select user_behavior_segment, tenure_group, status, retention_contact, count(1)
from tmp.churners
group by user_behavior_segment, tenure_group, status, retention_contact
order by user_behavior_segment, tenure_group, status, retention_contact;

select date_trunc('month',last_video_view_date) lvv_month, count(1)
from tmp.churners
group by date_trunc('month',last_video_view_date)
order by date_trunc('month',last_video_view_date);

select ch.gcsi_plan_id, product_name, cancel_date::date, count(1)
from tmp.churners ch
join gcsi.t_plans p on ch.gcsi_plan_id = p.plan_id
group by ch.gcsi_plan_id, product_name, cancel_date::date
order by ch.gcsi_plan_id, product_name, cancel_date::date;

select count(1) 
from subscription_d
where cancel_date > '20160411' and cancel_date < '20160412'
and paid_through_date > '20160414'
;

select cancel_date::date, count(1) 
from subscription_d
where cancel_date > '20160411' and cancel_date < '20160412'
group by cancel_date::date
order by cancel_date::date;

drop table if exists dwh_updater_manual_fix;

create table dwh_updater_manual_fix as
select subscription_id from 
(select subscription_id from gcsi.updater_manual_fix
union
select subscription_id from gcsi.updater_manual_fix2
union
select subscription_id from gcsi.updater_manual_fix3
union
select subscription_id from gcsi.updater_manual_fix4
union
select subscription_id from gcsi.updater_manual_fix5
union
select subscription_id from gcsi.updater_manual_fix6
union
select subscription_id from gcsi.updater_manual_fix8) f1
where exists
 (select subscription_id
        from common.user_d ud
        where f1.subscription_id = ud.subscription_id 
          and status = 'Hold'
          and valid_from > '20151206'
          and (valid_to - valid_from) > 15) ;


select status, day_timestamp, updater_sub, count(1)
from 
(select  paid_through_date::date ptd_date, 
case when subscription_id in 
		(select subscription_id from dwh_updater_manual_fix)
     then 1 else 0 
    end updater_sub,
    case when status = 'Active' and paid_through_date < day_timestamp then 'Suspended' 
         else status
    end status
from common.daily_status_snapshot
where paid_through_date> '20160306') foo
group by status, ptd_date, updater_sub
order by status, ptd_date, updater_sub;

select cancel_date::date, count(1)
from common.subscription_d
where subscription_id in (select subscription_id from dwh_updater_manual_fix)
  and paid_through_date > '20160404'
  and paid_through_date < '20160409'
group by cancel_date::date
order by cancel_date::date;

select status, count(1)
from common.subscription_d
where subscription_id in (select subscription_id from dwh_updater_manual_fix)
  and paid_through_date > '20160304'
group by status;

select * 
from common.subscription_d
where subscription_id in (select subscription_id from dwh_updater_manual_fix)
  and paid_through_date > '20160404'
  and paid_through_date < '20160409'
  and status = 'Active'; 


  select subscription_id,
  day_key, 
   case when paid_through_date > day_timestamp 
        case when (cancel_date is null or cancel_date >  day_timestamp)and
                  (end_date is null or cancel_date >  day_timestamp) then 'Retained'
             when cancel_date <= day_timestamp then 'Lapsed'
        when paid_through_date 
  (SELECT  
  DISTINCT ON (dss.subscription_id)
  dss.subscription_id
  , dss.paid_through_date::date
  , dss.end_date::date
  , dss.next_review_date::date
  , dss.cancel_date::date
  , dss.status
  FROM common.subscription_d dss
  join dwh_updater_manual_fix mf on dss.subscription_id = mf.subscription_id
  WHERE 
  dss.paid_through_date >= '2016-03-06 06:00:00')

select count(1) from updater_manual_fix2;


create table moiram.dss2 as (select * from common.daily_status_snapshot where 1=2);

insert into moiram.dss2
  select dd.day_timestamp,
      gcsi_user_id,
      ud.subscription_id,
      -- they are suspended, the paid_through_date is the schedule_date
     -- or, if it's a current suspension, the current_paid_through_date
      case when status = 'Hold' or status = 'Start/Hold' and 
                          sh.initiator = 'PAYMENT'
           then coalesce(ptd.schedule_date,ud.paid_through_date)
           else coalesce(ptd.paid_through_date,ud.paid_through_date)
           end paid_through_date,
      case when status like 'Trial%' then status
           when status = 'Hold' or status = 'Start/Hold' then 
           case when sh.initiator in  ('CUSTOMER', 'ADMINISTRATIVE')
                then 'Hold'
                else 'Suspended'
           end 
           when cancel_date::date <= dd.day_timestamp::date then 
           case when coalesce(ptd.paid_through_date,ud.paid_through_date) > dd.day_timestamp 
                then 'Lapsed'
                else 'Cancelled'
           end 
           else status
      end status
  from common.user_d ud
  join common.date_d dd
      on ud.valid_from <= dd.day_timestamp AND
         ud.valid_to >= dd.day_timestamp
  left join lateral
       --choose ADMINISTRATIVE hold over payment hold if we have two concurrent holds
      (select  subscription_id, min(initiator) initiator
       from gcsi.subscription_hold h 
       where  h.deleted = 'f'  
         and day_timestamp >= h.start_date::date  
         and (day_timestamp < h.end_date::date or h.end_date is null)
        group by subscription_id) sh
      on ud.subscription_id = sh.subscription_id   
  left join tmp.paid_through_dates ptd
      on ud.subscription_id = ptd.subscription_id and 
         day_timestamp >= ptd.schedule_date::date and
         day_timestamp < ptd.paid_through_date::date
  where day_key >= [$start]
    and day_key < [$end]
  --  and day_timestamp < current_date::date
         -- there are cases where cancel_date is not null and status is still active
         -- because of comps
   and (cancel_date is null or 
        cancel_date >= [$start] or
        ud.paid_through_date >= [$start] or
        status in( 'Active', 'Lapsed', 'Suspended'));


select day_timestamp, status,  count(1)
from common.daily_status_snapshot
where day_timestamp > '20160301'
  and subscription_id in (select subscription_id from updater_manual_fix)
group by day_timestamp, status
order by day_timestamp;


select paid_through_date::date, count(1)
from common.subscription_d
where subscription_id in (select subscription_id from updater_manual_fix)
group by paid_through_date::date
order by paid_through_date::date;

  SELECT
  count(subscription_id) as total_loss
  , status
  FROM (
  SELECT
    day_timestamp,
    dss.subscription_id
    , dss.status
    FROM common.daily_status_snapshot dss
    INNER JOIN common.subscription_d sd ON sd.subscription_id = dss.subscription_id
    INNER JOIN common.user_dim ud ON ud.gcsi_user_id = dss.gcsi_user_id
    WHERE
    dss.day_timestamp >= $1
    AND dss.day_timestamp < $2
    AND dss.paid_through_date >= $3::date  + INTERVAL '6 hours'
    AND dss.paid_through_date < $1::date  + INTERVAL '6 hours'
    AND dss.paid_through_date >= (
        SELECT max(dss2.paid_through_date) as max_date
        FROM common.daily_status_snapshot dss2
        WHERE dss.subscription_id = dss2.subscription_id
        AND dss2.day_timestamp > $1
        AND dss2.day_timestamp <= $1::date + INTERVAL '15 day'
      )
    
    
    
  ) t
  GROUP BY status;


SELECT dss.*
FROM gcsi.updater_manual_fix umf
INNER JOIN common.daily_status_snapshot dss ON dss.subscription_id = umf.subscription_id
WHERE dss.day_timestamp::date = '2016-03-07'
ORDER BY dss.paid_through_date DESC;

SELECT count(1)
FROM gcsi.updater_manual_fix umf
INNER JOIN common.daily_status_snapshot dss ON dss.subscription_id = umf.subscription_id
WHERE dss.day_timestamp::date = '2016-01-01'
and paid_through_date > day_timestamp;

select * from tmp.paid_through_dates where subscription_id = 14178670;

select * from v_subscription_transactions where subscription_id = 14178670
order by schedule_date desc;

select * from subscription_event where subscription_id = 14178670;

select * from common.user_d where subscription_id = 17287221;

select * from common.daily_status_snapshot where subscription_id = 17287221
and day_timestamp between '20160301' and '20160322'
order by day_timestamp;


drop table if exists t2;

create table t2 as 
(select day_timestamp, valid_from, valid_to,
    case when day_timestamp >= valid_from AND
              day_timestamp::date - valid_from::date <= 15 then count else 0 end active_subs,
    case when  day_timestamp >= valid_from AND
							day_timestamp::date - valid_from::date = 16 then count else 0 end cancel_subs
 from 
(select  valid_from, valid_to, count(1) count 
from common.user_d ud
join  gcsi.updater_manual_fix umf
  on ud.subscription_id = umf.subscription_id AND
    ud.status = 'Hold' and 
     ud.valid_from < '2016-03-07' and
     ud.valid_from > '2016-01-01' and 
      ud.valid_to >= '2016-03-07' AND
      ud.valid_to < '29991231'
group by valid_from, valid_to
order by valid_from, valid_to) foo
join common.date_d dd
        on foo.valid_from <= dd.day_timestamp AND
         foo.valid_to >= dd.day_timestamp
  where day_key >= 20160130
    and day_key < 20160308);

select * from t2;

select day_timestamp::date, sum(active_subs) active_subs, sum(cancel_subs) cancel_subs
from t2
group by day_timestamp::date
order by  day_timestamp::date;

select voided_date::date, count(1)
from
(select vst.subscription_id, max(schedule_date) voided_date
from v_subscription_transactions vst
join gcsi.updater_manual_fix umf
  on vst.subscription_id = umf.subscription_id
where txn_state = 'VOIDED' 
  and schedule_date < '20160315'
group by vst.subscription_id) foo
group by voided_date::date
order by voided_date::date desc;

SELECT *
FROM common.financial_churn_summary fcs
WHERE 
fcs.churn_segment = 25
ORDER BY fcs.measure_date DESC
LIMIT 145;

SELECT distinct sd.subscription_id, paid_through_date,
sd.status
FROM
common.subscription_d sd
INNER JOIN common.user_dim ud ON ud.current_subscription = sd.subscription_id
WHERE
sd.cancel_date IS NULL 
AND sd.paid_through_date::date > '2016-04-14'::date - INTERVAL '15 days'
AND sd.paid_through_date::date < '2016-04-14'
order by status;

select * from common.daily_status_snapshot where subscription_id = 22633280
and day_timestamp between '20160301' and '20160322'
order by day_timestamp;


SELECT dss.*
FROM gcsi.updater_manual_fix umf
INNER JOIN common.daily_status_snapshot dss ON dss.subscription_id = umf.subscription_id
WHERE dss.day_timestamp::date = '2016-03-06'
ORDER BY dss.paid_through_date DESC;

select * from gcsi.subscription_event where subscription_id = 14178670;

select * from subscription_transaction where id = 33212792;

select * from subscription_event where id = 33212788;

select * from subscription_d where subscription_id = 14178670;

select * from gcsi.v_subscription_transactions where subscription_id = 14178670;

select * from tmp.paid_through_dates where  subscription_id = 24654367;

drop table if exists tmp.paid_through_dates;

create table tmp.paid_through_dates as 
select  sd.subscription_id, 
        se.schedule_date, 
        lead(se.schedule_date) over w as paid_through_date 
from common.subscription_d sd
join gcsi.subscription_event se 
    on se.subscription_id = sd.subscription_id
join gcsi.subscription_transaction st 
    on se.id = st.subscription_plan_event_id and 
       st.txn_state = 'SETTLED'
       -- I think I need at least a year for annuals
where se.schedule_date > '20130101'
window w as (
partition by sd.subscription_id
order by
    se.schedule_date
rows between unbounded preceding
and unbounded following
);


select * from common.user_d where status = 'Hold' and valid_from > '20160301' and valid_to < '20160310';

select * from common.daily_status_snapshot
where subscription_id = 24654367
  and day_timestamp between '20160301' and'20160315'
order by day_timestamp;

select * from tmp.paid_through_dates ptd
where  ptd.subscription_id = 24654367 and
         '20160303'::date >= ptd.schedule_date::date and
         '20160303'::date < ptd.paid_through_date::date;

select * from common.user_d ud where subscription_id = 24654367
      and ud.valid_from <=  '20160303'::date AND
         ud.valid_to >=  '20160303'::date;

select status, ptd.schedule_date, ptd.paid_through_date,
      case when status = 'Hold' or status = 'Start/Hold' 
           then coalesce(ptd.schedule_date,ud.paid_through_date)
           else coalesce(ptd.paid_through_date,ud.paid_through_date)
           end paid_through_date
from common.user_d ud 
join tmp.paid_through_dates ptd on ud.subscription_id = ptd.subscription_id
and
         '20160303'::date >= ptd.schedule_date::date and
         '20160303'::date < ptd.paid_through_date::date
where ud.subscription_id = 24654367
      and ud.valid_from <=  '20160303'::date AND
         ud.valid_to >=  '20160303'::date;


drop table if exists t1;

create table t1 as 
(select subscription_id, valid_from, valid_to 
from common.user_d
where status = 'Hold' AND
     valid_from >= '20160115' AND
      valid_to-valid_from > 15);

delete from t1
where EXISTS
  (select subscription_id from 
    (select  subscription_id, h.start_date::date hold_start, min(initiator) initiator
       from gcsi.subscription_hold h 
       where  h.deleted = 'f'  
         and t1.valid_from  = h.start_date::date
        group by subscription_id, hold_start) sh
   where initiator in ('CUSTOMER', 'ADMINISTRATIVE') AND
   t1.subscription_id = sh.subscription_id and
   t1.valid_from = hold_start);

select valid_from, count(1)
from t1
where valid_from < '20160404'
group by valid_from
order by valid_from;

select subscription_id from t1 where valid_from = '20160308';

select * from common.user_d where subscription_id = 25593475;

select * from common.current_users ud
join gcsi.updater_manual_fix umf
  on ud.subscription_id = umf.subscription_id
where status = 'Hold'
  and valid_from >= '20160406';

s