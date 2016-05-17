-- this is slightly different then Parker's query

drop table if exists tmp.paid_subscriptions;

create table tmp.paid_subscriptions as 
SELECT
       distinct gs.id subscription_id
   FROM
       gcsi.subscription gs
   JOIN gcsi.subscription_event se ON se.subscription_id = gs.id
   JOIN gcsi.subscription_transaction_subscription_events stse ON stse.subscription_event_id = se.id
   JOIN gcsi.subscription_transaction st ON st.id = stse.subscription_transaction_id and st.txn_state = 'SETTLED' 
   LEFT JOIN gcsi.credit_card_transaction cct ON cct.id = st.id
   LEFT JOIN gcsi.pay_pal_transaction ppt ON ppt.id = st.id
        AND (cct.amount > 0 OR ppt.amount > 0);


DROP TABLE if exists tmp.t_subscriptions_snap;
CREATE TABLE tmp.t_subscriptions_snap AS SELECT
    d.uid AS drupal_user_id,
    user_id AS gcsi_user_id,
    s.subscription_id,
    s.start_date,
    s.cancel_date,
    s.paid_through_date,
    s.next_review_date,
    s.end_date,
    s.plan_id,
    s.division,
    s.source_name,
    s.service_name,
    s.status,
    s.channel,
    ROW_NUMBER () OVER w AS rn
FROM
    gcsi.t_subscriptions s
JOIN gcsi.users u ON u. ID = s.user_id
JOIN drupal.gcsi_users d ON u.uuid = d.user_uuid
join tmp.paid_subscriptions ps on ps.subscription_id = s.subscription_id
WHERE
    source_name IN (
        'Gaiam TV',
        'MYO'
    ) 
WINDOW w AS (
PARTITION BY user_id
ORDER BY
    start_date,
    paid_through_date,
    end_date ROWS BETWEEN UNBOUNDED PRECEDING
AND UNBOUNDED FOLLOWING
);


drop table if exists tmp.subscriptions_tmp;

create table tmp.subscriptions_tmp as
select b.*,
       -- first attempt at "un-overlapping".  If two subscriptions just butt up against each 
       -- other (e.g. plan changes) this will be sufficient.
       case when (start_date,cancel_date) overlaps (next_start, next_end)
                then next_start - interval '1 day'
            when cancel_date is null and next_start is not null 
                then next_start - interval '1 day'
            when next_start is null then '2999-12-31'::date
            else cancel_date
       end  valid_to
from  --- start with subscriptions, order them by start_date and 
      --- get the start and end of the next subscription
      --- get the start and end of the next subscription for this user_id
    (select gcsi_user_id, subscription_id, 
            start_date, cancel_date, paid_through_date,
            case when cancel_date is NULL then age(current_date, start_date) 
                 when paid_through_date > cancel_date then age(paid_through_date,start_date)
                 else age(cancel_date,start_date)
            end subscription_age,
            lead(start_date) over w  next_start,
            lead(cancel_date)over w  next_end
    from 
        (select   gcsi_user_id, subscription_id, paid_through_date,
            coalesce(cancel_date,end_date) cancel_date,
            start_date            
        from tmp.t_subscriptions_snap) a
    window w as (
    partition by gcsi_user_id
    order by paid_through_date, start_date
    rows between unbounded preceding and unbounded following
    )
 order by gcsi_user_id, start_date, subscription_age, subscription_id) b;


-- this can only happen when there is another subscription with the same start date
-- the last subscription (see ordering above) with the same start date will not meet this condition 
delete from tmp.subscriptions_tmp
where valid_to < start_date;

-- two subscriptions with same start_date, different cancel dates
-- delete the one with the shorter age
delete from tmp.subscriptions_tmp st1
where EXISTS
   (select subscription_id 
    from tmp.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and st1.start_date::date = st2.start_date::date
      and st1.subscription_age < st2.subscription_age);

-- two subscriptions with that still overlap (different start_dates)
-- this can include open subscriptions, so use the current date if needed
-- delete the one with the shorter age
delete from tmp.subscriptions_tmp st1
where EXISTS
   (select subscription_id 
    from tmp.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and (st1.start_date,st1.valid_to) overlaps (st2.start_date, coalesce(st2.valid_to,current_date))
      and  st1.subscription_age < st2.subscription_age);



-- this should be zero; testing for overlaps
select count(1)
from  tmp.subscriptions_tmp st1
where EXISTS
   (select subscription_id 
    from tmp.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and (st1.start_date,st1.valid_to) overlaps (st2.start_date, coalesce(st2.valid_to,current_date)));

select count(1) from tmp.subscriptions_tmp where valid_to is null;


drop table if exists tmp.t_uniq_subs;


-- now use this to create a uniq_subs_table
create table tmp.t_uniq_subs as
select t1.*
from
        (select t.drupal_user_id,
        t.gcsi_user_id,
        t.subscription_id,
        t.start_date,
        t.paid_through_date,
        t.next_review_date,
        case when t.cancel_date is not null
             then least(t.cancel_date, d1.valid_to)
             else NULL
        end  cancel_date,
        case when t.end_date is not null
             then least(t.end_date, d1.valid_to)
            else NULL
        end  end_date,
        t.plan_id,
        t.division,
        t.source_name,
        t.service_name,
        t.channel,
        t.rn
  from tmp.t_subscriptions_snap t
  join tmp.subscriptions_tmp d1
    on t.subscription_id = d1.subscription_id) t1
where t1.start_date::date <> t1.cancel_date::date
      or t1.cancel_date is null;



drop table if exists tmp.users_tmp;

create table tmp.users_tmp as SELECT
    s.gcsi_user_id,
    s.subscription_id,
    s.start_date,
    s.cancel_date,
    s.paid_through_date,
    s.next_review_date,
    s.end_date,
    lead(s.subscription_id) over w next_subscription_id,
    lead(s.start_date) over w next_start_date
FROM
    tmp.t_uniq_subs s
WINDOW w AS (
        PARTITION BY s.gcsi_user_id
        ORDER BY
            rn
    );


drop table  if exists  tmp.tmp_holds_prelim;

create table  tmp.tmp_holds_prelim as
    (SELECT DISTINCT on (gcsi_user_id,sh.subscription_id, hold_start_date)
        sh.id,
        s.gcsi_user_id,
        sh.subscription_id,
        sh.start_date::date AS hold_start_date,
        sh.end_date::date AS hold_end_date
    FROM
        gcsi.subscription_hold sh
    JOIN tmp.users_tmp s ON sh.subscription_id = s.subscription_id
    WHERE
        deleted = 'f' AND
        s.start_date <= sh.start_date
    ORDER BY gcsi_user_id, subscription_id, hold_start_date);

-- if the subscription has a cancel_date and an open hold 
-- set the hold_end date to the cancel_date
update tmp.tmp_holds_prelim h
set hold_end_date = ts.cancel_date::date
from gcsi.t_subscriptions ts
where h.subscription_id = ts.subscription_id
  and h.hold_end_date is NULL
  and ts.cancel_date is not null;

drop table  if exists  tmp.tmp_holds;

create table tmp.tmp_holds as
  (select 
    h1.id, 
    h1.gcsi_user_id,
    h1.subscription_id, 
    h1.hold_start_date,
    case when h1.hold_end_date is null then
         case when h2.new_end_date is null
              then h2.last_start_date + interval '6 months'
              else h2.new_end_date
         end
         else h1.hold_end_date
    end hold_end_date
from  tmp.tmp_holds_prelim h1
left join
    (select id, min(new_end_date) new_end_date,
                max(start_date) last_start_date
     from
        (select h3.*,
               h4.id  id2,
               h4.hold_end_date new_end_date,
               h4.hold_start_date start_date
        from tmp.tmp_holds_prelim h3
        join tmp.tmp_holds_prelim h4
          on h3.subscription_id = h4.subscription_id and 
             h3.hold_start_date < h4.hold_start_date
        where h3.hold_end_date is null) t
     group by id) h2
    on h1.id  = h2.id);

drop sequence if exists hold_groups;
create sequence hold_groups;

drop table if exists tmp.hold_overlaps2;

-- take care of overlapping holds
create table tmp.hold_overlaps2 as
select  
   case when prev_id is null then 0
        when hold_start_date <=  prev_hold_end then 1
        when hold_end_date = prev_hold_end then  2
        when (hold_start_date - interval '1 day') = prev_hold_end then 3
        else 0
   end hold_overlap,
   case when prev_id is null then nextval('hold_groups')
        when hold_start_date <=  prev_hold_end then 0
        when hold_end_date = prev_hold_end then 0
        when (hold_start_date - interval '1 day') = prev_hold_end then 0
        else nextval('hold_groups')
   end hold_group,
   h3.*
from
    (select h1.*,
        first_value(id) over w prev_id,
        lag(hold_end_date) over w prev_hold_end,
        row_number() over w hold_number
    from  tmp.tmp_holds h1
    where EXISTS
        (select subscription_id 
         from tmp.tmp_holds h2
         where h1.subscription_id = h2.subscription_id
           and h1.id <> h2.id
           and ((h1.hold_start_date::date,h1.hold_end_date) overlaps (h2.hold_start_date, h2.hold_end_date)
            or (h2.hold_start_date::date - h1.hold_end_date::date) between 0 and  1 -- same day or next day
            or (h1.hold_start_date::date -  h2.hold_end_date::date) between 0 and  1))  -- need the other half to stitch them together
    window w as
    (partition by subscription_id 
     order by hold_start_date
     rows between unbounded preceding and unbounded following
    )) h3;


update tmp.hold_overlaps2 h1 set hold_group = 
   (select max(hold_group) from tmp.hold_overlaps2 h2
    where h2.hold_number < h1.hold_number AND
         h2.gcsi_user_id = h1.gcsi_user_id and
         h2.subscription_id = h1.subscription_id)
where hold_group = 0;

delete from tmp.tmp_holds where id in (select id from tmp.hold_overlaps2 where hold_overlap > 0);

update tmp.tmp_holds h 
set hold_end_date = o.hold_end_date
FROM
    (select * from 
        (select hold_overlap,
               hold_group, 
               first_value(id) over w id,
               last_value(hold_end_date) over w hold_end_date
        from tmp.hold_overlaps2
        window w as (
           partition by hold_group
           order by hold_number
           rows between unbounded preceding and unbounded following )) t
    where hold_overlap = 0) o
where h.id = o.id;



truncate table tmp.tmp_activity;


INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id) gcsi_user_id,
        subscription_id,
        start_date AS activity_date,
        'Start' activity
    FROM
        tmp.users_tmp
);

INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id) gcsi_user_id,
        subscription_id,
        end_date AS activity_date,
        'End' activity
    FROM
        tmp.users_tmp
  where end_date is not null and 
        (end_date::date > cancel_date::date OR
         cancel_date is null)
);


INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id) gcsi_user_id,
        subscription_id,
        paid_through_date AS activity_date,
        'Paid_Through' activity
    FROM
        tmp.users_tmp u1
  where paid_through_date::date > cancel_date::date
    and paid_through_date::date > end_date::date
    and not EXISTS  -- no overlapping subscription
        (select subscription_id 
         from tmp.users_tmp u2
         where u1.gcsi_user_id = u2.gcsi_user_id
           and u1.subscription_id <> u2.subscription_id
           and u1.start_date < u2.start_date
           and u2.start_date::date <= u1.paid_through_date::date)
);
/*
INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id) gcsi_user_id,
        subscription_id,
        next_review_date AS activity_date,
        'Next Review' activity
    FROM
        tmp.users_tmp
where next_review_date is not null
);
*/
INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id) gcsi_user_id,
        subscription_id,
        cancel_date AS activity_date,
        'Cancel' activity
    FROM
        tmp.users_tmp
where cancel_date is not null
);


INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id, hold_start_date) gcsi_user_id,
        subscription_id,
        hold_start_date AS activity_date,
        'Hold Start' activity,
        id
    FROM
        tmp.tmp_holds
where hold_start_date is not null
);


INSERT INTO tmp.tmp_activity (
    SELECT DISTINCT
        ON (subscription_id, hold_end_date) gcsi_user_id,
        subscription_id,
        hold_end_date AS activity_date,
        'Hold End' activity,
        id
    FROM
        tmp.tmp_holds
where hold_end_date is not null
);

 -- the following adjustments came about from testing the creation 
 -- of the user dimension and finding errors

 
 -- this is primarily to handle the case where Travis swept the database and
 -- multiple subscriptions for a user have the same cancel date.  If a subscription has a
 -- cancel or hold end activity that overlaps the start of the next subscription start, force it
 -- to one day before the subscription start

with t1 as
(select subscription_id, 
        next_start_date::date - interval '1 day' cancel_date
from tmp.users_tmp)
update tmp.tmp_activity ta set activity_date = t1.cancel_date
from t1
where (activity = 'Cancel' OR
       activity = 'Hold End')
  and activity_date::date > t1.cancel_date::date
  and ta.subscription_id = t1.subscription_id;


 -- when I have a 'Hold End' activity that's the 
 -- same date (or later) as the cancel date, delete the 'Hold End' record
delete from tmp.tmp_activity ta
where activity = 'Hold End' 
  and exists 
     (select ta.subscription_id
      from tmp.tmp_activity t1 
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date <= ta.activity_date::date
        and t1.activity = 'Cancel');


 -- when I have a 'Hold Start' activity that's the 
 -- same date as the cancel date, delete the 'Hold Start' record
delete from tmp.tmp_activity ta
where activity = 'Hold Start' 
  and exists 
     (select ta.subscription_id
      from tmp.tmp_activity t1 
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date <= ta.activity_date::date
        and t1.activity = 'Cancel');

-- when I have a 'Hold End' and a new 'Hold Start' on the same day, delete the 'Hold End'
delete from tmp.tmp_activity ta
where activity = 'Hold End' 
  and exists 
     (select ta.subscription_id
      from tmp.tmp_activity t1 
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date = ta.activity_date::date
        and t1.activity = 'Hold Start'
        and t1.row_number > ta.row_number);

 -- when I have two consecutive 'Hold Start' activities  delete the  second one
delete from tmp.tmp_activity ta
where activity = 'Hold Start' 
  and (subscription_id, activity_date) in 
       (select subscription_id, activity_date from 
          (select gcsi_user_id, subscription_id, activity_date, activity,
                 lag(activity) over
                      (partition by subscription_id order by activity_date  
                       ROWS BETWEEN UNBOUNDED PRECEDING
                       AND UNBOUNDED FOLLOWING)  prev_activity
          from tmp.tmp_activity) t
where activity = 'Hold Start' and prev_activity = 'Hold Start');

 -- when I have two consecutive 'Hold End' activities  delete the  first one
delete from tmp.tmp_activity ta
where activity = 'Hold End' 
  and (subscription_id, activity_date) in 
       (select subscription_id, activity_date from 
          (select gcsi_user_id, subscription_id, activity_date, activity,
                 lead(activity) over
                      (partition by subscription_id order by activity_date  
                       ROWS BETWEEN UNBOUNDED PRECEDING
                       AND UNBOUNDED FOLLOWING)  next_activity
          from tmp.tmp_activity) t
where activity = 'Hold End' and next_activity = 'Hold End');

 -- when I have  'Hold Start' and 'Hold End' on the same day, bump the 'Hold End' by one DAY
-- so I don't have overlapping valid_from/valid_to dates
update tmp.tmp_activity ta
set activity_date = activity_date + interval '1 day'
where activity = 'Hold End' 
  and exists 
     (select ta.subscription_id
      from tmp.tmp_activity t1 
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date = ta.activity_date::date
        and t1.activity = 'Hold Start'
        and t1.row_number = ta.row_number);



 --OK!!! Finally ready to create the table with valid_from and valid_to dates
drop table if exists tmp.user_activity_tmp;

CREATE TABLE tmp.user_activity_tmp AS (
    SELECT
        t1.gcsi_user_id,
        t1.subscription_id,
        t1.activity,
        t1.activity_date::date valid_from,
    case when gcsi_user_id = next_user
         then greatest(activity_date, t1.next_activity_date - integer '1')::date
         else to_date('29991231','yyyymmdd')
    end valid_to,
    case when gcsi_user_id = next_user
         then 'N'
         else 'Y'
    end current_record
 from
     (select t.*, 
         lead (activity_date::date)over w  next_activity_date,
         lead  (gcsi_user_id)  over w next_user
      from tmp.tmp_activity t 
      window w as (
            partition by t.gcsi_user_id
            order by
                activity_date,
                row_number rows between unbounded preceding
            and unbounded following
      )
    ) t1
);


--one more thing to clean up situation wihere Start and Hold Start have same date

drop table if exists tmp.start_hold_tmp;

create table tmp.start_hold_tmp as 
select u1.* ,  u2.valid_to hold_valid_to, u2.current_record hold_cur_rec
from tmp.user_activity_tmp u1
join tmp.user_activity_tmp u2
  on  u1.subscription_id = u2.subscription_id and 
      u1.valid_from = u2.valid_from
where u1.activity = 'Start'
  and u2.activity = 'Hold Start';

update tmp.user_activity_tmp ua
set valid_to = greatest(st.valid_to, st.hold_valid_to),
    current_record = greatest(st.current_record, st.hold_cur_rec),
    activity = 'Start/Hold'
from tmp.start_hold_tmp st 
where ua.subscription_id = st.subscription_id 
  and ua.valid_from = st.valid_from 
  and ua.activity = 'Start';

delete from tmp.user_activity_tmp ua using tmp.start_hold_tmp st 
where ua.subscription_id = st.subscription_id 
  and ua.valid_from = st.valid_from 
  and ua.activity = 'Hold Start';

-- if there are Start and Cancel records with same 
-- valid_from/valid_to dates, delete the Start record
delete from tmp.user_activity_tmp ua 
where ua.activity = 'Start'
  and ua.valid_from = ua.valid_from 
  and EXISTS
    (select subscription_id
     from tmp.user_activity_tmp ub
     where ua.subscription_id = ub.subscription_id
       and ub.activity = 'Cancel'
       and ua.valid_from = ub.valid_from);



-- get plan segments for each row
drop table if exists tmp.segments_tmp;

create table tmp.segments_tmp as 
(select t3.*,
    seg.duration_period::interval plan_period
 from
    (select subscription_id, valid_from,
            max(segment_id) segment_id
    from
        (select subscription_id,  
               valid_from,
               schedule_date,
               last_value(segment_id) 
               over (partition by subscription_id, valid_from 
               order by schedule_date
               rows between unbounded preceding and unbounded following) segment_id
        from
            (select ua.subscription_id, 
                    ua.valid_from, 
                    se.schedule_date,
                    spe .segment_id
             from tmp.user_activity_tmp ua
             join gcsi.subscription_event se on ua.subscription_id = se.subscription_id
             join gcsi.subscription_plan_event spe on se.id = spe.id 
             where se.schedule_date <= ua.valid_to) t) t1
    group by subscription_id, valid_from) t3
 join gcsi.segment seg  on t3.segment_id = seg.id);


DROP TABLE if exists tmp.user_attributes_tmp;

create table tmp.user_attributes_tmp as (
select t.*,
       uo.segment_name onboarding_segment,
       uo.parent_segment onboarding_parent
from
    (select
        drupal_user_id,
        gcsi_user_id,
        min (start_date :: date) user_start_date
    from
        tmp.t_uniq_subs
   group by drupal_user_id, gcsi_user_id) t
 left JOIN
   (select uid, max(choice) choice
     from 
       (select uid,
               last_value(choice) 
                   over (partition by uid 
                   order by timestamp
                   rows between unbounded preceding and unbounded following) as choice
        from drupal.user_onboard_event_log) t
      group by uid) dr
   on dr.uid = t.drupal_user_id
left join common.user_onboard_segments uo
    on dr.choice = segment_id);
    
-- these are subscription-level attributes not in t_subscriptions
drop table if exists tmp.subscription_attributes_tmp;

create table tmp.subscription_attributes_tmp as
select us.subscription_id,
       p.product_name plan_name,
       c.department campaign_dept,
       ubs.user_behavior_segment
from tmp.t_uniq_subs us
left join gcsi.t_plans p on us.plan_id = p.plan_id 
left join common.campaign_tracking c on us.channel = c.reported_channel
left join 
    (select uid drupal_user_id, max(segment_name) user_behavior_segment
     from 
        (select uid, 
       last_value(segment_name) over 
                    (partition by uid
                     order by engagement_ratio
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) segment_name
        from 
     common.user_behavior_segmentation) t
        group by uid) ubs
  on us.drupal_user_id = ubs.drupal_user_id;

DROP TABLE if exists common.user_d cascade;

CREATE TABLE common."user_d" (
"id"                        serial,
"drupal_user_id"            int8,
"gcsi_user_id"              int8,
"user_start_date"           date,
"user_end_date"             timestamp(6),
"subscription_id"           int8,
"subscription_start_date"   timestamp(6),
"paid_through_date"         timestamp(6),
"next_review_date"          timestamp(6),
"cancel_date"               timestamp(6),
"subscription_end_date"     timestamp(6),
"activity"                  text COLLATE "default",
"status"                    text COLLATE "default",
"entitled"                  bool,
"plan_id"                   int8,
"segment_id"                int8,
"plan_name"                 text COLLATE "default",
"plan_period"               interval,
"cid_channel"               text COLLATE "default",
"campaign_dept"             text COLLATE "default",
"user_behavior_segment"     text COLLATE "default",
"source_name"               text COLLATE "default",
"service_name"              text COLLATE "default",
"winback"                   text COLLATE "default",
"onboarding_segment"        text COLLATE "default",
"onboarding_parent"         text COLLATE "default",
"subscription_cohort"       text COLLATE "default",
"sub_cohort_segment"        text COLLATE "default",
"valid_from"                date,
"valid_to"                  date,
"current_record"            text COLLATE "default"
)
WITH (OIDS=FALSE)
;


insert into common.user_d
   (drupal_user_id, gcsi_user_id, user_start_date, user_end_date, 
        subscription_id, subscription_start_date, paid_through_date, 
        next_review_date, cancel_date, subscription_end_date, activity, 
        status, entitled, plan_id, segment_id, plan_name, plan_period, 
        cid_channel, campaign_dept, user_behavior_segment, source_name, 
        service_name, winback, onboarding_segment, onboarding_parent, 
    valid_from, valid_to, current_record)
(select t.drupal_user_id,
        t.gcsi_user_id,
        ut.user_start_date,
        case when t.end_date < ua.valid_to
             then t.end_date
             else NULL
        end user_end_date,
        t.subscription_id,
        t.start_date subscription_start_date,
        paid_through_date,
        t.next_review_date,
        case when t.cancel_date < ua.valid_to then t.cancel_date
             when activity = 'Cancel' then  ua.valid_from
             else NULL
        end cancel_date,        
        case when t.end_date < ua.valid_to
             then t.end_date
             else NULL
        end subscription_end_date,
        ua.activity,
        case when ua.activity in ('Start', 'Hold End') then 'Active'
             when ua.activity in ('Start/Hold', 'Hold Start') then 'Hold'
             when ua.activity in ('Cancel', 'End','Paid_Through') then 'Cancelled'
        end status,
        case when ua.activity in ('Start/Hold', 'Hold Start', 'Paid_Through') then 'f'::bool
             when t.cancel_date is null then 't'::bool
             when t.cancel_date > valid_to then 
                   case when ua.activity in ('Cancel', 'End')
                        then 'f'::bool
                        else 't'::bool
                   end
             when t.cancel_date < paid_through_date then 't'::bool
             else 'f'::bool
        end entitled,  --swag.  use drupal instead?
        t.plan_id,
                seg.segment_id,
        s.plan_name,
        seg.plan_period,
        t.channel,
        s.campaign_dept,
        s.user_behavior_segment,
        t.source_name,
        t.service_name,
        case when ut.user_start_date::date < t.start_date::date
             then 'Winback'
             else 'New User'
        end winback,
        ut.onboarding_segment,
        ut.onboarding_parent,
        ua.valid_from,
        ua.valid_to,
        ua.current_record
from tmp.t_uniq_subs t
left join tmp.user_activity_tmp ua
   on t.subscription_id = ua.subscription_id
join tmp.user_attributes_tmp ut
   on t.gcsi_user_id = ut.gcsi_user_id
join tmp.subscription_attributes_tmp s
   on t.subscription_id = s.subscription_id
join tmp.segments_tmp seg
   on ua.subscription_id = seg.subscription_id and 
      ua.valid_from = seg.valid_from
order by gcsi_user_id, valid_from);
   
update common.user_d d1 set subscription_cohort = t.campaign
from 
(select subscription_id,
 case when campaign like '%Cosmic Disc%' then 'Cosmic Disclosure'
      when campaign like '%Conscious Cleanse%' then 'Conscious Cleanse'
      when campaign like '%al008%'then 'Commit to You'
      when campaign like '%al026%'then 'Gaiam Prospect Offer'
 end campaign
from common.user_d d
join common.campaign_tracking c
    on d.cid_channel = c.reported_channel) t
where d1.subscription_id = t.subscription_id
  and t.campaign is not null;

update common.user_d d1 set sub_cohort_segment = c.sub_cohort_segment
from common.campaign_tracking c
where d1.cid_channel = c.reported_channel
  and d1.subscription_cohort = 'Gaiam Prospect Offer';

update common.user_d d1 set sub_cohort_segment = 'cid'
where subscription_cohort is not null
  and sub_cohort_segment is null;

update common.user_d d1 
set subscription_cohort = 'Commit to You',
    sub_cohort_segment = 'guide opt-in'
where drupal_user_id in
   (select uid from drupal.flag_content 
    where fid = 11 
      and content_id = 92376)
  and d1.subscription_start_date >= '2014-12-26'::date
  and d1.subscription_start_date < '2015-02-01'::date;

update common.user_d d1 
set subscription_cohort = 'Conscious Cleanse',
    sub_cohort_segment = 'guide opt-in'
where drupal_user_id in
   (select uid from drupal.flag_content 
    where fid = 11 
      and content_id = 97961)
  and d1.subscription_start_date >= '2014-12-26'::date
  and d1.subscription_start_date < '2015-02-01'::date;



drop table if exists tmp.cosmic_decision_tree_tmp;


-- alternative ways of joining cosmic cohort
create table tmp.cosmic_decision_tree_tmp (
gcsi_user_id int,
subscription_start_date date,
goode_cid int default 0,
wilcock_cid int default 0,
cd_first_video_engagement int default 0,
cd_any_video_engagement int default 0);

insert into tmp.cosmic_decision_tree_tmp 
     (gcsi_user_id, subscription_start_date, goode_cid, wilcock_cid, cd_first_video_engagement, cd_any_video_engagement)
(select 
       gcsi_user_id,
       subscription_start_date::date,
       case when cid_channel like '%goode%' then 1 else 0 end,
       case when cid_channel= '%wilcock%' 
                 and subscription_start_date >= '2015-07-21'::date 
            then 1 else 0 end,
       case when v1.drupal_user_id is not null then 1 else 0 end,
       case when v2.drupal_user_id is not null then 1 else 0 end
from common.user_d u
left join -- first video engagement
        (select drupal_user_id
    from
        (select drupal_user_id, min(first_nid) first_nid
        from
            (select user_id drupal_user_id,
                    first_value(nid) 
                         over (partition by user_id 
                         order by created
                         rows between unbounded preceding and unbounded following) first_nid
             from moiram.video_tmp
             where created > 1435709437 AND
                   user_id > 0)t
        group by drupal_user_id) t2
    join common.video_d v
       on t2.first_nid = v.media_nid
    where series_title = 'Cosmic Disclosure') v1
  on u.drupal_user_id = v1.drupal_user_id
left join --any video engagement
    (select distinct
          user_id drupal_user_id
     from moiram.video_tmp vt
     join common.video_d v
       on vt.nid = v.media_nid
    where series_title = 'Cosmic Disclosure') v2
  on u.drupal_user_id = v2.drupal_user_id
where u.subscription_start_date >= '2015-07-01'::date
  and u.current_record = 'Y');

update common.user_d d1 set sub_cohort_segment = 'goode'
where gcsi_user_id in
   (select gcsi_user_id
    from tmp.cosmic_decision_tree_tmp t
    where t.goode_cid = 1 
      and t.cd_any_video_engagement = 1)
   and sub_cohort_segment is null;

update common.user_d d1 set sub_cohort_segment = 'wilcock'
where gcsi_user_id in
   (select gcsi_user_id
    from tmp.cosmic_decision_tree_tmp
    where wilcock_cid = 1 
      and cd_any_video_engagement = 1)
  and sub_cohort_segment is null;

update common.user_d d1 set sub_cohort_segment = 'first video engagement'
where gcsi_user_id in
   (select gcsi_user_id
    from tmp.cosmic_decision_tree_tmp
    where cd_first_video_engagement = 1)
   and sub_cohort_segment is null;

update common.user_d d1 set subscription_cohort = 'Cosmic Disclosure'
where sub_cohort_segment in
   ('first video engagement', 'goode', 'wilcock')
  AND subscription_cohort is null;

create view common.current_users
as select * from common.user_d where current_record = 'Y';
/*
select subscription_cohort, sub_cohort_segment, status, count(distinct gcsi_user_id)
from common.user_d
where current_record = 'Y'
group by subscription_cohort, sub_cohort_segment, status;
*/
