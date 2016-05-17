
-- get refund transactions; the only ones we care about are multi-month where 
-- there was a full refund.
drop table if exists moiram.refund_transactions;

create table moiram.refund_transactions as
select ref.*,
       lem.entitlement_end_date
from
   (select distinct on (se.subscription_id)
      se.subscription_id ,
      ts.user_id gcsi_user_id,
      seg.duration_period,
      rt.* 
   from
   ((select distinct on (cct1.retry_id)
           cct1.amount as refund_amount,
           st.transaction_date as refund_date,
           cct1.transaction_number as tx_ref_num,
           cct2.id original_txn_id
         from gcsi.gaiam_division gd
            join gcsi.subscription_transaction st on st.division_id = gd.id
            join gcsi.credit_card_transaction cct1 on cct1.id = st.id
            join gcsi.direct_capture_transaction dct on dct.refund_id = st.id
            join gcsi.credit_card_transaction cct2 on cct2.id = dct.id
         where
           st.txn_state = 'SETTLED' and 
           gd.name = 'Gaiam TV' and
           cct1.amount = cct2.amount -- full refund
         order by cct1.retry_id, st.id)
     UNION
     -- PayPal  Refund Transactions
     (select  
             ppt.amount as refund_amount,
             st.transaction_date as refund_date,
             response.transaction_id::varchar(40) as tx_ref_num,
             pprt.id original_txn_id
      from gcsi.gaiam_division gd
      join gcsi.subscription_transaction st on st.division_id = gd.id
      join gcsi.pay_pal_refund_transaction ppr on ppr.id = st.id
      join gcsi.pay_pal_transaction ppt on ppr.id = ppt.id
      join gcsi.pay_pal_response_data response on response.id = ppt.last_response_id
      join gcsi.pay_pal_refundable_transaction pprt on pprt.refund_id = ppr.id
      join gcsi.pay_pal_transaction ppt2 on ppt2.id = pprt.id
      where st.txn_state = 'SETTLED'
        and gd.name = 'Gaiam TV'
        and ppt.amount = ppt2.amount)) rt
    join gcsi.subscription_transaction_subscription_events stse on stse.subscription_transaction_id = rt.original_txn_id
    join gcsi.subscription_event se on stse.subscription_event_id = se.id
    join gcsi.subscription_plan_event spe on spe.id = stse.subscription_event_id
    join gcsi.subscription_transaction st2 on st2.id = rt.original_txn_id
    join gcsi.segment seg on seg.id = spe.segment_id
    join gcsi.t_subscriptions ts on se.subscription_id  =  ts.subscription_id
    where seg.duration_period <> 'P1M'
    order by se.subscription_id, refund_date)ref
join gcsi.users u on ref.gcsi_user_id = u.id
left join drupal.gcsi_users gu on gu.user_uuid = u.uuid
left join 
    (select uid, to_timestamp(eend) entitlement_end_date
     from
        (select uid, max("end") eend
         from drupal.lemonade_entitlements
         group by uid) ent
     ) lem 
    on gu.uid = lem.uid;

--alter table moiram.refund_transactions owner to dw_admin;

create index idxm_rt_sid on moiram.refund_transactions using btree (subscription_id);

drop table if exists moiram.paid_subscriptions_tmp;

create table moiram.paid_subscriptions_tmp as
select gs.id subscription_id
from gcsi.subscription gs
join gcsi.subscription_event se on se.subscription_id = gs.id
join gcsi.subscription_transaction_subscription_events stse on stse.subscription_event_id = se.id
join gcsi.subscription_transaction st on st.id = stse.subscription_transaction_id and st.txn_state = 'SETTLED'
left join gcsi.credit_card_transaction cct on cct.id = st.id and cct.amount >0
left join gcsi.pay_pal_transaction ppt on ppt.id = st.id and  ppt.amount > 0;

drop table if exists moiram.paid_subscriptions;

create table moiram.paid_subscriptions as 
select distinct subscription_id from moiram.paid_subscriptions_tmp;


--alter table tmp.paid_subscriptions_tmp owner to dw_admin;

create index idxm_ps_sid on moiram.paid_subscriptions using btree (subscription_id);

drop table if exists moiram.dwh_subscriptions;
create table moiram.dwh_subscriptions as 
select t.*
from
    (select
        d.uid AS drupal_user_id,
        user_id AS gcsi_user_id,
        s.subscription_id,
        s.start_date,
        case when rt.refund_date < s.cancel_date then rt.refund_date
             when s.cancel_date is null then rt.refund_date
             else s.cancel_date
        end cancel_date,
        case when rt.refund_date < s.paid_through_date then rt.refund_date
             else s.paid_through_date
        end paid_through_date,
        case when rt.refund_date < s.next_review_date then rt.refund_date
             else s.next_review_date
        end next_review_date,
        case when rt.refund_date < s.end_date then rt.refund_date
             when s.end_date is null then rt.refund_date
             else s.end_date
        end end_date,
        s.plan_id,
        s.division,
        s.source_name,
        s.service_name,
        s.status,
        s.channel,
        seg.id initial_segment_id,
        case when seg.segment_type = 'TRIAL' and s.start_date > '20160301'
             then 1 else 0
        end trial_start_flag,
        case when ds.subscription_id is null then 0 else 1 end duplicate_flag,
        case when psub.subscription_id is null then 0 else 1 end paid_subscription_flag
    from gcsi.t_subscriptions s
    join gcsi.users u on u. id = s.user_id
    join drupal.gcsi_users d on u.uuid = d.user_uuid
    join gcsi.plan_segments ps on s.plan_id = ps.plan_id and segment_sequence = 0
    join gcsi.segment seg on ps.segment_id = seg.id
    left join moiram.refund_transactions rt on rt.subscription_id = s.subscription_id
    left join moiram.paid_subscriptions psub on psub.subscription_id = s.subscription_id
    left join 
        (select subscription_id from gcsi.subscription_event se
         join gcsi.subscription_cancellation_event sce 
             on se.id = sce.id and
                 sce.reason = 'DUPLICATE_SUBSCRIPTION') ds
    on ds.subscription_id  = s.subscription_id
    where (s.start_date <> s.cancel_date or s.cancel_date is null) 
      and (s.start_date <> s.end_date or s.end_date is null)
      and s.start_date <> s.paid_through_date
      and s.source_name IN ('Gaiam TV','MYO')) t
 where t.duplicate_flag = 0
   and (t.paid_subscription_flag = 1 or t.trial_start_flag = 1);

--alter table moiram.dwh_subscriptions owner to dw_admin;

drop table if exists moiram.next_starts;


 --- start with subscriptions, order them by paid_through_date, start_date and
 --- get the start and end of the next subscription for this user_id
create table moiram.next_starts as
select gcsi_user_id, subscription_id,
    start_date, cancel_date, paid_through_date,
    status, 
    subscription_age,
    lead(start_date) over w  next_start,
    lead(cancel_date)over w  next_end
from
    (select  gcsi_user_id, subscription_id, 
        paid_through_date,
        status, 
        age(paid_through_date,start_date) subscription_age,
        coalesce(cancel_date,end_date) cancel_date,
        start_date
    from moiram.dwh_subscriptions) t
window w as (
    partition by gcsi_user_id
    order by paid_through_date, start_date, subscription_id
    rows between unbounded preceding and unbounded following);
 
 --alter table moiram.next_starts owner to dw_admin;

drop table if exists moiram.subscriptions_tmp;

create table moiram.subscriptions_tmp as
select ns.*,
       -- first attempt at "un-overlapping".  If two subscriptions just butt up against each
       -- other (e.g. plan changes) this will be sufficient.
       case when status in ('Active','Hold', 'AdminHold') then '2999-12-31'::date
            when (start_date,cancel_date) overlaps (next_start, coalesce(next_end, current_date))
                then next_start - interval '1 day'
            when cancel_date is null and next_start is not null
                then next_start - interval '1 day'
            when next_start is null then '2999-12-31'::date
            else cancel_date
       end  valid_to
from  moiram.next_starts ns;
--alter table moiram.subscriptions_tmp owner to dw_admin;

delete from moiram.subscriptions_tmp
where valid_to < start_date;

-- two subscriptions with same start_date, different cancel dates
-- delete the one with the shorter age
delete from moiram.subscriptions_tmp st1
where EXISTS
   (select subscription_id
    from tmp.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and st1.start_date::date = st2.start_date::date
      and st1.subscription_age < st2.subscription_age);
      
--update next_starts and valid_to dates
drop table if exists moiram.next_starts;


 --- start with subscriptions, order them by paid_through_date, start_date and
 --- get the start and end of the next subscription for this user_id
create table moiram.next_starts as
select gcsi_user_id, subscription_id, subscription_age,
    lead(start_date) over w  next_start,
    lead(cancel_date)over w  next_end
from
    (select   gcsi_user_id, subscription_id, 
        paid_through_date,
        subscription_age, 
        cancel_date,
        start_date
    from moiram.subscriptions_tmp) t
window w as (
    partition by gcsi_user_id
    order by paid_through_date, start_date
    rows between unbounded preceding and unbounded following);
 
create index idx_ns_sid on moiram.next_starts using btree (subscription_id);


 update moiram.subscriptions_tmp st set next_start = 
    (select next_start from moiram.next_starts ns where st.subscription_id = ns.subscription_id);
    
 update moiram.subscriptions_tmp st set next_end = 
    (select next_end from moiram.next_starts ns where st.subscription_id = ns.subscription_id);
    
 update moiram.subscriptions_tmp st set valid_to =
    case when status in ('Active','Hold', 'AdminHold') then '2999-12-31'::date
               when next_start is null and next_end is null then '2999-12-31'::date
         when (start_date,cancel_date) overlaps (next_start, coalesce(next_end,current_date))
             then next_start - interval '1 day'
         when cancel_date is null and next_start is not null
             then next_start - interval '1 day'
         else cancel_date
    end;

-- two subscriptions with that still overlap (different start_dates)
-- never delete an active subscription
delete from moiram.subscriptions_tmp st1
where EXISTS
   (select subscription_id
    from moiram.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and (st1.start_date,st1.valid_to) overlaps (st2.start_date, st2.valid_to)
      and  st1.subscription_age < st2.subscription_age
    )
and cancel_date is not null;


-- two subscriptions with that still overlap (different start_dates)
-- two actives that haven't been caught by Travis' script yet
delete from moiram.subscriptions_tmp st1
where EXISTS
   (select subscription_id
    from moiram.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and (st1.start_date,st1.valid_to) overlaps (st2.start_date, st2.valid_to)
      and  st2.status = 'Active'
 --     and  st2.start_date < st1.start_date
    );

-- this should be zero; testing for overlaps
select count(1)
from  moiram.subscriptions_tmp st1
where EXISTS
   (select subscription_id
    from moiram.subscriptions_tmp st2
    where st1.gcsi_user_id = st2.gcsi_user_id
      and st1.subscription_id <> st2.subscription_id
      and (st1.start_date,st1.valid_to) overlaps (st2.start_date, st2.valid_to));

select count(1) from moiram.subscriptions_tmp where valid_to is null;

drop table if exists moiram.t_uniq_subs;

-- now use this to create a uniq_subs_table
create table moiram.t_uniq_subs as
(select t.drupal_user_id,
    t.gcsi_user_id,
    t.subscription_id,
    t.start_date,
    t.next_review_date,
--we're only using these dates to serialize the subscriptions,
-- we go back to t_subscriptions for real data when building user_d
    case when t.cancel_date is not null
         then least(t.cancel_date, d1.valid_to)
         else NULL
    end  cancel_date,
    case when t.end_date is not null
         then least(t.end_date, d1.valid_to)
        else NULL
    end  end_date,
    least(t.paid_through_date,d1.valid_to) paid_through_date,
    t.plan_id,
    t.division,
    t.source_name,
    t.service_name,
    t.channel,
    t.trial_start_flag,
    t.initial_segment_id
from moiram.dwh_subscriptions t
join moiram.subscriptions_tmp d1
on t.subscription_id = d1.subscription_id);

--alter table moiram.t_uniq_subs owner to dw_admin;
create index idxm_uniq_sid on moiram.t_uniq_subs using btree (subscription_id);
create index idxm_uniq_gcsiid on moiram.t_uniq_subs using btree (gcsi_user_id);

drop table if exists moiram.users_tmp;

create table moiram.users_tmp as select
    s.gcsi_user_id,
    s.subscription_id,
    s.start_date,
    s.cancel_date,
    s.paid_through_date,
    s.next_review_date,
    s.end_date,
    s.trial_start_flag,
    lead(s.subscription_id) over w next_subscription_id,
    lead(s.start_date) over w next_start_date
from
    moiram.t_uniq_subs s
window w as (
partition by s.gcsi_user_id
order by start_date,
    paid_through_date,
    end_date
rows between unbounded preceding and unbounded following);

--alter table moiram.users_tmp owner to dw_admin;



drop table  if exists  moiram.tmp_holds_prelim;

create table  moiram.tmp_holds_prelim as
    (select distinct on (gcsi_user_id,sh.subscription_id, hold_start_date)
        sh.id,
        s.gcsi_user_id,
        sh.subscription_id,
        sh.start_date::date as hold_start_date,
        sh.end_date::date as hold_end_date
    from
        gcsi.subscription_hold sh
    join moiram.users_tmp s on sh.subscription_id = s.subscription_id
    where
        deleted = 'f' and
        s.start_date <= sh.start_date
    order by gcsi_user_id, subscription_id, hold_start_date);

-- if the subscription has a cancel_date and an open hold
-- set the hold_end date to the cancel_date
update moiram.tmp_holds_prelim h
set hold_end_date = ts.cancel_date::date
from gcsi.t_subscriptions ts
where h.subscription_id = ts.subscription_id
  and h.hold_end_date is NULL
  and ts.cancel_date is not null;

--alter table moiram.tmp_holds_prelim owner to dw_admin;

drop table  if exists  moiram.tmp_holds;

create table moiram.tmp_holds as
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
from  moiram.tmp_holds_prelim h1
left join
    (select id, min(new_end_date) new_end_date,
                max(start_date) last_start_date
     from
        (select h3.*,
               h4.id  id2,
               h4.hold_end_date new_end_date,
               h4.hold_start_date start_date
        from moiram.tmp_holds_prelim h3
        join moiram.tmp_holds_prelim h4
          on h3.subscription_id = h4.subscription_id and
             h3.hold_start_date < h4.hold_start_date
        where h3.hold_end_date is null) t
     group by id) h2
    on h1.id  = h2.id);

drop sequence if exists hold_groups;
create sequence hold_groups;

--alter table moiram.tmp_holds owner to dw_admin;
alter sequence hold_groups owner to dw_admin;

drop table if exists moiram.hold_overlaps2;

-- take care of overlapping holds
create table moiram.hold_overlaps2 as
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
    from  moiram.tmp_holds h1
    where EXISTS
        (select subscription_id
         from moiram.tmp_holds h2
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

--alter table moiram.hold_overlaps2 owner to dw_admin;

update moiram.hold_overlaps2 h1 set hold_group =
   (select max(hold_group) from moiram.hold_overlaps2 h2
    where h2.hold_number < h1.hold_number AND
         h2.gcsi_user_id = h1.gcsi_user_id and
         h2.subscription_id = h1.subscription_id)
where hold_group = 0;

delete from moiram.tmp_holds where id in (select id from moiram.hold_overlaps2 where hold_overlap > 0);

update moiram.tmp_holds h
set hold_end_date = o.hold_end_date
FROM
    (select * from
        (select hold_overlap,
               hold_group,
               first_value(id) over w id,
               last_value(hold_end_date) over w hold_end_date
        from moiram.hold_overlaps2
        window w as (
           partition by hold_group
           order by hold_number
           rows between unbounded preceding and unbounded following )) t
    where hold_overlap = 0) o
where h.id = o.id;



truncate table moiram.tmp_activity;
--handle paid_starts and trial_starts differently

insert into moiram.tmp_activity 
(select distinct on (subscription_id) gcsi_user_id,
    subscription_id,
    start_date as activity_date,
    'Start' activity
from moiram.users_tmp
where trial_start_flag = 0);


insert into moiram.tmp_activity 
(select distinct on (subscription_id) gcsi_user_id,
    subscription_id,
    start_date as activity_date,
    'Trial Start' activity
from moiram.users_tmp
where trial_start_flag = 1);

insert into moiram.tmp_activity 
(select distinct on (ut.subscription_id) gcsi_user_id,
    ut.subscription_id,
    first_shedule_date as activity_date,
    'Start' activity
from moiram.users_tmp ut
join  --first schedule date is trial end
   (select  se.subscription_id, 
        min(se.schedule_date) first_shedule_date
   from gcsi.subscription_event se 
   join gcsi.subscription_transaction_subscription_events stse 
       on stse.subscription_event_id = se.id
   join gcsi.subscription_transaction st 
       on st.id = stse.subscription_transaction_id
    group by subscription_id) fst
   on ut.subscription_id = fst.subscription_id
where trial_start_flag = 1);

insert into moiram.tmp_activity
(select distinct on (subscription_id) gcsi_user_id,
    subscription_id,
    end_date as activity_date,
    'End' activity
from moiram.users_tmp
where end_date is not null and
    (end_date::date > cancel_date::date or
     cancel_date is null));


insert into moiram.tmp_activity (
select distinct on (subscription_id) gcsi_user_id,
    subscription_id,
    paid_through_date as activity_date,
    'Paid_Through' activity
from moiram.users_tmp u1
where paid_through_date::date > cancel_date::date
and paid_through_date::date > end_date::date
and not exists  -- no overlapping subscription
    (select subscription_id
     from moiram.users_tmp u2
     where u1.gcsi_user_id = u2.gcsi_user_id
       and u1.subscription_id <> u2.subscription_id
       and u1.start_date < u2.start_date
       and u2.start_date::date <= u1.paid_through_date::date));


insert into moiram.tmp_activity (
select distinct
    on (subscription_id) gcsi_user_id,
    subscription_id,
    cancel_date as activity_date,
    'Cancel' activity
from moiram.users_tmp
where cancel_date is not null);


insert into moiram.tmp_activity (
select distinct
    on (subscription_id, hold_start_date) gcsi_user_id,
    subscription_id,
    hold_start_date as activity_date,
    'Hold Start' activity,
    id
from moiram.tmp_holds
where hold_start_date is not null);

-- we don't future activities in the user dimension
insert into moiram.tmp_activity (
select distinct
    on (subscription_id, hold_end_date) gcsi_user_id,
    subscription_id,
    hold_end_date as activity_date,
    'Hold End' activity,
    id
from moiram.tmp_holds
where hold_end_date is not null
  and hold_end_date::date <= current_date);


 -- the following adjustments came about from testing the creation
 -- of the user dimension and finding errors


 -- this is primarily to handle the case where Travis swept the database and
 -- multiple subscriptions for a user have the same cancel date.  If a subscription has a
 -- cancel or hold end activity that overlaps the start of the next subscription start, force it
 -- to one day before the subscription start

with t1 as
(select subscription_id,
        next_start_date::date - interval '1 day' cancel_date
from moiram.users_tmp)
update moiram.tmp_activity ta set activity_date = t1.cancel_date
from t1
where (activity = 'Cancel' OR
       activity = 'End' OR
       activity = 'Hold End')
  and activity_date::date > t1.cancel_date::date
  and ta.subscription_id = t1.subscription_id;


 -- when I have a 'Hold End' activity that's the
 -- same date (or later) as the cancel date, delete the 'Hold End' record
delete from moiram.tmp_activity ta
where activity = 'Hold End'
  and exists
     (select ta.subscription_id
      from moiram.tmp_activity t1
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date <= ta.activity_date::date
        and t1.activity = 'Cancel');

 -- when I have a 'Hold End' activity that's the
 -- same date (or later) as the start date, delete the 'Hold End' record
-- this can happen on a trial subscription
delete from moiram.tmp_activity ta
where activity = 'Hold End'
  and exists
     (select ta.subscription_id
      from moiram.tmp_activity t1
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date > ta.activity_date::date
        and t1.activity = 'Start');


 -- when I have a 'Hold Start' activity that's the
 -- same date as the cancel date, delete the 'Hold Start' record
delete from moiram.tmp_activity ta
where activity = 'Hold Start'
  and exists
     (select ta.subscription_id
      from moiram.tmp_activity t1
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date <= ta.activity_date::date
        and t1.activity = 'Cancel');

-- when I have a 'Hold End' and a new 'Hold Start' on the same day, delete the 'Hold End'
delete from moiram.tmp_activity ta
where activity = 'Hold End'
  and exists
     (select ta.subscription_id
      from moiram.tmp_activity t1
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date = ta.activity_date::date
        and t1.activity = 'Hold Start'
        and t1.row_number > ta.row_number);

 -- when I have two consecutive 'Hold Start' activities  delete the  second one
delete from moiram.tmp_activity ta
where activity = 'Hold Start'
  and (subscription_id, activity_date) in
       (select subscription_id, activity_date from
          (select gcsi_user_id, subscription_id, activity_date, activity,
                 lag(activity) over
                      (partition by subscription_id order by activity_date
                       rows between unbounded preceding
                       and unbounded following)  prev_activity
          from moiram.tmp_activity) t
where activity = 'Hold Start' and prev_activity = 'Hold Start');

 -- when I have two consecutive 'Hold End' activities  delete the  first one
delete from moiram.tmp_activity ta
where activity = 'Hold End'
  and (subscription_id, activity_date) in
       (select subscription_id, activity_date from
          (select gcsi_user_id, subscription_id, activity_date, activity,
                 lead(activity) over
                      (partition by subscription_id order by activity_date
                       rows between unbounded preceding
                       and unbounded following)  next_activity
          from moiram.tmp_activity) t
where activity = 'Hold End' and next_activity = 'Hold End');

 -- when I have  'Hold Start' and 'Hold End' on the same day, bump the 'Hold End' by one DAY
-- so I don't have overlapping valid_from/valid_to dates
update moiram.tmp_activity ta
set activity_date = activity_date + interval '1 day'
where activity = 'Hold End'
  and exists
     (select ta.subscription_id
      from moiram.tmp_activity t1
      where t1.subscription_id = ta.subscription_id
        and t1.activity_date::date = ta.activity_date::date
        and t1.activity = 'Hold Start'
        and t1.row_number = ta.row_number);




 --OK!!! Finally ready to create the table with valid_from and valid_to dates
drop table if exists moiram.user_activity;

create table moiram.user_activity as (
    select
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
      from moiram.tmp_activity t
      window w as (
            partition by t.gcsi_user_id
            order by
                activity_date,
                row_number rows between unbounded preceding
            and unbounded following
      )
    ) t1
);


create index idxm_ua_sid on moiram.user_activity using btree(subscription_id);
create index idxm_ua_sidvf on moiram.user_activity using btree(subscription_id, valid_from);

--alter table moiram.user_activity owner to dw_admin;

--one more thing to clean up situation wihere Start and Hold Start have same date

drop table if exists moiram.start_hold_tmp;

create table moiram.start_hold_tmp as
select u1.* ,  u2.valid_to hold_valid_to, u2.current_record hold_cur_rec
from moiram.user_activity u1
join moiram.user_activity u2
  on  u1.subscription_id = u2.subscription_id and
      u1.valid_from = u2.valid_from
where u1.activity = 'Start'
  and u2.activity = 'Hold Start';

--alter table moiram.start_hold_tmp owner to dw_admin;

update moiram.user_activity ua
set valid_to = greatest(st.valid_to, st.hold_valid_to),
    current_record = greatest(st.current_record, st.hold_cur_rec),
    activity = 'Start/Hold'
from moiram.start_hold_tmp st
where ua.subscription_id = st.subscription_id
  and ua.valid_from = st.valid_from
  and ua.activity = 'Start';

delete from moiram.user_activity ua using moiram.start_hold_tmp st
where ua.subscription_id = st.subscription_id
  and ua.valid_from = st.valid_from
  and ua.activity = 'Hold Start';

-- if there are Start and Cancel records with same
-- valid_from/valid_to dates, delete the Start record
delete from moiram.user_activity ua
where (ua.activity = 'Start' or ua.activity = 'Trial Start')
  and ua.valid_from = ua.valid_to
  and EXISTS
    (select subscription_id
     from moiram.user_activity ub
     where ua.subscription_id = ub.subscription_id
       and ub.activity = 'Cancel'
       and ua.valid_from = ub.valid_from);



drop table if exists moiram.segments;

create table moiram.segments as
select ua.subscription_id, 
        ua.activity,
        ua.valid_from,
        ua.valid_to,
        ua.current_record,
        case when t3.segment_id is null and tus.trial_start_flag =1 then 1 
             when ua.prev_activity = 'Trial Start' and activity in  ('Start/Hold', 'Hold Start') then 1
             else 0 
        end trial_segment,
        coalesce (t3.segment_id, tus.initial_segment_id) segment_id,       
        seg.duration_period::interval plan_period
from moiram.t_uniq_subs tus 
join 
     (select u1.*,
             lag(activity) 
                over (partition by subscription_id
                order by valid_from
                rows between unbounded preceding and unbounded following) prev_activity
       from moiram.user_activity u1) ua
    on ua.subscription_id = tus.subscription_id
left join 
    (select subscription_id, valid_from,
            max(t1.segment_id) segment_id
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
             from moiram.user_activity ua
             join gcsi.subscription_event se on ua.subscription_id = se.subscription_id
             join gcsi.subscription_plan_event spe on se.id = spe.id
             where se.schedule_date::date <= ua.valid_to) t) t1
    group by subscription_id, valid_from) t3
  on ua.subscription_id = t3.subscription_id and 
     ua.valid_from = t3.valid_from
join gcsi.segment seg  on coalesce (t3.segment_id, tus.initial_segment_id) = seg.id;


--alter table moiram.segments owner to dw_admin;

drop table if exists moiram.user_status;

create table moiram.user_status as
select useg.*,
    case when useg.activity = 'Trial Start' then 'Trial'
         when useg.trial_segment = 1 then
         case when useg.activity = 'Hold End' then 'Trial'
              when useg.activity in ('Start/Hold', 'Hold Start') then 'Trial Hold'
              when useg.activity in ('Cancel', 'End','Paid_Through') then 'Trial Cancelled'
         end
         else
         case when useg.activity in ('Start', 'Hold End') then 'Active'
             when useg.activity in ('Start/Hold', 'Hold Start') then 'Hold'
             when useg.activity in ('Cancel', 'End','Paid_Through') then 'Cancelled'
         end 
    end status
from moiram.segments useg;

create index idxm_ustat_sid on moiram.user_status (subscription_id);
    
--alter table moiram.user_status owner to dw_admin;

-- these are subscription-level attributes not in t_subscriptions
drop table if exists moiram.subscription_attributes;

create table moiram.subscription_attributes as
select us.gcsi_user_id,
       us.subscription_id,
       p.product_name plan_name,
       c.department campaign_dept,
       c.campaign_segment
from moiram.t_uniq_subs us
left join gcsi.t_plans p on us.plan_id = p.plan_id
left join common.campaign_tracking c on us.channel = c.reported_channel;

--alter table moiram.subscription_attributes owner to dw_admin;
create index idxm_sattr_sid on moiram.subscription_attributes (subscription_id);

 -- REAL user behavior segment
drop table if exists moiram.user_behavior;

create table moiram.user_behavior as
    (select distinct on (uid)
           uid drupal_user_id,
           segment_name
    from common.user_behavior_segmentation
    group by uid, segment_name, engagement_ratio
    order by uid, engagement_ratio desc);

--alter table moiram.user_behavior owner to dw_admin;
create index idxm_tub_uid on moiram.user_behavior (drupal_user_id);

-- onboarding_segment
drop table if exists moiram.onboarding_segments;

create table moiram.onboarding_segments as
(select t.drupal_user_id,
    uo.segment_name onboarding_segment,
    uo.parent_segment onboarding_parent
 from
    (select distinct on (uid)
         uid drupal_user_id,
         choice
    from drupal.user_onboard_event_log
    group by uid, choice, timestamp
    order by uid, timestamp desc) t
left join common.user_onboard_segments uo
    on t.choice = uo.segment_id);

--alter table moiram.onboarding_segments owner to dw_admin;
create index idxm_tos_uid on moiram.onboarding_segments (drupal_user_id);

-- playlist activity segment
drop table if exists moiram.playlist_segments;

create table moiram.playlist_segments as
 (select distinct on (drupal_user_id)
     drupal_user_id,
     reporting_segment segment_name
  from common.playlist_activity pa
  join common.video_d v on pa.page_nid = v.page_nid
  where pa.page_nid not in
      ( 29587,79501,88651,89331,94881,
         70241,78711,79816,81426,86046,
         55316,81226,82296,83481,85796,
         16034,34871,39356,43976,46186,
         3716,30463,30525,30936,31021,
         2755,3204,42231,44801,49641)
  group by drupal_user_id, reporting_segment
  order by drupal_user_id, count(1) desc);

--alter table moiram.playlist_segments owner to dw_admin;
create index idxm_tps_uid on moiram.playlist_segments (drupal_user_id);

 -- guide segment

drop table if exists moiram.guide_segments;

create table moiram.guide_segments as
 (select distinct on (uid)
        uid drupal_user_id,
        gd.site_segment segment_name
 from drupal.flag_content fc
 join common.guide_d gd on fc.content_id = gd.guide_nid
 where fc.fid = 11
 group by uid, site_segment
 order by uid, count(1) desc);

--alter table moiram.guide_segments owner to dw_admin;
create index idxm_tgs_uid on moiram.guide_segments (drupal_user_id);

-- cid code segment
drop table if exists moiram.campaign_segments;

create table moiram.campaign_segments as
    (select distinct on (gcsi_user_id)
        gcsi_user_id,
        campaign_segment segment_name
     from moiram.subscription_attributes
     where campaign_segment in ('My Yoga', 'Seeking Truth', 'Spiritual Growth')
     order by gcsi_user_id, subscription_id desc);

--alter table moiram.campaign_segments owner to dw_admin;
create index idxm_tcs_gcsiid on moiram.campaign_segments (gcsi_user_id);




drop table if exists moiram.user_attributes;

create table moiram.user_attributes as (
select t.*,
    os.onboarding_segment,
    os.onboarding_parent,
    ubs.segment_name ubs_segment_name,
    ps.segment_name  playlist_segment_name,
    gs.segment_name  guide_segment_name,
    cs.segment_name  campaign_segment_name,
    case when ubs.segment_name is not null and
              ubs.segment_name <> 'No Clear Segment' then ubs.segment_name
         when ps.segment_name is not null then ps.segment_name
         when gs.segment_name is not null then gs.segment_name
         when cs.segment_name is not null then cs.segment_name
         else null
    end user_behavior_segment
from
    (select  -- user_start_date
        drupal_user_id,
        gcsi_user_id,
        min (start_date :: date) user_start_date
    from
        moiram.t_uniq_subs
   group by drupal_user_id, gcsi_user_id) t
left join moiram.onboarding_segments os
   on t.drupal_user_id = os.drupal_user_id
left join moiram.user_behavior ubs
  on t.drupal_user_id = ubs.drupal_user_id
left join moiram.playlist_segments ps
  on t.drupal_user_id = ps.drupal_user_id
left join moiram.guide_segments gs
  on t.drupal_user_id = gs.drupal_user_id
left join moiram.campaign_segments cs
  on t.gcsi_user_id = cs.gcsi_user_id);

--alter table moiram.user_attributes owner to dw_admin;
create index idxm_uattr_user on moiram.user_attributes (gcsi_user_id);
create index idxm_uattr_duid on moiram.user_attributes (drupal_user_id);

drop table if exists moiram.suh_segments;

create table moiram.suh_segments as
select distinct on (drupal_user_id)
    drupal_user_id,
    reporting_segment segment_name
from
            (select coalesce(uid, client_uid) drupal_user_id, reporting_segment
       from drupal.smfplayer_user_history suh
       join moiram.video_d v
          on suh.nid = v.media_nid
       where coalesce(uid, client_uid) in
          (select drupal_user_id from moiram.user_attributes
           where user_behavior_segment is null)) ss
  group by drupal_user_id, reporting_segment
  order by drupal_user_id, count(1) desc;

--alter table moiram.campaign_segments owner to dw_admin;

update moiram.user_attributes ua
set user_behavior_segment =
(select segment_name from moiram.suh_segments ss
 where ua.drupal_user_id = ss.drupal_user_id)
where user_behavior_segment is null;

DROP TABLE if exists moiram.user_d cascade;

CREATE TABLE moiram."user_d" (
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
"status"                    text COLLATE "default",
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



insert into moiram.user_d
   (drupal_user_id, gcsi_user_id, user_start_date, user_end_date,
        subscription_id, subscription_start_date, paid_through_date,
        next_review_date, cancel_date, subscription_end_date, 
        status,  plan_id, segment_id, plan_name, plan_period,
        cid_channel, campaign_dept, user_behavior_segment, source_name,
        service_name, winback, onboarding_segment, onboarding_parent,
    valid_from, valid_to, current_record)
(select t.drupal_user_id,
        t.gcsi_user_id,
        ut.user_start_date,
        case when t.end_date < us.valid_to
             then t.end_date
             else NULL
        end user_end_date,
        t.subscription_id,
        t.start_date subscription_start_date,
        paid_through_date,
        t.next_review_date,
        case when t.cancel_date < us.valid_to then t.cancel_date
             when activity = 'Cancel' then  us.valid_from
             else NULL
        end cancel_date,
        case when t.end_date < us.valid_to
             then t.end_date
             else NULL
        end subscription_end_date,       
        us.status,
        t.plan_id,
        us.segment_id,
        s.plan_name,
        us.plan_period,
        t.channel,
        s.campaign_dept,
        ut.user_behavior_segment,
        t.source_name,
        t.service_name,
        case when ut.user_start_date::date < t.start_date::date
             then 'Winback'
             else 'New User'
        end winback,
        ut.onboarding_segment,
        ut.onboarding_parent,
        us.valid_from,
        us.valid_to,
        us.current_record
from moiram.t_uniq_subs t
join moiram.user_attributes ut
   on t.gcsi_user_id = ut.gcsi_user_id
join moiram.subscription_attributes s
   on t.subscription_id = s.subscription_id
join moiram.user_status us
   on t.subscription_id = us.subscription_id
order by gcsi_user_id, valid_from);