/*
select * from moiram.daily_status_y2014q1 where gcsi_user_id = 14239253
order by day_timestamp;


select * from tmp.paid_through_dates where subscription_id = 11760905;

select count(1) from moiram.daily_status_y2015q2;

select count(1) from common.daily_status_y2015q2;

drop table t2;

create table t2 as
select day_timestamp, count(1) num_rows, count(distinct gcsi_user_id) num_users from moiram.daily_status_y2014q1
group by day_timestamp;

--select * from t2 where num_rows <> num_users;

select * 
from moiram.daily_status_y2014q1
where day_timestamp = '20140101'
  and gcsi_user_id in 
    (select gcsi_user_id 
     from moiram.daily_status_y2014q1
      where day_timestamp = '20140101'
    group by gcsi_user_id
    having count(1) > 1);


select status, count(1)
from user_d 
where valid_from <= '20140101'
  and valid_to >= '20140101'
group by status;
*/
select status, count(1) 
from daily_status_y2014q1
--where day_timestamp = '20140101'
group by status;

select status, count(1) 
from common.daily_status_y2014q1
--where day_timestamp = '20140101'
group by status;



--except
select day_timestamp, gcsi_user_id, subscription_id
from common.daily_status_y2014q1
where status = 'Active'
EXCEPT
select day_timestamp, gcsi_user_id, subscription_id
from moiram.daily_status_y2014q1
where status = 'Active'
order by gcsi_user_id, day_timestamp

select * from moiram.daily_status_y2014q1
where gcsi_user_id in (168578,12776120, 14582111,14866418)
order by gcsi_user_id, day_timestamp

select * from gcsi.subscription_hold where subscription_id in (13121187,
12776127);



select * from common.user_d where gcsi_user_id in (168578,12776120);

select * from common.subscription_d where gcsi_user_id in (168578,12776120);

select * from common.daily_status_y2014q1 
where day_timestamp = '20140101'
  and gcsi_user_id in (168578,12776120);

select * from moiram.daily_status_y2014q1 
where day_timestamp between  '20140310' and '20140401'
  and gcsi_user_id = 14368036
order by day_timestamp;

select * from gcsi.subscription_hold where subscription_id in (13121187,
12776127);


select * from subscription_d where subscription_id in 
(select subscription_id from moiram.daily_status_y2014q1  where status = 'Cancelled' and day_timestamp = '20140101');

select * from moiram.daily_status_y2014q1  
where status = 'Cancelled' and day_timestamp = '20140101' and paid_through_date >'20140101';

select * from common.user_d where subscription_id in (18864058,31989601);

select * from gcsi.t_subscriptions where user_id = 14291846;

select *  from common.user_d where gcsi_user_id = 14291846;


select distinct u1.gcsi_user_id
from common.user_d u1
join common.user_d u2
  on  u1.gcsi_user_id = u2.gcsi_user_id
  and u1.subscription_id <> u2.subscription_id
  and u1.cancel_date >  '20140101' 
  and u1.cancel_date > u1.paid_through_date
  and u1.cancel_date <= (u1.paid_through_date::date +14)
  and u1.cancel_date::date = u2.subscription_start_date::date - 1;

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
where t.subscription_id = 12776127
      window w as (
            partition by t.gcsi_user_id
            order by
                activity_date,
                row_number rows between unbounded preceding
            and unbounded following
      )
    ) t1
;

  (select t.drupal_user_id,
        t.gcsi_user_id,
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
where t.gcsi_user_id = 12776120
order by gcsi_user_id, valid_from);  

select * from tmp.segments_tmp
where subscription_id = 12776127;

drop table t3; 
create table t3 as
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

select max(valid_from) from tmp.segments_tmp;

create table moiram.segments_tmp as (select * from tmp.segments_tmp);
       