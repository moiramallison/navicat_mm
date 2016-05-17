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
        from tmp.dwh_subscriptions) a
    window w as (
    partition by gcsi_user_id
    order by paid_through_date, start_date
    rows between unbounded preceding and unbounded following
    )
 order by gcsi_user_id, start_date, subscription_age, subscription_id) b;
--alter table moiram.subscriptions_tmp owner to dw_admin;