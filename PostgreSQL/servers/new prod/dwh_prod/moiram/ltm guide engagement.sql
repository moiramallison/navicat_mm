

drop table if exists test_3140;

create table test_3140 as 
(select * from 
    (select "EMAIL_ADDRESS" email_address,
				"VERSION_A_B" cohort,
        du.uid drupal_user_id,
        ud.gcsi_user_id
        from "SG_LTM_Analysis" fb
     left join drupal.users du  on lower(fb."EMAIL_ADDRESS") = lower(du.mail)
     left join common.user_dim ud on du.uid = ud.drupal_user_id) t
  where drupal_user_id is not null);

select count(1), count(distinct drupal_user_id) from test_3140;


drop table if exists tmp.users_3140;

create table tmp.users_3140 as 
(select b.*,
  sd.subscription_id, 
  dss.paid_through_date opt_in_ptd,
  sd.paid_through_date current_ptd,
  sd.cancel_date,
  opt_in_date + 24 window_end
 from
    (select distinct on (gcsi_user_id)
       gcsi_user_id,
       drupal_user_id,
       cohort,
       opt_in_date::date opt_in_date
     from
        (select gox.*,
            coalesce(t.cohort, 'C') cohort
        from common.guide_opt_ins gox
        left join test_3140 t
            on gox.drupal_user_id = t.drupal_user_id 
        where gox.guide_title =   'How to Meditate'
          and opt_in_date > '20160101'
          and opt_in_date < '20160322') f
     order by gcsi_user_id, opt_in_date) b
left join common.daily_status_snapshot dss on b.gcsi_user_id = dss.gcsi_user_id
    and day_timestamp = opt_in_date::date
left join common.subscription_d sd on dss.subscription_id = sd.subscription_id);

select cohort, count(1)
from tmp.users_3140
group by cohort;


select count(1), count(distinct drupal_user_id) from tmp.users_3140;

drop table if exists tmp.guide_views_3140;

create table tmp.guide_views_3140 as
select  distinct u.drupal_user_id, gd.guide_title,  gd.guide_day, e.id
from tmp.users_3140 u
join common.qualified_guide_views e 
    on u.drupal_user_id = e.user_id and 
       e.guide_title = 'How to Meditate' 
join common.guide_d gd 
    on e.nid = gd.media_nid and 
       gd.guide_title  = 'How to Meditate' 
where to_timestamp(created)  > opt_in_date
  and to_timestamp(created)  < window_end;


select count(distinct drupal_user_id) from tmp.guide_views_3140;

drop table if exists tmp.guide_day_completes_3140;

create table tmp.guide_day_completes_3140 as 
select distinct on (u.drupal_user_id, gd.guide_day)
    u.drupal_user_id, 
        gd.guide_day,
        case when coalesce(gdc.drupal_user_id,gv.drupal_user_id) is not null 
         then 1
         else 0
    end guide_day_complete
from tmp.users_3140 u
join (select distinct guide_title, guide_day 
      from common.guide_d) gd on  gd.guide_title = 'How to Meditate' 
left join common.guide_day_completes gdc 
      on u.drupal_user_id = gdc.drupal_user_id and 
       gdc.guide_title  = 'How to Meditate' and
       gd.guide_day = gdc.guide_day and 
       completion_date > opt_in_date and
       completion_date < window_end
left join tmp.guide_views_3140 gv
    on u.drupal_user_id = gv.drupal_user_id and 
       gd.guide_day = gv.guide_day
order by drupal_user_id, guide_day, completion_date;

select count(distinct drupal_user_id) from tmp.guide_day_completes_3140;

drop table if exists tmp.video_views_3140;

create table tmp.video_views_3140 as
select drupal_user_id,
        sum(qualified_video_view)   qualified_video_views,
        sum(engaged_video_view)     engaged_video_views
from
    (select u.drupal_user_id, qvv.id,
         case when qvv.id is null then 0 
              when gv.id is null then 1  -- we only want views not associated witht their guide
              else 0  
         end  qualified_video_view,
         case when evv.id is null then 0
              when gv.id is null then 1  -- we only want views not associated witht their guide
              else 0  
         end  engaged_video_view
    from tmp.users_3140 u
    left join 
        (select u1.drupal_user_id, q.id
         from tmp.users_3140 u1
         join common.qualified_video_views q 
           on u1.drupal_user_id = q.user_id
         where to_timestamp(created)  > opt_in_date
           and to_timestamp(created)  < window_end) qvv
      on u.drupal_user_id = qvv.drupal_user_id
    left join 
        (select  u3.drupal_user_id, e.id
         from tmp.users_3140 u3
         join common.engaged_video_views e 
           on u3.drupal_user_id = e.user_id
         where to_timestamp(created)  > opt_in_date
           and to_timestamp(created)  < window_end) evv
      on u.drupal_user_id = evv.drupal_user_id  and 
         qvv.id = evv.id
    left join tmp.guide_views_3140 gv
      on qvv.id = gv.id ) t2
group by drupal_user_id;

select count(distinct drupal_user_id) from tmp.video_views_3140;

drop table if exists tmp.results_3140;

create table tmp.results_3140 as 
select gcsi_user_id, 
       case when cohort = 'A' then 'Regular Comms'
            when cohort = 'B' then 'Dynamic Comms'
            when cohort = 'C' then 'Control'
       end cohort,
       case when vv.qualified_video_views > 0 then 1 else 0 end qualified_video_view_flag,
       case when vv.engaged_video_views > 0 then 1 else 0 end engaged_video_view_flag,
       case when gdc.num_guide_days > 0 then 1 else 0 end guide_day_complete_flag,
       vv.qualified_video_views,
       vv.engaged_video_views,
       gdc.num_guide_days,
       case when u2.current_ptd > u2.opt_in_ptd then 'Renewed' 
            when u2.current_ptd > current_date::date  then 'Renewed'
            when u2.current_ptd = u2.opt_in_ptd then 'Cancelled'
            when u2.cancel_date < u2.opt_in_date then 'Lapsed at Opt-in'
            when u2.cancel_date > u2.opt_in_date and 
                 u2.cancel_date < u2.opt_in_ptd then 'Cancelled'
       end renewal_status
from tmp.users_3140 u2
join tmp.video_views_3140 vv on u2.drupal_user_id = vv.drupal_user_id
join 
        (select drupal_user_id, sum(guide_day_complete) num_guide_days
     from tmp.guide_day_completes_3140
     group by drupal_user_id) gdc
        on u2.drupal_user_id = gdc.drupal_user_id;



select * from tmp.results_3140 where renewal_status is not null;

select cohort, renewal_status, count(1)
from tmp.results_3140
group by cohort, renewal_status
order by cohort, renewal_status;

select cohort, count(*) from tmp.users_3140
where gcsi_user_id in 
   (select gcsi_user_id from tmp.results_3140 where renewal_status is null)
group by cohort;

select * from common.subscription_d where gcsi_user_id in 
(select gcsi_user_id from tmp.results_3140 where renewal_status is null)
and paid_through_date > '20151231';
  