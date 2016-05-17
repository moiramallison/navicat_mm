

drop table if exists test_2553;

create table test_2553 as 
(select * from 
    (select "EMAIL_ADDRESS" email_address,
        case when "Guide" = 'CC' then 'Conscious Cleanse'
             when "Guide" = 'LTM' then 'How to Meditate' 
             when "Guide" = 'BY' then 'The Balanced You' 
        end guide_title,
        du.uid drupal_user_id,
        ud.gcsi_user_id
        from fb_reengagement_2553 fb
     left join drupal.users du  on lower(fb."EMAIL_ADDRESS") = lower(du.mail)
     left join common.user_dim ud on du.uid = ud.drupal_user_id) t
  where drupal_user_id is not null);

select count(distinct drupal_user_id) from test_2553;

drop table if exists tmp.users_2553;

create table tmp.users_2553 as 
(select b.*,
  sd.subscription_id, 
  dss.paid_through_date,
  case when cancel_date < dss.paid_through_date then cancel_date else null end cancel_date,
  case when guide_title = 'The Balanced You'
       then opt_in_date + 31
       else opt_in_date + 24
  end window_end
 from
    (select distinct on (gcsi_user_id)
       gcsi_user_id,
       drupal_user_id,
       guide_title,
       test_control,
       opt_in_date::date opt_in_date
     from
        (select gox.*,
            case when t.drupal_user_id is null then 'Control' else 'Test' end  test_control
        from common.guide_opt_ins gox
        left join test_2553 t
            on gox.drupal_user_id = t.drupal_user_id AND
                 gox.guide_title = t.guide_title
        where gox.guide_title in  ('Conscious Cleanse', 'How to Meditate', 'The Balanced You')
          and opt_in_date > '20151226'
          and opt_in_date < '20160106') f
     order by gcsi_user_id, test_control desc, opt_in_date) b
left join common.daily_status_snapshot dss on b.gcsi_user_id = dss.gcsi_user_id
    and day_timestamp = '20160106'
left join common.subscription_d sd on dss.subscription_id = sd.subscription_id);

select test_control, count(1)
from tmp.users_2553
group by test_control;


select count(distinct drupal_user_id) from tmp.users_2553;

drop table if exists tmp.guide_views_2553;

create table tmp.guide_views_2553 as
select  distinct u.drupal_user_id, gd.guide_title,  gd.guide_day, e.id
from tmp.users_2553 u
join common.qualified_guide_views e 
    on u.drupal_user_id = e.user_id and 
       u.guide_title = e.guide_title
join common.guide_d gd 
    on e.nid = gd.media_nid and 
       u.guide_title = gd.guide_title
where to_timestamp(created)  > opt_in_date
  and to_timestamp(created)  < window_end;


select count(distinct drupal_user_id) from tmp.guide_views_2553;

drop table if exists tmp.guide_day_completes_2553;

create table tmp.guide_day_completes_2553 as 
select distinct on (u.drupal_user_id, gd.guide_day)
    u.drupal_user_id, 
		gd.guide_day,
		case when coalesce(gdc.drupal_user_id,gv.drupal_user_id) is not null 
         then 1
         else 0
    end guide_day_complete
from tmp.users_2553 u
join (select distinct guide_title, guide_day 
      from common.guide_d) gd on u.guide_title = gd.guide_title
left join common.guide_day_completes gdc 
	  on u.drupal_user_id = gdc.drupal_user_id and 
       u.guide_title = gdc.guide_title and
       gd.guide_day = gdc.guide_day and 
       completion_date > opt_in_date and
       completion_date < window_end
left join tmp.guide_views_2553 gv
    on u.drupal_user_id = gv.drupal_user_id and 
       gd.guide_day = gv.guide_day
order by drupal_user_id, guide_day, completion_date;

select count(distinct drupal_user_id) from tmp.guide_day_completes_2553;

drop table if exists tmp.video_views_2553;

create table tmp.video_views_2553 as
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
    from tmp.users_2553 u
    left join 
        (select u1.drupal_user_id, q.id
         from tmp.users_2553 u1
         join common.qualified_video_views q 
           on u1.drupal_user_id = q.user_id
         where to_timestamp(created)  > opt_in_date
           and to_timestamp(created)  < window_end) qvv
      on u.drupal_user_id = qvv.drupal_user_id
    left join 
        (select  u3.drupal_user_id, e.id
         from tmp.users_2553 u3
         join common.engaged_video_views e 
           on u3.drupal_user_id = e.user_id
         where to_timestamp(created)  > opt_in_date
           and to_timestamp(created)  < window_end) evv
      on u.drupal_user_id = evv.drupal_user_id  and 
         qvv.id = evv.id
    left join tmp.guide_views_2553 gv
      on qvv.id = gv.id ) t2
group by drupal_user_id;

select count(distinct drupal_user_id) from tmp.video_views_2553;

drop table if exists tmp.results_2553;

create table tmp.results_2553 as 
select gcsi_user_id, test_control,
       case when vv.qualified_video_views > 0 then 1 else 0 end qualified_video_view_flag,
       case when vv.engaged_video_views > 0 then 1 else 0 end engaged_video_view_flag,
       case when gdc.num_guide_days > 0 then 1 else 0 end guide_day_complete_flag,
       vv.qualified_video_views,
       vv.engaged_video_views,
       gdc.num_guide_days,
       case when u2.cancel_date is null then 'Renewed' else 'Cancelled' end renewal_status
from tmp.users_2553 u2
join tmp.video_views_2553 vv on u2.drupal_user_id = vv.drupal_user_id
join 
		(select drupal_user_id, sum(guide_day_complete) num_guide_days
     from tmp.guide_day_completes_2553
     group by drupal_user_id) gdc
		on u2.drupal_user_id = gdc.drupal_user_id;

select distinct test_control, num_guide_days, ntile2
from 
(select test_control, num_guide_days,
       ntile(2) over (partition by test_control order by num_guide_days) ntile2
from tmp.results_2553) foo
order by test_control, num_guide_days;

select * from tmp.results_2553;


    

