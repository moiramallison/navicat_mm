drop table if exists tmp.last_video_viewed;

create table tmp.last_video_viewed  as 
(select drupal_user_id, 
		    max(media_nid) media_nid,  
        max(title) title,  
        max(series_title) series_title, 
        max(created_date) created_date
from
  (select  drupal_user_id,
          last_value(media_nid) over w as media_nid,
          last_value(created_date) over w as created_date,
          last_value(title) over w as title,
          last_value(series_title ) over w as series_title
    from
        (select  user_id drupal_user_id,vdc.media_nid, vdc.created_date, v.title, v.series_title
         from common.video_daily_cube vdc
         join common.video_d v  on vdc.media_nid = v.media_nid 
         where v.feature = 'Feature'
           and vdc.created_date >= '2015-01-01'::date) foo
    window w as (
    partition by drupal_user_id
    order by created_date
    rows between unbounded preceding and unbounded following
    )) x
group by drupal_user_id) ;

drop table if exists tmp.cancelled_2015;

create table tmp.cancelled_2015 as
select bar.drupal_user_id, last_active_date::date, baz.adjusted_cancel_date
from
    (select drupal_user_id, max(valid_to) last_active_date
     from common.user_d ud
     where status = 'Active'
     group by drupal_user_id) bar
join
   (select foo.* from
        (select cu .drupal_user_id, 
            case when status = 'Cancelled' and 
                      cancel_date between '2015-01-01'::date and  '2015-12-01'::date and
                      paid_through_date < '2015-12-01'::date
                 then cancel_date::date
                 when status = 'Hold' and valid_from <= '2015-11-15' 
                 then valid_from::date
            end  adjusted_cancel_date
          from common.current_users cu) foo
    where adjusted_cancel_date is not null) baz
on bar.drupal_user_id = baz.drupal_user_id;

drop table if exists tmp.tmp_view_b4_cancel;

create table tmp.tmp_view_b4_cancel as
(select cu .drupal_user_id, adjusted_cancel_date, last_active_date, media_nid, title, series_title, created_date::date view_date
from tmp.cancelled_2015 cu
join tmp.last_video_viewed lvv
  on cu .drupal_user_id = lvv.drupal_user_id);


select num_days, count(1)
from
(select last_active_date::date - view_date::date num_days
from tmp.tmp_view_b4_cancel) x
where num_days >=0
group by num_days
order by num_days;

drop table if exists tmp.churn_by_title; 

create table tmp.churn_by_title as 
select f.media_nid, f.title, series_title,  sum(num_views) num_views, count(distinct user_id) num_users, max(churners)
from common.video_monthly_cube vmc
join 
	(select  media_nid, title,  series_title, count(1) churners
	from tmp.tmp_view_b4_cancel
	group by  media_nid, title, series_title) f
on vmc.media_nid = f.media_nid
where month_key >= 201501 
  and month_key <= 201511
group by f.media_nid, f.title, f.series_title;


drop table if exists tmp.video_3month_smry;

create table tmp.video_3month_smry as
(select user_id, media_nid,
       max(per1) per1,
       max(per2) per2,
       max(per3) per3,
       sum(num_views_1) num_views_1,
       sum(num_views_2) num_views_2,
       sum(num_views_3) num_views_3
from
   (select  user_id, media_nid,
        case when month_key >= 201505 and month_key <= 201507 then 1 else 0 end per1,
        case when month_key >= 201506 and month_key <= 201508 then 1 else 0 end per2,
        case when month_key >= 201507 and month_key <= 201509 then 1 else 0 end per3,
        case when month_key >= 201505 and month_key <= 201507 then num_views else 0 end num_views_1,
        case when month_key >= 201506 and month_key <= 201508 then num_views else 0 end num_views_2,
        case when month_key >= 201507 and month_key <= 201509 then num_views else 0 end num_views_3
   from common.video_monthly_cube vmc 
   where month_key >= 201505 
     and month_key <= 201509) foo
group by user_id, media_nid);


 
drop table if exists tmp.video_3month_cube;
 
create table tmp.video_3month_cube as
select v3.media_nid, v.title, v.series_title, v.site_segment,
       p1.num_users num_users_1,
       p2.num_users num_users_2,       
       p3.num_users num_users_3,       
       p1.num_views num_views_1,
       p2.num_views num_views_2,       
       p3.num_views num_views_3             
from (select distinct media_nid from tmp.video_3month_smry) v3
join common.video_d v on v3.media_nid = v.media_nid
left join (select media_nid, count(distinct user_id) num_users,
             sum(num_views_1) num_views 
           from tmp.video_3month_smry 
           where per1 = 1
           group by media_nid) p1 
     on v.media_nid = p1.media_nid
left join (select media_nid, count(distinct user_id) num_users,
             sum(num_views_2) num_views 
           from tmp.video_3month_smry 
           where per2 = 1
           group by media_nid) p2 
     on v.media_nid = p2.media_nid
left join (select media_nid, count(distinct user_id) num_users,
             sum(num_views_2) num_views 
           from tmp.video_3month_smry 
           where per3 = 1
           group by media_nid) p3 
     on v.media_nid = p3.media_nid
;


-- get rid of videos associated with guides:

delete from tmp.video_3month_cube
where media_nid in (select media_nid from common.guide_d);

drop table if exists tmp.churn_by_title; 

create table tmp.churn_by_title as 
select media_nid, f.title, series_title,  sum(num_views) num_views, count(distinct user_id) num_users, max(churners)
from common.video_monthly_cube vmc
join 
	(select  media_nid, title,  series_title, count(1) churners
	from tmp.tmp_view_b4_cancel
	group by  media_nid, title, series_title) f
on vmc.media_nid = f.media_nid
where month_key >= 201501 
  and month_key <= 201511
group by f.media_nid, f.title, f.series_title;

select * from tmp.churn_by_title where series_title is null;

drop table if exists tmp.churn_by_title_cube;

create table tmp.churn_by_title_cube as 
(select 1 period, 
         c3.media_nid,
         title, site_segment,
         coalesce(num_users_1,0)	num_users,
         coalesce(num_views_1,0)  num_views,
         coalesce(churners,0)  num_churners
from tmp.video_3month_cube c3
left join 
	(select  media_nid,  count(1) churners
	 from tmp.tmp_view_b4_cancel
   where adjusted_cancel_date >= '2015-08-01'::date
     and adjusted_cancel_date < '2015-09-01'::date
	 group by  media_nid) foo
on c3.media_nid = foo.media_nid
where series_title is null);

insert into tmp.churn_by_title_cube
(select 2 period, 
         c3.media_nid,
         title, site_segment,
         coalesce(num_users_2,0),	
         coalesce(num_views_2,0) ,
         coalesce(churners,0) 
from tmp.video_3month_cube c3
left join 
	(select  media_nid,  count(1) churners
	 from tmp.tmp_view_b4_cancel
   where adjusted_cancel_date >= '2015-09-01'::date
     and adjusted_cancel_date < '2015-10-01'::date
	 group by  media_nid) foo
on c3.media_nid = foo.media_nid
where series_title is null);

insert into tmp.churn_by_title_cube
(select 3 period, 
         c3.media_nid,
         title, site_segment,
         coalesce(num_users_3,0),
         coalesce(num_views_3,0),
         coalesce(churners,0)
from tmp.video_3month_cube c3
left join 
	(select  media_nid,  count(1) churners
	 from tmp.tmp_view_b4_cancel
   where adjusted_cancel_date >= '2015-10-01'::date
     and adjusted_cancel_date < '2015-11-01'::date
	 group by  media_nid) foo
on c3.media_nid = foo.media_nid
where series_title is null);


drop table if exists tmp.series_3month_smry;

create table tmp.series_3month_smry as    
	(select vs.*, v.series_title, v.site_segment
	from tmp.video_3month_smry vs
	join common.video_d v on vs.media_nid = v.media_nid
    where v.series_title is not null);

-- a couple series have multiple site segments:  fix this here

select series_title, site_segment, count(1) from common.video_d
where series_title in 
   (select series_title from tmp.series_3month_cube 
    group by series_title
    having count(1) > 1)
group by series_title, site_segment
order by series_title;

update tmp.series_3month_smry set site_segment = 'Film & Series' where series_title in
    ('Common Ground', 'Wisdom at Work', 'Conscious Film Series', 'Wisdom of Dreams');

update tmp.series_3month_smry set site_segment = 'My Yoga' where series_title in
   ('Achieving Optimum Health with Dr. Miranda Wiley', 'Deepak Chopra: The Seven Spiritual Laws of Yoga',
    'Moments of Calm', 'Peak Performance Yoga', 'Rodney Yee‘s Daily Yoga');
    
update tmp.series_3month_smry set site_segment = 'Seeking Truth' where series_title in
   ('Beyond Belief', 'Edge Media TV', 'Gardiner’s World', 'On the Edge', 'Open Minds',
    'Techniques of Discovery', 'The Damanhur Federation', 'Wisdom Teachings');
   
update tmp.series_3month_smry set site_segment = 'Spiritual Growth' where series_title in
  ('Book Tours', 'Conscious Media Network',  'Conversations with Remarkable People', 'Corinne Edwards Interviews',
   'Healing Matrix', 'Health Choices', 'INNERVIEWS', 'Inspirations', 'Living in Balance', 'Meditation 101', 
   'Mind Shift', 'Omega','On the Road with Lilou', 'Sacred Weeds', 'Secrets to Health', 'Success 3.0 Summit - Success Talks',
   'Thinking Allowed', 'Well-Being', 'World of Wisdom');
   
update tmp.series_3month_smry set site_segment = 'Spiritual Growth' where series_title like 'Eye of the Spirit%';
update tmp.series_3month_smry set site_segment = 'Seeking Truth' where series_title like 'Reality TV%';

drop table if exists tmp.series_3month_cube;
 
create table tmp.series_3month_cube as    
select st.series_title, st.site_segment, 
    p1.num_users num_users_1,
    p2.num_users num_users_2,       
    p3.num_users num_users_3,       
    p1.num_views num_views_1,
    p2.num_views num_views_2,       
    p3.num_views num_views_3 
from (select distinct series_title, site_segment from tmp.series_3month_smry) st
left join (select series_title, count(distinct user_id) num_users,
             sum(num_views_1) num_views 
           from tmp.series_3month_smry
           where per1 = 1
           group by series_title) p1 
     on st.series_title = p1.series_title
left join (select series_title, count(distinct user_id) num_users,
             sum(num_views_1) num_views 
           from tmp.series_3month_smry
           where per2 = 1
           group by series_title) p2 
     on st.series_title = p2.series_title
left join (select series_title, count(distinct user_id) num_users,
             sum(num_views_1) num_views 
           from tmp.series_3month_smry
           where per3 = 1
           group by series_title) p3 
     on st.series_title = p3.series_title;

drop table tmp.churn_by_series_cube;

create table tmp.churn_by_series_cube as 
(select 1 period, 
         c3.series_title, site_segment, 
         coalesce(num_users_1,0)    num_users,
         coalesce(num_views_1,0)  num_views,
         coalesce(churners,0)  num_churners
from tmp.series_3month_cube c3
left join 
    (select  series_title,  count(1) churners
     from tmp.tmp_view_b4_cancel
   where adjusted_cancel_date >= '2015-08-01'::date
     and adjusted_cancel_date < '2015-09-01'::date
     group by  series_title) foo
on c3.series_title = foo.series_title);

insert into tmp.churn_by_series_cube
(select 2 period, 
         c3.series_title, site_segment,
         coalesce(num_users_2,0),   
         coalesce(num_views_2,0) ,
         coalesce(churners,0) 
from tmp.series_3month_cube c3
left join 
    (select  series_title,  count(1) churners
     from tmp.tmp_view_b4_cancel
   where series_title is not null 
     and adjusted_cancel_date >= '2015-09-01'::date
     and adjusted_cancel_date < '2015-10-01'::date
     group by  series_title) foo
on c3.series_title = foo.series_title);

insert into tmp.churn_by_series_cube
(select 3 period, 
         c3.series_title, site_segment, 
         coalesce(num_users_3,0),
         coalesce(num_views_3,0),
         coalesce(churners,0)
from tmp.series_3month_cube c3
left join 
    (select  series_title,  count(1) churners
     from tmp.tmp_view_b4_cancel
   where adjusted_cancel_date >= '2015-10-01'::date
     and adjusted_cancel_date < '2015-11-01'::date
     group by  series_title) foo
on c3.series_title = foo.series_title);

select * from tmp.churn_by_series_cube
where series_title in 
(select series_title from tmp.churn_by_series_cube where num_users > 300);




select * from tmp.churn_by_title_cube
where media_nid in 
   (select media_nid from tmp.churn_by_title_cube where num_users > 300);

sum(num_views) num_views, count(distinct user_id) num_users, max(churners)
left join tmp.tmp_view_b4_cancel on 
	group by   series_title) f
on vmc.series_title = f.series_title

drop table if exists tmp.series_usage;

create table tmp.series_usage as 
select baz.series_title, baz.num_views, num_users, t.num_churners
from 
    (select series_title, sum(num_views) num_views, count(distinct user_id) num_users
    from 
        (select series_title, num_views, user_id
        from 
            (select  media_nid, num_views, user_id
            from common.video_monthly_cube vmc
            where month_key >= 201501 
              and month_key <= 201511
              and media_nid in (select media_nid from tmp.tmp_view_b4_cancel)) foo
        join common.video_d v on foo.media_nid = v.media_nid) bar
        group by series_title)baz
join 	(select  series_title,  count(distinct drupal_user_id) num_churners
	from tmp.tmp_view_b4_cancel
	group by  series_title) t  
  on baz.series_title = t.series_title;

select * from tmp.series_usage  order by num_users desc;


select count(1) from common.video_daily_cube
where media_nid = 92836
  and created_date between '01'

select * from common.video_d where lower(title) like '%sun is shining%'


(select user_id drupal_user_id,
          last_value(vdc.media_nid) over w as media_nid,
          last_value(vdc.created_date) over w as created_date,
          last_value(v.title
    from common.video_daily_cube vdc
    join common.video_d v  on vdc.media_nid = v.media_nid 
    where v.feature = 'Feature'
    window w as (
    partition by user_id
    order by vdc.created_date
    rows between unbounded preceding and unbounded following
    ))

-- get rid of videos associated with guides:

delete from tmp.video_3month_cube
where media_nid in (select media_nid from common.guide_d);

select * from tmp.churn_by_title_cube
where media_nid in 
   (select media_nid from tmp.churn_by_title_cube where num_users > 300 and site_segment = 'Spiritual Growth');

