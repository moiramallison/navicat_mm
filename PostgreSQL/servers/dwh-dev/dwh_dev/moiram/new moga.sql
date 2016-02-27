drop table if exists guide_opt_ins;


create table guide_opt_ins as
select 
    fc.uid drupal_user_id,
    to_timestamp(fc.timestamp)::date opt_in_date,
    g.guide_nid,
    g.guide_name
from drupal.flag_content fc
join tmp.guides g on fc.content_id = g.guide_nid ;

update  tmp.guides g1
set num_opt_ins = c
from
   (select guide_nid, count(1) c
    from guide_opt_ins
    group by guide_nid) g2
where g1.guide_nid = g2.guide_nid;    
                
 --unique on subscription_id, not gcsi_user_id
drop table if exists moga_sub_master cascade;

create table moga_sub_master as
select distinct on (subscription_id, guide_nid)
    ud.gcsi_user_id,
    ud.drupal_user_id,
    ud.subscription_id,
    g.guide_nid,
    g.opt_in_date,
    ud.onboarding_segment,
    ud.user_behavior_segment,
    ud.campaign_dept,
    ud.subscription_start_date,
    ud.cancel_date,
    ud.paid_through_date,
    ud.winback
from common.user_d ud
join guide_opt_ins g
    on ud.drupal_user_id = g.drupal_user_id
where ud.valid_from <= g.opt_in_date and
      ud.valid_to >= g.opt_in_date;
      

--nothing is summarized here...      
drop table if exists moga_video_views;

create table moga_video_views as
select vv.*,
    case when days_since_viewed <=30 then 1
	 when days_since_viewed <=60 then 2
	 when days_since_viewed <=90 then 3
    end period
from
	(select 
	   go.drupal_user_id,
	   go.guide_nid,
	   vdc.created_date,
	   vdc.num_views, 
	   vdc.watched, 
	   vdc.completion_ratio,
	   v.duration,
	   go.opt_in_date - vdc.created_date::date                                      days_since_viewed,  
	   case when reporting_segment = 'My Yoga' then num_views else 0 end            yoga_views,       
	   case when reporting_segment = 'My Yoga' then watched else 0 end              yoga_watched,
	   case when reporting_segment = 'My Yoga' then duration else 0 end             yoga_video_duration,
	   case when reporting_segment = 'Spiritual Growth' then num_views else 0 end   sg_views,
	   case when reporting_segment = 'Spiritual Growth' then watched else 0 end     sg_watched,
	   case when reporting_segment = 'Spiritual Growth' then duration else 0 end    sg_video_duration,
	   case when reporting_segment = 'Seeking Truth' then num_views else 0 end      st_views,
	   case when reporting_segment = 'Seeking Truth' then watched else 0 end        st_watched,
	   case when reporting_segment = 'Seeking Truth' then duration else 0 end       st_video_duration,
	   case when guide_day is null then 0 else 1 end                                guide_day_views
	from  guide_opt_ins go
	left join  common.video_daily_cube vdc
	    on go.drupal_user_id = vdc.user_id
	left join common.video_d v
	   on vdc.media_nid = v.media_nid
	left join common.guide_d g
	   on vdc.media_nid = g.media_nid) vv
where days_since_viewed >=0 and
      days_since_viewed <=90;
    
drop table if exists moga_video_views_smry cascade;

create table moga_video_views_smry as 
select
    drupal_user_id,
    guide_nid,
    period, 
    coalesce(vxc.num_views,0)                       num_views,
    coalesce(vxc.num_days_watched,0)                num_days_watched,
    coalesce(vxc.hrs_watched,0)                     hrs_watched,
    coalesce(vxc.avg_completion_ratio,0)            avg_completion_ratio,
    coalesce(vxc.engagement_ratio,0)                engagement_ratio,
    coalesce(vxc.yoga_engagement_ratio,0)           yoga_engagement_ratio,
    coalesce(vxc.sg_engagement_ratio,0)             sg_engagement_ratio,
    coalesce(vxc.st_engagement_ratio,0)             st_engagement_ratio,
    coalesce(vxc.yoga_views,0)                      yoga_views,
    coalesce(vxc.yoga_watched,0)                    yoga_watched,
    coalesce(vxc.sg_views,0)                        sg_views,
    coalesce(vxc.sg_watched,0)                      sg_watched,
    coalesce(vxc.st_views,0)                        st_views,
    coalesce(vxc.st_watched,0)                      st_watched,
    days_since_last_video_view
from
    (select drupal_user_id,
       guide_nid,
       period,
       sum(num_views)               num_views, 
       count(distinct created_date) num_days_watched, 
       round(sum(watched)/3600,5)   hrs_watched,
       avg(completion_ratio)        avg_completion_ratio,
       case when sum(duration) > 0
            then round(sum(watched)/sum(duration),5)                  
            else 0
       end                                                  engagement_ratio,
       case when sum(yoga_video_duration) > 0
            then round(sum(yoga_watched)/sum(yoga_video_duration),5)
            else 0
       end                                                  yoga_engagement_ratio,
       case when sum(sg_video_duration) > 0
            then round(sum(sg_watched)/sum(sg_video_duration),5)
            else 0
       end                                                  sg_engagement_ratio,
          case when sum(st_video_duration) > 0
            then round(sum(st_watched)/sum(st_video_duration),5)
            else 0
       end                                                  st_engagement_ratio,
       sum(yoga_views)                                      yoga_views,
       sum(yoga_watched)                                    yoga_watched,
       sum(sg_views)                                        sg_views,
       sum(sg_watched)                                      sg_watched,
       sum(st_views)                                        st_views,
       sum(st_watched)                                      st_watched,
       min(days_since_viewed)                               days_since_last_video_view
    from moga_video_views vv
    where guide_day_views = 0
    group by drupal_user_id, guide_nid, period) vxc
;
    
drop table if exists moga_guide_views cascade;
    
create table moga_guide_views as 
select 
   vdc.user_id,
   gd.guide_nid,
   gd.guide_title,
   gd.guide_day,
   gd.guide_day_nid,
   vdc.created_date,
   vdc.completion_ratio
from common.video_daily_cube vdc
join common.guide_d gd
    on gd.media_nid = vdc.media_nid and
       (vdc.media_nid = vdc.extra_nid OR
        gd.guide_day_nid = vdc.extra_nid);
       
   
--backwards compatibility for Tableau

create or replace view guide_views as 
select * from moga_guide_views;

create or replace view guide_tenure as 
select drupal_user_id,
       guide_nid,
       subscription_start_date::date - opt_in_date::date tenure_at_opt_in,
       winback
from moga_sub_master;


--moga and rod/coll 30 days after opt-in
create or replace view video_views_moga as 
select * from moga_video_views_smry
where period = 1
  and guide_nid in (107311, 107431);






