drop table if exists common.cohort_d;

create table common.cohort_d (
   cohort_id            integer,
   cohort_name      varchar(255),
   rule_id              integer,
   rule_name            varchar(255),
   gcsi_user_id     integer,
   subscription_id  integer,
   created          timestamp);



truncate table common.cohort_d;

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  1,  'Cosmic Disclosure', 1, 'cid', gcsi_user_id, subscription_id, start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%s024'
       and sd.start_date >= '20150601')foo);



insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, created)
(select  1,  subscription_cohort, 1003, sub_cohort_segment, gcsi_user_id, start_date
 from 
    (select   subscription_cohort, sub_cohort_segment, gcsi_user_id, min(subscription_start_date) start_date
     from common.user_d u
     where subscription_cohort = 'Cosmic Disclosure'
       and sub_cohort_segment = 'goode'
       and not exists 
           (select gcsi_user_id from common.cohort_d c
            where u.gcsi_user_id = c.gcsi_user_id 
              and cohort_id = 1)
     group by subscription_cohort, sub_cohort_segment, gcsi_user_id)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select 2,  'Conscious Cleanse', 4, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%c015')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  3,  'Commit to You', 5, 'cid', gcsi_user_id, subscription_id, start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%al008')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, created)
(select  4,  subscription_cohort, 6, sub_cohort_segment, gcsi_user_id, start_date
 from 
    (select   subscription_cohort, sub_cohort_segment, gcsi_user_id, min(subscription_start_date) start_date
     from common.user_d
     where subscription_cohort = 'Gaiam Prospect Offer'
       and sub_cohort_segment = 'cid'
     group by subscription_cohort, sub_cohort_segment, gcsi_user_id)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  5,  'You Year', 7, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like  '%al036')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  6,  'Seeking Truth Marathon', 8, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%s038')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  7,  'SG Learn to Meditate', 9, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%sg034')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  8,  'Balanced You', 10, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%al005')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  9,  'Hidden Origins', 11, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.t_subscriptions ts 
       on sd.subscription_id = ts.subscription_id 
     where ts.channel like '%s039')foo);

drop table if exists tmp.fve_series_title;

create table tmp.fve_series_title as 
(select gcsi_user_id, cohort_id, subscription_id, start_date
from
    (select   gcsi_user_id, cohort_id,
         first_value(subscription_id) over w subscription_id, 
         first_value(start_date) over w start_date,
         row_number() over w as rn
     from 
        (select * from
            (select sd.gcsi_user_id, sd.subscription_id, sd.start_date,
             case when sd.start_date > '20150701' and
                       fve.series_title = 'Cosmic Disclosure' 
                  then 1
                  when sd.start_date > '20151226' and 
                       fve.series_title = 'Hidden Origins'
                  then 9
             end cohort_id
             from common.subscription_d sd 
             join first_video_engagement fve 
               on sd.drupal_user_id = fve.drupal_user_id) foo
          where cohort_id is not null) bar
    window w as (
    partition by gcsi_user_id, cohort_id
    order by start_date
    rows between unbounded preceding
        and unbounded following)) bar
where rn = 1);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  1,  'Cosmic Disclosure', 1002, 'first video_engagement', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_series_title
     where cohort_id = 1 
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 1)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  9,  'Hidden Origins', 1004, 'first video_engagement', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_series_title
     where cohort_id = 9
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 9)foo);

drop table if exists tmp.fve_guide_title;

create table tmp.fve_guide_title as 
(select gcsi_user_id, cohort_id, subscription_id, start_date
from
    (select   gcsi_user_id, cohort_id,
         first_value(subscription_id) over w subscription_id, 
         first_value(start_date) over w start_date,
         row_number() over w as rn
     from 
        (select * from
            (select sd.gcsi_user_id, sd.subscription_id, sd.start_date,
             case when sd.start_date > '20150401' and
                       g.guide_title = 'Conscious Cleanse' 
                  then 2
                  when sd.start_date > '20141225' and
                       g.guide_title = 'Commit to You' 
                  then 3
                  when sd.start_date > '20151226' and
                       g.guide_title = 'SG Learn to Meditate' 
                  then 7
                  when sd.start_date > '20151226' and
                       g.guide_title = 'The Balanced You'
                  then 8
             end cohort_id
             from common.subscription_d sd 
             join first_video_engagement fve 
               on sd.drupal_user_id = fve.drupal_user_id
             join common.guide_d g 
                on fve.extra_nid = g.guide_day_nid) foo
          where cohort_id is not null) bar
    window w as (
    partition by gcsi_user_id, cohort_id
    order by start_date
    rows between unbounded preceding
        and unbounded following)) bar
where rn = 1);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select distinct 2,  'Conscious Cleanse', 1005, 'first_video_engagement', gcsi_user_id, subscription_id,start_date
 from        
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 2
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 2)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  3,  'Commit to You', 1006, 'first_video_engagement', gcsi_user_id, subscription_id,start_date
 from        
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 3
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 3)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  7,  'SG Learn to Meditate', 1007, 'first_video_engagement', gcsi_user_id, subscription_id,start_date
 from       
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 7
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 7)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  8,  'Balanced You', 1008, 'first_video_engagement', gcsi_user_id, subscription_id,start_date
 from        
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 8
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 8)foo);
  
  
  --testing
    select cohort_name, rule_id,  count(distinct gcsi_user_id)
    from common.cohort_d
    group by cohort_name, rule_id;
    
    select cohort_name,  count(distinct gcsi_user_id)
    from common.cohort_d
    where created >= '20151226'::date
    group by cohort_name;
    
    select * from common.cohort_d
    where gcsi_user_id in 
    (select gcsi_user_id
     from common.cohort_d 
     group by gcsi_user_id
      having count(1) > 1)
    order by gcsi_user_id;
