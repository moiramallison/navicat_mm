
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
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%s024'
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
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%c015')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  3,  'Commit to You', 5, 'cid', gcsi_user_id, subscription_id, start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%al008')foo);

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
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like  '%al036')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  6,  'Seeking Truth Marathon', 8, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%s038')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  7,  'SG Learn to Meditate', 9, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%sg034')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  8,  'Balanced You', 10, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%al005')foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  9,  'Hidden Origins', 11, 'cid', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join gcsi.v_subscription_creation_data cd 
       on sd.subscription_id = cd.subscription_id 
     where cd.channel like '%s039')foo);

drop table if exists tmp.fve_series_title;

create table tmp.fve_series_title as 
select   gcsi_user_id, 
     subscription_id, 
     min(start_date) start_date,
     sum(cosmic_disclosure) cosmic_disclosure,
     sum(hidden_origins) hidden_origins,
     sum(seeking_truth_marathon) seeking_truth_marathon
 from 
    (select * from
        (select sd.gcsi_user_id, sd.subscription_id, sd.start_date,
             case when sd.start_date > '20150701' and
                       fve.series_title = 'Cosmic Disclosure' 
                  then 1
                  else 0
              end cosmic_disclosure,
              case when sd.start_date > '20151226' and 
                       fve.series_title = 'Hidden Origins'
                  then 1
                  else 0 
              end hidden_origins,
              case when sd.start_date > '20151226' and 
                        sd.start_date <= '20160103' and 
                       fve.site_segment = 'Seeking Truth'
                  then 1
                  else 0 
             end seeking_truth_marathon
         from common.subscription_d sd 
         join first_video_engagement fve 
           on sd.drupal_user_id = fve.drupal_user_id) foo
      where greatest(cosmic_disclosure,hidden_origins,seeking_truth_marathon) >0) bar
group by gcsi_user_id, subscription_id;


insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  1,  'Cosmic Disclosure', 3020, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_series_title
     where cosmic_disclosure  = 1 
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 1)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  9,  'Hidden Origins', 3004, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_series_title
     where hidden_origins  = 1
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 9)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  8,  'Seeking Truth Marathon', 3005, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_series_title
     where seeking_truth_marathon = 1
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 8)foo);


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
(select distinct 2,  'Conscious Cleanse', 3009, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from        
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 2
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 2)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  3,  'Commit to You', 3006, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from        
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 3
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 3)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  7,  'SG Learn to Meditate', 3007, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from       
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 7
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 7)foo);

insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id,created)
(select  8,  'Balanced You', 3008, 'first video engagement', gcsi_user_id, subscription_id,start_date
 from        
     (select gcsi_user_id, subscription_id, start_date
     from tmp.fve_guide_title
     where cohort_id = 8
except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 8)foo);

-- guide opt-ins

          
insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  2,  'Conscious Cleanse', 2009, 'guide opt-in 201504', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join drupal.flag_content fc
       on sd.drupal_user_id = fc.uid and
          fc.fid = 11
     join common.guide_d gd
       on fc.content_id = gd.guide_nid and
          gd.guide_title = 'Conscious Cleanse'
     where start_date > '20150416' and 
          to_timestamp(fc.timestamp) <= '20150426'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 2) foo); 
          
 insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
 (select  2,  'Conscious Cleanse', 2008, 'guide opt-in 201507', gcsi_user_id, subscription_id,start_date
  from 
     (select gcsi_user_id, sd.subscription_id, sd.start_date
      from common.subscription_d sd
      join drupal.flag_content fc
        on sd.drupal_user_id = fc.uid and
           fc.fid = 11
      join common.guide_d gd
        on fc.content_id = gd.guide_nid and
           gd.guide_title = 'Conscious Cleanse'
      where start_date > '20150707' and 
          to_timestamp(fc.timestamp) <= '20150718'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 2) foo); 
          
 insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
 (select  2,  'Conscious Cleanse', 2007, 'guide opt-in 201510', gcsi_user_id, subscription_id,start_date
  from 
     (select gcsi_user_id, sd.subscription_id, sd.start_date
      from common.subscription_d sd
      join drupal.flag_content fc
        on sd.drupal_user_id = fc.uid and
           fc.fid = 11
      join common.guide_d gd
        on fc.content_id = gd.guide_nid and
           gd.guide_title = 'Conscious Cleanse'
      where start_date > '20151025' and 
          to_timestamp(fc.timestamp) <= '20151105'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 2) foo);  
          
insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  2,  'Conscious Cleanse', 2007, 'guide opt-in 201601', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join drupal.flag_content fc
       on sd.drupal_user_id = fc.uid and
          fc.fid = 11
     join common.guide_d gd
       on fc.content_id = gd.guide_nid and
          gd.guide_title = 'Conscious Cleanse'
     where start_date > '20151226' and 
          to_timestamp(fc.timestamp) <= '20160111'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 2) foo); 
          
insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
(select  3,  'Commit To You', 2010, 'guide opt-in', gcsi_user_id, subscription_id,start_date
 from 
    (select gcsi_user_id, sd.subscription_id, sd.start_date
     from common.subscription_d sd
     join drupal.flag_content fc
       on sd.drupal_user_id = fc.uid and
          fc.fid = 11
     join common.guide_d gd
       on fc.content_id = gd.guide_nid and
          gd.guide_title = 'Commit To You'
     where start_date > '20141226' and 
          to_timestamp(fc.timestamp) <= '20150111'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 3) foo); 
          
 insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
 (select  7,  'SG Learn to Meditate', 2001, 'guide opt-in', gcsi_user_id, subscription_id,start_date
  from 
     (select gcsi_user_id, sd.subscription_id, sd.start_date
      from common.subscription_d sd
      join drupal.flag_content fc
        on sd.drupal_user_id = fc.uid and
           fc.fid = 11
      join common.guide_d gd
        on fc.content_id = gd.guide_nid and
           gd.guide_title = 'How to Meditate'
      where start_date > '20151226' and 
          to_timestamp(fc.timestamp) <= '20160111'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 7) foo); 
          
  insert into common.cohort_d (cohort_id, cohort_name, rule_id, rule_name, gcsi_user_id, subscription_id, created)
  (select  8,  'Balanced You', 2002, 'guide opt-in', gcsi_user_id, subscription_id,start_date
   from 
      (select gcsi_user_id, sd.subscription_id, sd.start_date
       from common.subscription_d sd
       join drupal.flag_content fc
         on sd.drupal_user_id = fc.uid and
            fc.fid = 11
       join common.guide_d gd
         on fc.content_id = gd.guide_nid and
            gd.guide_title = 'The Balanced You'
       where start_date > '20151226' and 
          to_timestamp(fc.timestamp) <= '20160111'
 except -- "upsert"
    select gcsi_user_id, subscription_id, created 
    from common.cohort_d
    where cohort_id = 8) foo); 

  
alter table common.cohort_d owner to dw_admin;

