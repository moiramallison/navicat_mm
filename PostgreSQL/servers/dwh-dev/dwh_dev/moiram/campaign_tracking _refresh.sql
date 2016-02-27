drop table if exists common.campaign_tracking;

create table common.campaign_tracking as 
(select distinct 
     "Campaign" as campaign,
     "Department" as department,
     "Sub-Type" as sub_type,
     "CID Code" as cid_code,
     "Final Link Query" as final_link_query,
     "Reported Channel" as reported_channel
 from moiram.campaign_tracking);

alter table common.campaign_tracking add 
	subscription_cohort text COLLATE "default";

alter table common.campaign_tracking add
	sub_cohort_segment text COLLATE "default";

update common.campaign_tracking
   set subscription_cohort = 'Cosmic Disclosure'
   where campaign like '%Cosmic Disc%';

update common.campaign_tracking
   set subscription_cohort = 'Conscious Cleanse'
   where campaign like '%Conscious Cleanse%';

update common.campaign_tracking
   set subscription_cohort = 'Commit to You'
   where campaign like '%al008%';

update common.campaign_tracking
   set subscription_cohort = 'Gaiam Prospect Offer'
   where campaign like '%al026%';

update common.campaign_tracking
   set sub_cohort_segment = 'MYO - 3 month'
   where campaign like '%al026%'
     and cid_code like '%MY%3mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'MYO - 6 month'
   where campaign like '%al026%'
     and cid_code like '%MY%6mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'MYO - Basic'
   where campaign like '%al026%'
     and cid_code like '%MY%Basic%';

update common.campaign_tracking
   set sub_cohort_segment = 'Seeking Truth - 3 month'
   where campaign like '%al026%'
     and cid_code like '%ST%3mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'Seeking Truth - 6 month'
   where campaign like '%al026%'
     and cid_code like '%ST%6mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'Seeking Truth - Basic'
   where campaign like '%al026%'
     and cid_code like '%ST%Basic%';

update common.campaign_tracking
   set sub_cohort_segment = 'Spiritual Growth - 3 month'
   where campaign like '%al026%'
     and cid_code like '%SG%3mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'Spiritual Growth - 6 month'
   where campaign like '%al026%'
     and cid_code like '%SG%6mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'Spiritual Growth - Basic'
   where campaign like '%al026%'
     and cid_code like '%SG%Basic%';

update common.campaign_tracking
   set sub_cohort_segment = 'No Channel - 3 month'
   where campaign like '%al026%'
     and cid_code like '%NULL%3mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'No Channel - 6 month'
   where campaign like '%al026%'
     and cid_code like '%NULL%6mon%';

update common.campaign_tracking
   set sub_cohort_segment = 'No Channel - Basic'
   where campaign like '%al026%'
     and cid_code like '%NULL%Basic%';

select * from common.campaign_tracking
where reported_channel in 
(select reported_channel from common.campaign_tracking
 group by reported_channel
 having count(1) > 1);

