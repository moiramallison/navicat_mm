--first get all the data into one TABLE

drop table if exists test_fb_reengagement;

create table test_fb_reengagement as
	select t1.*, 'MY_Members_120-395_Dormant' as testgroup, 'Test' as testcontrol
	from "MY_Members_120-395_Dormant" t1
union
    select t1.*, 'MY_Members_120-395_Low' as testgroup, 'Test' as testcontrol
    from "MY_Members_120-395_Low" t1
union
    select t1.*, 'MY_Members_120-395_Medium' as testgroup, 'Test' as testcontrol
    from "MY_Members_120-395_Medium" t1
union
    select t1.*, 'SG_Members_120-395_Dormant' as testgroup, 'Test' as testcontrol
    from "SG_Members_120-395_Dormant" t1
 union
     select t1.*, 'SG_Members_120-395_Low' as testgroup, 'Test' as testcontrol
     from "SG_Members_120-395_Low" t1
 union
     select t1.*, 'SG_Members_120-395_Medium' as testgroup, 'Test' as testcontrol
    from "SG_Members_120-395_Medium" t1
union
    select t1.*, 'ST_Members_120-395_Dormant' as testgroup, 'Test' as testcontrol
    from "ST_Members_120-395_Dormant" t1
union
    select t1.*, 'ST_Members_120-395_Low' as testgroup, 'Test' as testcontrol
    from "ST_Members_120-395_Low" t1
union
    select t1.*, 'ST_Members_120-395_Medium' as testgroup, 'Test' as testcontrol
    from "ST_Members_120-395_Medium" t1
union
    select t1.*, 'ST_Members_120-395_Medium' as testgroup, 'Test' as testcontrol
    from "ST_Members_120-395_Medium" t1
union
    select t1.*,  'Control' as testcontrol
    from "PercentControl3.14" t1;

--not sure where this got introduced. fix it here
update  test_fb_reengagement set testgroup = 'ST_Members_120-395_Dormant'
where testgroup = 'ST_Members_12-395_Dormant';
update  test_fb_reengagement set testgroup = 'ST_Members_120-395_Medium'
where testgroup like 'ST_Members_120-395-Medium';

-- trim emails
update test_fb_reengagement set "EMAIL_ADDRESS" = trim("EMAIL_ADDRESS");


drop table if exists tmp.retention_test;

create table tmp.retention_test as 
select tst.*,
       to_timestamp(du.access) last_access_date,
   case when ud.last_video_view_date > '20160315'::date and
             ud.last_video_view_date > '20160328'::date
        then 1 else 0 
   end video_view,
   case when du.access  > 1458136800 and 
             du.access  < 1459238400
        then 1 else 0 
   end login,
   sd.status
from
   (select 
           "EMAIL_ADDRESS"                  email_address,
           du.uid        										drupal_user_id,
           split_part(testgroup, '_', 4) 		engagement_group,
           split_part(testgroup, '_', 1) 		segment_group,
           testcontrol 											version_group
    from moiram.test_fb_reengagement t
    left join drupal.users du 
        on lower(t."EMAIL_ADDRESS") = lower(du.mail))tst
left join common.user_dim ud
    on tst.drupal_user_id = ud.drupal_user_id
left join drupal.users du
    on tst.drupal_user_id = du.uid
left join common.subscription_d sd
    on ud.current_subscription = sd.subscription_id;

select email_address,   drupal_user_id,  engagement_group,   segment_group, version_group,  last_access_date, video_view, login,  status
from tmp.retention_test
where drupal_user_id is  not null;

select * from tmp.retention_test
where engagement_group not in ('Medium', 'Dormant','Low');



 
