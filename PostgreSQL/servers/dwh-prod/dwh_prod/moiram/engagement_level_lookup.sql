drop table if exists common.engagement_level;

create table common.engagement_level (
   user_behavior_segment varchar(255),
   tenure_cat_low   integer,
   tenure_cat_high  integer,
   days_since_last_video_view_low integer,
   days_since_last_video_view_high integer,
   engagement_level varchar(10)
);


alter table common.engagement_level owner to dw_admin;

insert into common.engagement_level values('My Yoga',0,120,0,15,'High');
insert into common.engagement_level values('My Yoga',0,120,16,40,'Medium');
insert into common.engagement_level values('My Yoga',0,120,41,70,'Low');
insert into common.engagement_level values('My Yoga',0,120,71,999999,'Dormant');
insert into common.engagement_level values('My Yoga',121,395,0,20,'High');
insert into common.engagement_level values('My Yoga',121,395,21,45,'Medium');
insert into common.engagement_level values('My Yoga',121,395,46,90,'Low');
insert into common.engagement_level values('My Yoga',121,395,91,999999,'Dormant');
insert into common.engagement_level values('My Yoga',396,999999,0,20,'High');
insert into common.engagement_level values('My Yoga',396,999999,21,45,'Medium');
insert into common.engagement_level values('My Yoga',396,999999,46,90,'Low');
insert into common.engagement_level values('My Yoga',396,999999,91,999999,'Dormant');

insert into common.engagement_level values('Spiritual Growth',0,120,0,9,'High');
insert into common.engagement_level values('Spiritual Growth',0,120,10,40,'Medium');
insert into common.engagement_level values('Spiritual Growth',0,120,41,60,'Low');
insert into common.engagement_level values('Spiritual Growth',0,120,61,999999,'Dormant');
insert into common.engagement_level values('Spiritual Growth',121,395,0,20,'High');
insert into common.engagement_level values('Spiritual Growth',121,395,21,40,'Medium');
insert into common.engagement_level values('Spiritual Growth',121,395,41,90,'Low');
insert into common.engagement_level values('Spiritual Growth',121,395,91,999999,'Dormant');
insert into common.engagement_level values('Spiritual Growth',396,999999,0,14,'High');
insert into common.engagement_level values('Spiritual Growth',396,999999,15,30,'Medium');
insert into common.engagement_level values('Spiritual Growth',396,999999,31,90,'Low');
insert into common.engagement_level values('Spiritual Growth',396,999999,91,999999,'Dormant');

insert into common.engagement_level values('Seeking Truth',0,120,0,6,'High');
insert into common.engagement_level values('Seeking Truth',0,120,7,14,'Medium');
insert into common.engagement_level values('Seeking Truth',0,120,15,45,'Low');
insert into common.engagement_level values('Seeking Truth',0,120,46,999999,'Dormant');
insert into common.engagement_level values('Seeking Truth',121,395,0,12,'High');
insert into common.engagement_level values('Seeking Truth',121,395,13,21,'Medium');
insert into common.engagement_level values('Seeking Truth',121,395,22,90,'Low');
insert into common.engagement_level values('Seeking Truth',121,395,91,999999,'Dormant');
insert into common.engagement_level values('Seeking Truth',396,999999,0,10,'High');
insert into common.engagement_level values('Seeking Truth',396,999999,11,17,'Medium');
insert into common.engagement_level values('Seeking Truth',396,999999,18,90,'Low');
insert into common.engagement_level values('Seeking Truth',396,999999,91,999999,'Dormant');

insert into common.engagement_level values('No Clear Segment',0,120,0,10,'High');
insert into common.engagement_level values('No Clear Segment',0,120,11,25,'Medium');
insert into common.engagement_level values('No Clear Segment',0,120,26,60,'Low');
insert into common.engagement_level values('No Clear Segment',0,120,61,999999,'Dormant');
insert into common.engagement_level values('No Clear Segment',121,395,0,14,'High');
insert into common.engagement_level values('No Clear Segment',121,395,15,25,'Medium');
insert into common.engagement_level values('No Clear Segment',121,395,26,90,'Low');
insert into common.engagement_level values('No Clear Segment',121,395,91,999999,'Dormant');
insert into common.engagement_level values('No Clear Segment',396,999999,0,14,'High');
insert into common.engagement_level values('No Clear Segment',396,999999,15,30,'Medium');
insert into common.engagement_level values('No Clear Segment',396,999999,31,90,'Low');
insert into common.engagement_level values('No Clear Segment',396,999999,91,999999,'Dormant');